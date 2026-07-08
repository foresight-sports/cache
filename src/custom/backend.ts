import * as core from "@actions/core";
import {
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    S3Client
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import * as crypto from "crypto";
import { createReadStream, statSync } from "fs";
import { Agent } from "https";

import {
    cacheUtils as utils,
    CompressionMethod,
    DownloadOptions,
    getDownloadOptions
} from "../actionsCacheShims.js";
import { downloadCacheHttpClientConcurrent } from "./downloadUtils";
import { streamedRestore } from "./streamingRestore";
import { transferArchive, TransferParams } from "./transferEngine";
import { computeEffectivePartSize } from "./utils/partSize";

export interface ArtifactCacheEntry {
    cacheKey?: string;
    scope?: string;
    cacheVersion?: string;
    creationTime?: string;
    archiveLocation?: string;
}

// if executing from RunsOn, unset any existing AWS credential env variables so that we can use the IAM instance profile for credentials
// see unsetCredentials() in https://github.com/aws-actions/configure-aws-credentials/blob/v4.0.2/src/helpers.ts#L44
// Note: we preserve AWS_REGION and AWS_DEFAULT_REGION as they are needed for SDK initialization
if (process.env.RUNS_ON_RUNNER_NAME && process.env.RUNS_ON_RUNNER_NAME !== "") {
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_ACCESS_KEY;
    delete process.env.AWS_SESSION_TOKEN;
}

// Bumped 1.0 -> 2.0 to start a fresh cache generation: the no-compression fast
// path now writes zstd-compressed archives (see custom/utils/uncompressedTar.ts),
// which are not byte-compatible with previously stored raw-tar archives that share
// the same file extension. Bumping the salt namespaces old and new archives apart
// so a stale raw-tar entry is never fetched and fed to the zstd extractor.
const versionSalt = "2.0";
const bucketName = process.env.RUNS_ON_S3_BUCKET_CACHE;
const endpoint = process.env.RUNS_ON_S3_BUCKET_ENDPOINT;
const region =
    process.env.RUNS_ON_AWS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;
const forcePathStyle =
    process.env.RUNS_ON_S3_FORCE_PATH_STYLE === "true" ||
    process.env.AWS_S3_FORCE_PATH_STYLE === "true";

const uploadQueueSize = Number(process.env.UPLOAD_QUEUE_SIZE || "16");
const uploadPartSize =
    Number(process.env.UPLOAD_PART_SIZE || "64") * 1024 * 1024;
const downloadQueueSize = Number(process.env.DOWNLOAD_QUEUE_SIZE || "16");
const downloadPartSize =
    Number(process.env.DOWNLOAD_PART_SIZE || "32") * 1024 * 1024;

// The AWS SDK's default HTTP handler caps concurrent sockets at maxSockets=50,
// which throttles multipart concurrency above ~48 no matter how large the queue
// size is. This socket pool is attached to s3Client, which only the upload path
// uses (the lib-storage Upload fans out its PUT-part requests through it). The
// download path builds its own @actions/http-client HttpClient from a presigned
// URL (see downloadUtils.ts / downloadCacheHttpClientConcurrent) and never
// touches s3Client, so download concurrency is governed separately there. Size
// the pool to the upload queue, plus headroom.
const s3Client = new S3Client({
    region,
    forcePathStyle,
    endpoint,
    requestHandler: new NodeHttpHandler({
        httpsAgent: new Agent({
            keepAlive: true,
            maxSockets: uploadQueueSize + 8
        })
    })
});

export function getCacheVersion(
    paths: string[],
    compressionMethod?: CompressionMethod,
    enableCrossOsArchive = false
): string {
    // don't pass changes upstream
    const components = paths.slice();

    // Add compression method to cache version to restore
    // compressed cache as per compression method
    if (compressionMethod) {
        components.push(compressionMethod);
    }

    // Only check for windows platforms if enableCrossOsArchive is false
    if (process.platform === "win32" && !enableCrossOsArchive) {
        components.push("windows-only");
    }

    // Add salt to cache version to support breaking changes in cache entry
    components.push(versionSalt);

    return crypto
        .createHash("sha256")
        .update(components.join("|"))
        .digest("hex");
}

function getS3Prefix(
    paths: string[],
    { compressionMethod, enableCrossOsArchive }
): string {
    const repository = process.env.GITHUB_REPOSITORY;
    const version = getCacheVersion(
        paths,
        compressionMethod,
        enableCrossOsArchive
    );

    return ["cache", repository, version].join("/");
}

export async function getCacheEntry(
    keys,
    paths,
    { compressionMethod, enableCrossOsArchive }
) {
    const cacheEntry: ArtifactCacheEntry = {};

    // Find the most recent key matching one of the restoreKeys prefixes
    for (const restoreKey of keys) {
        const s3Prefix = getS3Prefix(paths, {
            compressionMethod,
            enableCrossOsArchive
        });
        const listObjectsParams = {
            Bucket: bucketName,
            Prefix: [s3Prefix, restoreKey].join("/")
        };

        try {
            const { Contents = [] } = await s3Client.send(
                new ListObjectsV2Command(listObjectsParams)
            );
            if (Contents.length > 0) {
                // Sort keys by LastModified time in descending order
                const sortedKeys = Contents.sort(
                    (a, b) => Number(b.LastModified) - Number(a.LastModified)
                );
                const s3Path = sortedKeys[0].Key; // Return the most recent key
                cacheEntry.cacheKey = s3Path?.replace(`${s3Prefix}/`, "");
                cacheEntry.archiveLocation = `s3://${bucketName}/${s3Path}`;
                return cacheEntry;
            }
        } catch (error) {
            console.error(
                `Error listing objects with prefix ${restoreKey} in bucket ${bucketName}:`,
                error
            );
        }
    }

    return cacheEntry; // No keys found
}

export async function downloadCache(
    archiveLocation: string,
    archivePath: string,
    options?: DownloadOptions
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    if (!region) {
        throw new Error("Environment variable RUNS_ON_AWS_REGION not set");
    }

    // Narrow the module-level env consts to non-undefined for use inside the
    // closure below (the guards above already asserted them).
    const bucket = bucketName;

    const archiveUrl = new URL(archiveLocation);
    const objectKey = archiveUrl.pathname.slice(1);

    // Node fallback: presigned-URL ranged downloader with validation retries.
    // This is the always-present safety net used when s5cmd/aws-cli are missing
    // or fail. The presigned URL is signed with the same S3Client credentials the
    // direct engine path uses, so read access is identical either way.
    const nodeDownload = async (): Promise<void> => {
        const maxRetries = 3;
        let lastError: Error | undefined;

        for (let attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                const command = new GetObjectCommand({
                    Bucket: bucket,
                    Key: objectKey
                });
                const url = await getSignedUrl(s3Client, command, {
                    expiresIn: 3600
                });

                await downloadCacheHttpClientConcurrent(url, archivePath, {
                    ...options,
                    downloadConcurrency: downloadQueueSize,
                    concurrentBlobDownloads: true,
                    partSize: downloadPartSize
                });

                // If we get here, download succeeded
                return;
            } catch (error) {
                const errorMessage = (error as Error).message;
                lastError = error as Error;

                // Only retry on validation failures, not on other errors
                if (
                    errorMessage.includes("Download validation failed") ||
                    errorMessage.includes("Range request not supported") ||
                    errorMessage.includes("Content-Range header")
                ) {
                    if (attempt < maxRetries) {
                        const delayMs = Math.pow(2, attempt - 1) * 1000; // exponential backoff
                        core.warning(
                            `Download attempt ${attempt} failed: ${errorMessage}. Retrying in ${delayMs}ms...`
                        );
                        await new Promise(resolve =>
                            setTimeout(resolve, delayMs)
                        );
                        continue;
                    }
                }

                // For non-retryable errors or max retries reached, throw the error
                throw error;
            }
        }

        // This should never be reached, but just in case
        throw (
            lastError || new Error("Download failed after all retry attempts")
        );
    };

    // Defense-in-depth: s5cmd/aws-cli exit 0 is trusted blindly, so after a
    // native download independently confirm the object landed whole by comparing
    // the on-disk size against the S3 object's ContentLength (HeadObject via the
    // same credentialed s3Client). A mismatch throws, which transferArchive
    // treats as an engine failure and falls through to the next engine rather
    // than hard-failing. A HeadObject that itself fails is not a mismatch, so we
    // swallow it and trust the native result (the node fallback would re-validate
    // byte counts anyway). The node engine validates its own download internally.
    const verifyNativeDownload = async (): Promise<void> => {
        let expectedBytes: number | undefined;
        try {
            const head = await s3Client.send(
                new HeadObjectCommand({ Bucket: bucket, Key: objectKey })
            );
            expectedBytes = head.ContentLength;
        } catch (error) {
            core.debug(
                `Skipping native-download size check (HeadObject failed: ${
                    (error as Error).message
                }).`
            );
            return;
        }
        if (typeof expectedBytes !== "number") {
            return;
        }
        const actualBytes = statSync(archivePath).size;
        if (expectedBytes !== actualBytes) {
            throw new Error(
                `native download size mismatch: S3 ContentLength ${expectedBytes} B != local file ${actualBytes} B`
            );
        }
    };

    // Engine chain: s5cmd -> aws-cli -> node. The native engines pull the object
    // directly by bucket+key (same credential chain the S3Client signs with);
    // the node presigned downloader is the guaranteed fallback if a native
    // engine is unavailable or fails. The archive key is derived identically to
    // the upload path, so any engine restores any engine's cache.
    await transferArchive(
        "download",
        {
            bucket,
            key: objectKey,
            archivePath,
            endpoint,
            region,
            forcePathStyle
        },
        nodeDownload,
        undefined,
        verifyNativeDownload
    );
}

/**
 * Streamed restore: pull s3://bucket/key straight through the decompressor+tar
 * so the download and the extraction OVERLAP (no scratch archive on disk),
 * instead of downloadCache()-to-file THEN extractTar(). Returns true when the
 * cache was streamed AND extracted end-to-end; returns false to signal the
 * caller to fall back to the file-based download+extract path (used when no
 * streaming engine is available/succeeds, or when the S3 config is missing so
 * the file-based path can surface the clear error). This never throws for a
 * streaming failure — a fall-back is always safe because the file-based path
 * re-extracts from scratch (tar -x overwrites any partial files a broken stream
 * left behind), and its own size/integrity checks are not bypassed.
 */
export async function downloadCacheStreaming(
    archiveLocation: string
): Promise<boolean> {
    if (!bucketName || !region) {
        // Let the file-based downloadCache() surface the clear config error.
        return false;
    }

    const bucket = bucketName;
    const archiveUrl = new URL(archiveLocation);
    const objectKey = archiveUrl.pathname.slice(1);

    // archivePath is unused for streaming (no scratch file is written); the
    // streaming builders derive everything from bucket/key/endpoint/region.
    const params: TransferParams = {
        bucket,
        key: objectKey,
        archivePath: "",
        endpoint,
        region,
        forcePathStyle
    };

    try {
        await streamedRestore(params);
        return true;
    } catch (error) {
        core.warning(
            `Streamed restore unavailable/failed (${
                (error as Error).message
            }); falling back to download-to-file + extract.`
        );
        return false;
    }
}

export async function saveCache(
    key: string,
    paths: string[],
    archivePath: string,
    {
        compressionMethod,
        enableCrossOsArchive,
        cacheSize: archiveFileSize,
        uploadChunkSize
    }
): Promise<void> {
    void archiveFileSize;
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    if (!region) {
        throw new Error("Environment variable RUNS_ON_AWS_REGION not set");
    }

    // Narrow the module-level env const to non-undefined for use inside the
    // closure below (the guard above already asserted it).
    const bucket = bucketName;

    const s3Prefix = getS3Prefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const s3Key = `${s3Prefix}/${key}`;

    // Stat the archive up front so we can both report its size and size the
    // multipart upload against it.
    const cacheSize = utils.getArchiveFileSizeInBytes(archivePath);

    // @aws-sdk/lib-storage enforces a hard MAX_PARTS = 10000 ceiling. With a fixed
    // part size, any payload larger than partSize * 10000 throws
    // "Exceeded 10000 parts" (e.g. 32 MB parts cap out at ~320 GB, 64 MB at ~640 GB).
    // computeEffectivePartSize raises the part size adaptively so any payload is
    // representable in <= ~9500 parts (headroom under 10000), scales the part size
    // UP for large archives (fewer parts => less per-part main-loop overhead), and
    // floors it at S3's 5 MiB multipart minimum so a small upload-chunk-size on a
    // mid-size cache can't produce a sub-5 MiB part that S3 rejects with
    // EntityTooSmall. `upload-chunk-size` (bytes), when provided, acts as a part-
    // size floor but is still raised here for very large archives.
    const configuredPartSize =
        uploadChunkSize && uploadChunkSize > 0
            ? uploadChunkSize
            : uploadPartSize;
    const effectivePartSize = computeEffectivePartSize(
        cacheSize,
        configuredPartSize
    );

    // Commit Cache
    core.info(
        `Cache Size: ~${Math.round(
            cacheSize / (1024 * 1024)
        )} MB (${cacheSize} B)`
    );

    core.info(`Uploading cache from ${archivePath} to ${bucket}/${s3Key}`);

    // Node fallback upload: the @aws-sdk/lib-storage multipart Upload through the
    // pooled s3Client. Always present; used when s5cmd/aws-cli are missing or fail.
    const nodeUpload = async (): Promise<void> => {
        const estimatedParts = Math.max(
            1,
            Math.ceil(cacheSize / effectivePartSize)
        );
        core.info(
            `Multipart upload (node/lib-storage): part size ~${Math.round(
                effectivePartSize / (1024 * 1024)
            )} MB, queue size ${uploadQueueSize}, ~${estimatedParts} parts for ${cacheSize} B.`
        );

        const multipartUpload = new Upload({
            client: s3Client,
            params: {
                Bucket: bucket,
                Key: s3Key,
                Body: createReadStream(archivePath)
            },
            // Part size in bytes (adaptively floored to stay under the 10000-part cap)
            partSize: effectivePartSize,
            // Max concurrency
            queueSize: uploadQueueSize
        });

        const progress = new UploadProgress(cacheSize);
        progress.startDisplayTimer();

        multipartUpload.on("httpUploadProgress", event => {
            if (typeof event.loaded === "number") {
                progress.setUploadedBytes(event.loaded);
            }
        });

        try {
            await multipartUpload.done();
            progress.setUploadedBytes(cacheSize);
        } finally {
            progress.stopDisplayTimer();
        }
    };

    // Engine chain: s5cmd -> aws-cli -> node. The native engines push the single
    // archive to the exact same s3://bucket/key the node path would, using the
    // same credential chain the S3Client uses; a missing/failing engine
    // transparently falls through to the next.
    await transferArchive(
        "upload",
        {
            bucket,
            key: s3Key,
            archivePath,
            endpoint,
            region,
            forcePathStyle,
            sizeBytes: cacheSize,
            partSizeMb: effectivePartSize / (1024 * 1024)
        },
        nodeUpload
    );

    core.info(`Cache saved successfully.`);
}

class UploadProgress {
    private readonly totalBytes: number;
    private uploadedBytes = 0;
    private readonly startTime = Date.now();
    private displayedComplete = false;
    private timer?: ReturnType<typeof setTimeout>;

    constructor(totalBytes: number) {
        this.totalBytes = totalBytes;
    }

    setUploadedBytes(bytes: number): void {
        if (bytes > this.uploadedBytes) {
            this.uploadedBytes = bytes;
        }
    }

    display(): void {
        if (this.displayedComplete) {
            return;
        }

        const percentage = this.totalBytes
            ? ((this.uploadedBytes / this.totalBytes) * 100).toFixed(1)
            : "0.0";
        const elapsedSeconds = Math.max(
            (Date.now() - this.startTime) / 1000,
            0.001
        );
        const mbPerSec = (
            this.uploadedBytes /
            (1024 * 1024) /
            elapsedSeconds
        ).toFixed(1);

        core.info(
            `Uploaded ${this.uploadedBytes} of ${this.totalBytes} (${percentage}%), ${mbPerSec} MBs/sec`
        );

        if (this.uploadedBytes >= this.totalBytes) {
            this.displayedComplete = true;
        }
    }

    startDisplayTimer(intervalMs = 1000): void {
        const tick = (): void => {
            this.display();

            if (!this.displayedComplete) {
                this.timer = setTimeout(tick, intervalMs);
            }
        };

        this.timer = setTimeout(tick, intervalMs);
    }

    stopDisplayTimer(): void {
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = undefined;
        }

        this.display();
    }
}
