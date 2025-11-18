import {
    S3Client,
    GetObjectCommand,
    ListObjectsV2Command
} from "@aws-sdk/client-s3";
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
import { createReadStream } from "fs";
import * as crypto from "crypto";
import {
    DownloadOptions,
    getDownloadOptions
} from "@actions/cache/lib/options";
import { CompressionMethod } from "@actions/cache/lib/internal/constants";
import * as core from "@actions/core";
import * as utils from "@actions/cache/lib/internal/cacheUtils";
import { Upload } from "@aws-sdk/lib-storage";
import { downloadCacheHttpClientConcurrent } from "./downloadUtils";

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

const versionSalt = "1.0";
const bucketName = process.env.RUNS_ON_S3_BUCKET_CACHE;
const endpoint = process.env.RUNS_ON_S3_BUCKET_ENDPOINT;
const region =
    process.env.RUNS_ON_AWS_REGION ||
    process.env.AWS_REGION ||
    process.env.AWS_DEFAULT_REGION;
const forcePathStyle =
    process.env.RUNS_ON_S3_FORCE_PATH_STYLE === "true" ||
    process.env.AWS_S3_FORCE_PATH_STYLE === "true";

const uploadQueueSize = Number(process.env.UPLOAD_QUEUE_SIZE || "4");
const uploadPartSize =
    Number(process.env.UPLOAD_PART_SIZE || "32") * 1024 * 1024;
const downloadQueueSize = Number(process.env.DOWNLOAD_QUEUE_SIZE || "8");
const downloadPartSize =
    Number(process.env.DOWNLOAD_PART_SIZE || "16") * 1024 * 1024;

const s3Client = new S3Client({ region, forcePathStyle, endpoint });

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

    const archiveUrl = new URL(archiveLocation);
    const objectKey = archiveUrl.pathname.slice(1);

    // Retry logic for download validation failures
    const maxRetries = 3;
    let lastError: Error | undefined;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const command = new GetObjectCommand({
                Bucket: bucketName,
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
                    await new Promise(resolve => setTimeout(resolve, delayMs));
                    continue;
                }
            }

            // For non-retryable errors or max retries reached, throw the error
            throw error;
        }
    }

    // This should never be reached, but just in case
    throw lastError || new Error("Download failed after all retry attempts");
}

export async function saveCache(
    key: string,
    paths: string[],
    archivePath: string,
    { compressionMethod, enableCrossOsArchive, cacheSize: archiveFileSize }
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    if (!region) {
        throw new Error("Environment variable RUNS_ON_AWS_REGION not set");
    }

    const s3Prefix = getS3Prefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const s3Key = `${s3Prefix}/${key}`;

    const multipartUpload = new Upload({
        client: s3Client,
        params: {
            Bucket: bucketName,
            Key: s3Key,
            Body: createReadStream(archivePath)
        },
        // Part size in bytes
        partSize: uploadPartSize,
        // Max concurrency
        queueSize: uploadQueueSize
    });

    // Commit Cache
    const cacheSize = utils.getArchiveFileSizeInBytes(archivePath);
    core.info(
        `Cache Size: ~${Math.round(
            cacheSize / (1024 * 1024)
        )} MB (${cacheSize} B)`
    );

    const totalParts = Math.ceil(cacheSize / uploadPartSize);
    core.info(`Uploading cache from ${archivePath} to ${bucketName}/${s3Key}`);
    const uploadProgress = new UploadProgressReporter(cacheSize);
    uploadProgress.startDisplayTimer();
    const partLoadedBytes = new Map<number, number>();
    let uploadedBytes = 0;

    multipartUpload.on("httpUploadProgress", progress => {
        const partNumber = progress.part ?? 0;

        if (typeof progress.loaded === "number") {
            if (partNumber > 0) {
                const previous = partLoadedBytes.get(partNumber) ?? 0;
                let delta = progress.loaded - previous;

                if (delta < 0) {
                    // part likely restarted, back out prior count and start fresh
                    uploadedBytes = Math.max(uploadedBytes - previous, 0);
                    partLoadedBytes.set(partNumber, 0);
                    delta = progress.loaded;
                }

                if (delta > 0) {
                    uploadedBytes += delta;
                    partLoadedBytes.set(partNumber, progress.loaded);
                }
            } else {
                uploadedBytes = Math.max(uploadedBytes, progress.loaded);
            }

            uploadProgress.setUploadedBytes(
                Math.min(uploadedBytes, cacheSize)
            );
        }

        if (
            partNumber > 0 &&
            typeof progress.loaded === "number" &&
            typeof progress.total === "number" &&
            progress.loaded === progress.total
        ) {
            core.info(`Uploaded part ${partNumber}/${totalParts}.`);
        }
    });

    try {
        await multipartUpload.done();
        core.info(`Cache saved successfully.`);
    } finally {
        uploadProgress.stopDisplayTimer();
    }
}

class UploadProgressReporter {
    private readonly totalBytes: number;
    private uploadedBytes: number;
    private readonly startTime: number;
    private displayedComplete: boolean;
    private timeoutHandle?: ReturnType<typeof setTimeout>;

    constructor(totalBytes: number) {
        this.totalBytes = totalBytes;
        this.uploadedBytes = 0;
        this.startTime = Date.now();
        this.displayedComplete = false;
    }

    setUploadedBytes(bytes: number): void {
        this.uploadedBytes = Math.min(bytes, this.totalBytes);
    }

    private getTransferredBytes(): number {
        return this.uploadedBytes;
    }

    private isDone(): boolean {
        return this.totalBytes === 0 || this.uploadedBytes >= this.totalBytes;
    }

    private display(): void {
        if (this.displayedComplete) {
            return;
        }

        const transferredBytes = this.getTransferredBytes();
        const percentage = this.totalBytes
            ? ((100 * transferredBytes) / this.totalBytes).toFixed(1)
            : "100.0";
        const elapsedTime = Date.now() - this.startTime;
        const uploadSpeed = elapsedTime
            ? (
                transferredBytes /
                (1024 * 1024) /
                (elapsedTime / 1000)
            ).toFixed(1)
            : "0.0";

        core.info(
            `Uploaded ${transferredBytes} of ${this.totalBytes} (${percentage}%), ${uploadSpeed} MBs/sec`
        );

        if (this.isDone()) {
            this.displayedComplete = true;
        }
    }

    startDisplayTimer(delayInMs = 1000): void {
        const displayCallback = (): void => {
            this.display();

            if (!this.isDone()) {
                this.timeoutHandle = setTimeout(displayCallback, delayInMs);
            }
        };

        this.timeoutHandle = setTimeout(displayCallback, delayInMs);
    }

    stopDisplayTimer(): void {
        if (this.timeoutHandle) {
            clearTimeout(this.timeoutHandle);
            this.timeoutHandle = undefined;
        }

        this.display();
    }
}
