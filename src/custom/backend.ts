import * as core from "@actions/core";
import {
    DeleteObjectsCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    PutObjectCommand,
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
import {
    getShardRetentionHours,
    isShardPartObjectKey,
    LEGACY_SHARD_GENERATION,
    parseShardManifest,
    planShardGenerationCleanup,
    PreviousEntryState,
    SHARD_MANIFEST_MAX_BYTES,
    shardGenerationKeyPrefix,
    ShardManifest,
    ShardObjectListing,
    ShardPart,
    shardsKeyPrefix
} from "./shardedArchive";
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

/** Minimal shape of a ListObjectsV2 `Contents` element this backend reads. */
export interface ListedObject {
    Key?: string;
    LastModified?: Date;
}

/**
 * Pick the most recently modified object among a prefix listing, ignoring
 * shard part objects (`<key>.shards/<generation>/part-NN.tzst`, or the
 * legacy `<key>.shards/part-NN.tzst`). A sharded entry stores its parts
 * under the entry key plus `.shards/`, so they match the same restore-key
 * prefix and are uploaded BEFORE the manifest; without this filter a part
 * could be chosen as "the newest key" and handed to the download as if it
 * were a whole archive. Returns undefined when nothing eligible is listed.
 */
export function selectNewestArchiveObject(
    contents: ListedObject[]
): string | undefined {
    const candidates = contents.filter(
        object => !!object.Key && !isShardPartObjectKey(object.Key)
    );
    if (candidates.length === 0) {
        return undefined;
    }
    // Sort keys by LastModified time in descending order
    candidates.sort((a, b) => Number(b.LastModified) - Number(a.LastModified));
    return candidates[0].Key;
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
            const s3Path = selectNewestArchiveObject(Contents); // The most recent key
            if (s3Path) {
                cacheEntry.cacheKey = s3Path.replace(`${s3Prefix}/`, "");
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

/**
 * Probe the object a cache entry points at and return its sharded-archive
 * manifest when it is one, or undefined for a legacy single archive (or on
 * any error, so the legacy download path can surface its own clear failure).
 * A manifest is a small JSON object; a HeadObject first keeps this a cheap
 * check (multi-GB legacy archives are never fetched here), and only a body
 * under SHARD_MANIFEST_MAX_BYTES that parses with the expected `format` is
 * treated as sharded.
 */
export async function getShardManifest(
    archiveLocation: string
): Promise<ShardManifest | undefined> {
    if (!bucketName) {
        return undefined;
    }
    const bucket = bucketName;
    const objectKey = new URL(archiveLocation).pathname.slice(1);

    let contentLength: number | undefined;
    try {
        const head = await s3Client.send(
            new HeadObjectCommand({ Bucket: bucket, Key: objectKey })
        );
        contentLength = head.ContentLength;
    } catch (error) {
        core.debug(
            `Sharded-archive probe skipped (HeadObject failed: ${
                (error as Error).message
            }); treating the entry as a single archive.`
        );
        return undefined;
    }
    if (
        typeof contentLength !== "number" ||
        contentLength <= 0 ||
        contentLength > SHARD_MANIFEST_MAX_BYTES
    ) {
        return undefined;
    }

    try {
        const object = await s3Client.send(
            new GetObjectCommand({ Bucket: bucket, Key: objectKey })
        );
        const body = await readObjectBody(object.Body);
        return parseShardManifest(body);
    } catch (error) {
        core.debug(
            `Sharded-archive probe skipped (GetObject failed: ${
                (error as Error).message
            }); treating the entry as a single archive.`
        );
        return undefined;
    }
}

// Collect a GetObject body as a UTF-8 string: the SDK mixes in
// transformToString() on Node streams; fall back to draining the stream so a
// bare Readable (e.g. a test double) works too.
async function readObjectBody(body: unknown): Promise<string> {
    if (body === undefined || body === null) {
        return "";
    }
    const mixed = body as { transformToString?: () => Promise<string> };
    if (typeof mixed.transformToString === "function") {
        return mixed.transformToString();
    }
    const chunks: Buffer[] = [];
    for await (const chunk of body as AsyncIterable<Buffer | string>) {
        chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    return Buffer.concat(chunks).toString("utf8");
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

    const s3Prefix = getS3Prefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const s3Key = `${s3Prefix}/${key}`;

    await uploadArchiveObject(s3Key, archivePath, uploadChunkSize);

    core.info(`Cache saved successfully.`);
}

/** A small object read back whole (a manifest candidate), or the fact that
 *  the object exists but is too large to be one (a legacy single archive). */
export interface EntryObjectRead {
    /** Body bytes, absent when the object exceeds SHARD_MANIFEST_MAX_BYTES. */
    body?: Buffer;
    lastModified?: Date;
}

/** S3 operations of a sharded save, injectable so the save/cleanup ordering
 *  can be unit-tested against an in-memory bucket. */
export interface ShardedSaveDeps {
    /**
     * Read the object at `s3Key` (HEAD, then GET only when it is small
     * enough to be a manifest). Resolves undefined when there is no such
     * object; rejects on any other failure.
     */
    readEntryObject: (s3Key: string) => Promise<EntryObjectRead | undefined>;
    /** Upload one local part file to `s3Key` (engine chain by default). */
    uploadObject: (
        s3Key: string,
        archivePath: string,
        uploadChunkSize?: number
    ) => Promise<void>;
    /** Write a small JSON body to `s3Key`. */
    putJsonObject: (s3Key: string, body: string) => Promise<void>;
    /** Every object under `prefix` (all pages) with its LastModified. */
    listObjects: (prefix: string) => Promise<ShardObjectListing[]>;
    /** Delete the given object keys (at most 1000 per call). */
    deleteObjectKeys: (keys: string[]) => Promise<void>;
    /** Clock used for the retention arithmetic. */
    now: () => Date;
}

function isNotFoundError(error: unknown): boolean {
    const typed = error as {
        name?: string;
        $metadata?: { httpStatusCode?: number };
    };
    return (
        typed?.name === "NotFound" ||
        typed?.name === "NoSuchKey" ||
        typed?.$metadata?.httpStatusCode === 404
    );
}

export const defaultShardedSaveDeps: ShardedSaveDeps = {
    readEntryObject: async s3Key => {
        let head: { ContentLength?: number; LastModified?: Date };
        try {
            head = await s3Client.send(
                new HeadObjectCommand({ Bucket: bucketName, Key: s3Key })
            );
        } catch (error) {
            if (isNotFoundError(error)) {
                return undefined;
            }
            throw error;
        }
        const read: EntryObjectRead = { lastModified: head.LastModified };
        if (
            typeof head.ContentLength !== "number" ||
            head.ContentLength <= 0 ||
            head.ContentLength > SHARD_MANIFEST_MAX_BYTES
        ) {
            return read;
        }
        try {
            const object = await s3Client.send(
                new GetObjectCommand({ Bucket: bucketName, Key: s3Key })
            );
            read.body = Buffer.from(await readObjectBody(object.Body), "utf8");
            read.lastModified = object.LastModified ?? read.lastModified;
        } catch (error) {
            if (isNotFoundError(error)) {
                return undefined;
            }
            throw error;
        }
        return read;
    },
    uploadObject: (s3Key, archivePath, uploadChunkSize) =>
        uploadArchiveObject(s3Key, archivePath, uploadChunkSize),
    putJsonObject: async (s3Key, body) => {
        await s3Client.send(
            new PutObjectCommand({
                Bucket: bucketName,
                Key: s3Key,
                Body: body,
                ContentType: "application/json"
            })
        );
    },
    listObjects: async prefix => {
        const objects: ShardObjectListing[] = [];
        let continuationToken: string | undefined;
        do {
            const page = await s3Client.send(
                new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: prefix,
                    ContinuationToken: continuationToken
                })
            );
            for (const object of page.Contents ?? []) {
                if (object.Key) {
                    objects.push({
                        key: object.Key,
                        lastModified: object.LastModified
                    });
                }
            }
            continuationToken = page.IsTruncated
                ? page.NextContinuationToken
                : undefined;
        } while (continuationToken);
        return objects;
    },
    deleteObjectKeys: async keys => {
        if (keys.length === 0) {
            return;
        }
        await s3Client.send(
            new DeleteObjectsCommand({
                Bucket: bucketName,
                Delete: {
                    Objects: keys.map(Key => ({ Key })),
                    Quiet: true
                }
            })
        );
    },
    now: () => new Date()
};

/** S3 DeleteObjects accepts at most this many keys per request. */
const DELETE_BATCH_SIZE = 1000;

/**
 * Inspect the object currently stored at the entry key BEFORE a save uploads
 * anything, so the cleanup that follows the publish knows which generation
 * it superseded and when that one became current. Never throws: an
 * uninspectable entry is reported as `unknown`, which makes the cleanup hold
 * back everything.
 */
export async function readPreviousEntryState(
    s3Key: string,
    deps: ShardedSaveDeps = defaultShardedSaveDeps
): Promise<PreviousEntryState> {
    try {
        const read = await deps.readEntryObject(s3Key);
        if (read === undefined) {
            core.info(
                `Sharded cache: no previous entry at ${s3Key}; this save creates it.`
            );
            return { kind: "absent" };
        }
        const manifest =
            read.body === undefined ? undefined : parseShardManifest(read.body);
        const stamp = read.lastModified?.toISOString() ?? "unknown time";
        if (manifest === undefined) {
            core.info(
                `Sharded cache: previous entry at ${s3Key} is a single archive (published ${stamp}).`
            );
            return { kind: "single", lastModified: read.lastModified };
        }
        const generation = manifest.generation ?? LEGACY_SHARD_GENERATION;
        core.info(
            `Sharded cache: previous entry at ${s3Key} is generation ${generation} (published ${stamp}).`
        );
        return { kind: "sharded", generation, lastModified: read.lastModified };
    } catch (error) {
        core.info(
            `Sharded cache: could not inspect the previous entry at ${s3Key} (${
                (error as Error).message
            }); stale part cleanup will be deferred.`
        );
        return { kind: "unknown" };
    }
}

export interface ShardCleanupOutcome {
    /** Part objects deleted. */
    deletedObjects: number;
    /** Generations fully deleted. */
    deletedGenerations: number;
    /** Generations protected outright (the new one, the superseded one). */
    keptGenerations: number;
    /** Generations left for a later save (young, or blanket hold). */
    deferredGenerations: number;
}

const NO_CLEANUP: ShardCleanupOutcome = {
    deletedObjects: 0,
    deletedGenerations: 0,
    keptGenerations: 0,
    deferredGenerations: 0
};

/**
 * Best-effort, deferred removal of generations under `<s3Key>.shards/` that
 * nothing can still be using. Called only AFTER the new manifest is in
 * place. A generation is deleted only when it is neither the one just
 * published nor the one just superseded (`previous`), the superseded entry
 * itself became current more than `retentionMs` ago, and every object of
 * the generation is older than `retentionMs` — see
 * planShardGenerationCleanup for why each condition is needed. Errors are
 * logged and swallowed: stale objects cost storage, not correctness.
 */
export async function cleanupOtherShardGenerations(
    s3Key: string,
    generation: string,
    previous: PreviousEntryState,
    retentionMs: number,
    deps: ShardedSaveDeps = defaultShardedSaveDeps
): Promise<ShardCleanupOutcome> {
    const allPrefix = shardsKeyPrefix(s3Key);
    const retentionHours = (retentionMs / 3600000).toFixed(1);
    try {
        const objects = await deps.listObjects(allPrefix);
        const plan = planShardGenerationCleanup(
            objects,
            allPrefix,
            generation,
            previous,
            retentionMs,
            deps.now()
        );
        for (
            let start = 0;
            start < plan.deleteKeys.length;
            start += DELETE_BATCH_SIZE
        ) {
            await deps.deleteObjectKeys(
                plan.deleteKeys.slice(start, start + DELETE_BATCH_SIZE)
            );
        }
        const summary = `deleted ${plan.deletedGenerations.length} generation(s) (${
            plan.deleteKeys.length
        } object(s)), kept ${plan.keptGenerations.length} (${plan.keptGenerations.join(
            ", "
        )}), deferred ${plan.deferredGenerations.length}${
            plan.deferredGenerations.length > 0
                ? ` (${plan.deferredGenerations.join(", ")})`
                : ""
        }; retention ${retentionHours} h`;
        if (plan.deferredReason !== undefined) {
            core.info(
                `Sharded cache: stale part cleanup under ${allPrefix} deferred because ${plan.deferredReason}: ${summary}.`
            );
        } else {
            core.info(
                `Sharded cache: stale part cleanup under ${allPrefix}: ${summary}.`
            );
        }
        return {
            deletedObjects: plan.deleteKeys.length,
            deletedGenerations: plan.deletedGenerations.length,
            keptGenerations: plan.keptGenerations.length,
            deferredGenerations: plan.deferredGenerations.length
        };
    } catch (error) {
        core.info(
            `Sharded cache: stale part cleanup under ${allPrefix} skipped (${
                (error as Error).message
            }); the entry is complete regardless.`
        );
        return NO_CLEANUP;
    }
}

/**
 * Save a sharded cache entry: note what is currently stored at the entry
 * key, upload every part to the generation-scoped key the manifest records
 * (`<s3prefix>/<key>.shards/<generation>/<part name>`, through the same
 * engine chain as a single archive), THEN write the JSON manifest as the
 * object at `<s3prefix>/<key>` — the very key a legacy archive would occupy
 * — and finally retire generations that have been superseded for longer
 * than the retention window. The manifest goes last so a listing can never
 * find a manifest whose parts are still missing, and because a generation's
 * part keys are unique, a replacement save that dies mid-way leaves the
 * previous manifest AND every object it references intact: it only adds
 * orphan parts, which the entry lookup ignores and a later save retires once
 * they are old enough. Nothing is ever deleted when the publish fails.
 */
export async function saveShardedCache(
    key: string,
    paths: string[],
    parts: ShardPart[],
    manifest: ShardManifest,
    {
        compressionMethod,
        enableCrossOsArchive,
        uploadChunkSize
    }: {
        compressionMethod: CompressionMethod;
        enableCrossOsArchive: boolean;
        uploadChunkSize?: number;
    },
    deps: ShardedSaveDeps = defaultShardedSaveDeps
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    if (!region) {
        throw new Error("Environment variable RUNS_ON_AWS_REGION not set");
    }

    const bucket = bucketName;
    const s3Prefix = getS3Prefix(paths, {
        compressionMethod,
        enableCrossOsArchive
    });
    const s3Key = `${s3Prefix}/${key}`;

    core.info(
        `Cache Size: ~${Math.round(
            manifest.totalBytes / (1024 * 1024)
        )} MB (${manifest.totalBytes} B) across ${parts.length} part(s)`
    );

    if (!manifest.generation) {
        throw new Error(
            "Sharded cache manifest has no generation; refusing to overwrite parts in place."
        );
    }
    const generationPrefix = shardGenerationKeyPrefix(key, manifest.generation);
    for (const part of parts) {
        if (!part.key.startsWith(generationPrefix)) {
            throw new Error(
                `Sharded cache part ${part.name} key ${part.key} is not under generation ${manifest.generation} of ${key}.`
            );
        }
    }
    core.info(`Sharded cache generation: ${manifest.generation}`);

    // Remember what this save supersedes before touching the bucket: the
    // cleanup after the publish must protect that generation (its readers
    // may still be downloading) and needs to know when it became current.
    const previous = await readPreviousEntryState(s3Key, deps);

    // Parts are uploaded one after another: each upload is already a
    // many-way multipart transfer that saturates the send side, and the
    // aws-cli engine rewrites its shared config before every transfer.
    // Each part goes to the exact key its manifest entry records.
    const uploadStartedAt = Date.now();
    for (const part of parts) {
        await deps.uploadObject(
            `${s3Prefix}/${part.key}`,
            part.path,
            uploadChunkSize
        );
    }

    const manifestBody = JSON.stringify(manifest);
    core.info(`Uploading shard manifest to ${bucket}/${s3Key}`);
    await deps.putJsonObject(s3Key, manifestBody);

    core.info(
        `Cache saved successfully (${parts.length} part(s) + manifest uploaded in ${(
            (Date.now() - uploadStartedAt) /
            1000
        ).toFixed(1)}s).`
    );

    // Only now, with the new manifest visible, retire what nothing can
    // still be using. Reached only when the publish above succeeded.
    const retentionMs = getShardRetentionHours() * 3600000;
    await cleanupOtherShardGenerations(
        s3Key,
        manifest.generation,
        previous,
        retentionMs,
        deps
    );
}

/**
 * Upload one local archive file to `s3Key` through the engine chain
 * (s5cmd -> aws-cli -> node lib-storage). Shared by the single-archive save
 * and every part of a sharded save.
 */
async function uploadArchiveObject(
    s3Key: string,
    archivePath: string,
    uploadChunkSize?: number
): Promise<void> {
    if (!bucketName) {
        throw new Error("Environment variable RUNS_ON_S3_BUCKET_CACHE not set");
    }

    // Narrow the module-level env const to non-undefined for use inside the
    // closure below (the guard above already asserted it).
    const bucket = bucketName;

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
