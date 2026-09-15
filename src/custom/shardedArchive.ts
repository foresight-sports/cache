// Sharded archive mode: N parallel `tar | zstd` streams instead of one.
//
// A single GNU tar stream is serial by construction: one reader walks the
// tree and one zstd filter compresses it (zstd -T0 parallelizes the
// compression, but the tar side and the extract side stay single-threaded).
// For a huge cache directory (Premier's Unity Library: hundreds of GB, ~50k
// mostly-large files) that serial pipeline costs ~12 min to create and ~10 min
// to extract per job, while the disks and the cores sit mostly idle.
//
// With `CACHE_ARCHIVE_SHARDS=N` (2..64) the save side enumerates every file
// under the cache paths, splits them into N size-balanced groups (greedy
// largest-first bin packing), and runs N independent
// `tar --no-recursion --files-from shard-<i>.txt | zstd` processes at once.
// Each part is uploaded to `<s3prefix>/<key>.shards/part-<i>.tzst`, and a
// small JSON manifest is written LAST as the object at `<s3prefix>/<key>` —
// the exact S3 key the legacy single archive uses — so a listing that finds
// the manifest always finds a complete set of parts, and a partial upload never
// looks like a valid cache entry. The restore side recognizes the manifest,
// downloads every part, and extracts all of them concurrently with the legacy
// extract command.
//
// Everything here is opt-in: with the knob unset (or 0 / 1 / invalid) the save
// and restore paths are byte-for-byte the legacy single-archive behavior, and a
// restore always accepts both shapes because it decides per entry by looking at
// the stored object, never at the knob.
import * as core from "@actions/core";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { CompressionMethod } from "../actionsCacheShims.js";
import {
    createTarFromFileList as defaultCreateTarFromFileList,
    extractTar as defaultExtractTar,
    getZstdCompressArgs
} from "./utils/uncompressedTar";

/** Env var: number of parallel tar|zstd shards (integer 2..64); anything else = legacy. */
export const ENV_ARCHIVE_SHARDS = "CACHE_ARCHIVE_SHARDS";
export const MIN_ARCHIVE_SHARDS = 2;
export const MAX_ARCHIVE_SHARDS = 64;

/** `format` field of the manifest object stored at the legacy archive key. */
export const SHARDED_ARCHIVE_FORMAT = "sharded-tzst-v1";
/** Appended to the entry's S3 key to form the part prefix: `<key>.shards/part-00.tzst`. */
export const SHARDS_KEY_SUFFIX = ".shards/";
/** Objects at or above this size are never inspected as a manifest candidate. */
export const SHARD_MANIFEST_MAX_BYTES = 1024 * 1024;

/** How many parts are downloaded at once on restore (each download is itself
 *  a highly concurrent multipart transfer, so a small fan-out is enough). */
export const SHARD_DOWNLOAD_CONCURRENCY = 2;

export interface ShardManifestEntry {
    /** Object name under `<key>.shards/`, e.g. `part-00.tzst`. */
    name: string;
    /** Compressed size of the part in bytes. */
    bytes: number;
    /** Number of regular files stored in the part. */
    files: number;
}

export interface ShardManifest {
    format: typeof SHARDED_ARCHIVE_FORMAT;
    shards: ShardManifestEntry[];
    totalBytes: number;
    totalFiles: number;
    createdAt: string;
}

export interface CacheFileEntry {
    /** Forward-slash path relative to tar's working directory (`-C`). */
    relPath: string;
    /** Size in bytes (balancing weight only). */
    size: number;
}

export interface CacheEntries {
    files: CacheFileEntry[];
    /** Directories with no children, kept so the restore recreates them. */
    emptyDirs: string[];
}

export interface ShardAssignment {
    /** Paths written to this shard's `--files-from` list, in list order. */
    relPaths: string[];
    /** Sum of the sizes of the regular files in this shard. */
    bytes: number;
    /** Number of regular files (empty directories are not counted). */
    files: number;
}

export interface ShardPart {
    index: number;
    name: string;
    /** Local path of the part file inside the archive staging directory. */
    path: string;
    /** Compressed size on disk. */
    bytes: number;
    files: number;
}

export interface ShardedArchiveResult {
    manifest: ShardManifest;
    parts: ShardPart[];
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested without a real tar/zstd/S3).
// ---------------------------------------------------------------------------

/**
 * Parse `CACHE_ARCHIVE_SHARDS`. Returns the shard count (2..64) when set to a
 * valid integer in range; returns 1 (= legacy single archive) when unset,
 * blank, 0, 1, or invalid. An out-of-range or non-integer value warns once per
 * call so a typo is visible in the log but can never break a save.
 */
export function getArchiveShardCount(
    env: NodeJS.ProcessEnv = process.env
): number {
    const raw = (env[ENV_ARCHIVE_SHARDS] ?? "").trim();
    if (raw === "") {
        return 1;
    }
    if (!/^\d+$/.test(raw)) {
        core.warning(
            `${ENV_ARCHIVE_SHARDS}='${raw}' is not an integer; using a single archive.`
        );
        return 1;
    }
    const count = Number(raw);
    if (count <= 1) {
        return 1;
    }
    if (count > MAX_ARCHIVE_SHARDS) {
        core.warning(
            `${ENV_ARCHIVE_SHARDS}=${count} exceeds the maximum of ${MAX_ARCHIVE_SHARDS}; using a single archive.`
        );
        return 1;
    }
    return count;
}

/** True for a shard part object (`<key>.shards/part-NN.tzst`), which must
 *  never be picked as "the newest cache entry" by a prefix listing. */
export function isShardPartObjectKey(key: string): boolean {
    return key.includes(SHARDS_KEY_SUFFIX);
}

/** Object name of shard `index`: `part-00.tzst`, `part-01.tzst`, ... */
export function shardPartName(index: number): string {
    return `part-${String(index).padStart(2, "0")}.tzst`;
}

/** File name of the `--files-from` list for shard `index`. */
export function shardListName(index: number): string {
    return `shard-${String(index).padStart(2, "0")}.txt`;
}

const PART_NAME_PATTERN = /^part-\d{2,}\.tzst$/;

/**
 * Decide whether an object body is a sharded-archive manifest. Anything that
 * is not UTF-8 JSON with `format === "sharded-tzst-v1"` and a well-formed,
 * non-empty `shards` array (safe names, numeric sizes) is NOT a manifest — a
 * legacy zstd archive, whose bytes never parse as JSON, returns undefined.
 */
export function parseShardManifest(
    body: Buffer | string
): ShardManifest | undefined {
    const text = typeof body === "string" ? body : body.toString("utf8");
    let parsed: unknown;
    try {
        parsed = JSON.parse(text);
    } catch {
        return undefined;
    }
    if (typeof parsed !== "object" || parsed === null) {
        return undefined;
    }
    const candidate = parsed as Record<string, unknown>;
    if (candidate.format !== SHARDED_ARCHIVE_FORMAT) {
        return undefined;
    }
    if (!Array.isArray(candidate.shards) || candidate.shards.length === 0) {
        return undefined;
    }
    const shards: ShardManifestEntry[] = [];
    for (const entry of candidate.shards as unknown[]) {
        if (typeof entry !== "object" || entry === null) {
            return undefined;
        }
        const { name, bytes, files } = entry as Record<string, unknown>;
        if (
            typeof name !== "string" ||
            !PART_NAME_PATTERN.test(name) ||
            typeof bytes !== "number" ||
            !Number.isFinite(bytes) ||
            bytes < 0 ||
            typeof files !== "number" ||
            !Number.isFinite(files) ||
            files < 0
        ) {
            return undefined;
        }
        shards.push({ name, bytes, files });
    }
    const totalBytes =
        typeof candidate.totalBytes === "number"
            ? candidate.totalBytes
            : shards.reduce((sum, shard) => sum + shard.bytes, 0);
    const totalFiles =
        typeof candidate.totalFiles === "number"
            ? candidate.totalFiles
            : shards.reduce((sum, shard) => sum + shard.files, 0);
    return {
        format: SHARDED_ARCHIVE_FORMAT,
        shards,
        totalBytes,
        totalFiles,
        createdAt:
            typeof candidate.createdAt === "string" ? candidate.createdAt : ""
    };
}

/**
 * Greedy largest-first bin packing: files are visited by size descending
 * (ties broken by path, so the result is deterministic for a given input set)
 * and each goes to the shard that currently holds the fewest bytes (ties to
 * the lowest index). Empty directories always go to shard 0. Shards that end
 * up with nothing (fewer entries than shards) are dropped, so the returned
 * array may be shorter than `shardCount` but is never empty when there is
 * anything to archive.
 */
export function assignFilesToShards(
    files: CacheFileEntry[],
    emptyDirs: string[],
    shardCount: number
): ShardAssignment[] {
    const count = Math.max(1, Math.floor(shardCount));
    const shards: ShardAssignment[] = [];
    for (let index = 0; index < count; index++) {
        shards.push({ relPaths: [], bytes: 0, files: 0 });
    }

    const ordered = files
        .slice()
        .sort(
            (a, b) =>
                b.size - a.size ||
                (a.relPath < b.relPath ? -1 : a.relPath > b.relPath ? 1 : 0)
        );

    for (const file of ordered) {
        let target = 0;
        for (let index = 1; index < shards.length; index++) {
            if (shards[index].bytes < shards[target].bytes) {
                target = index;
            }
        }
        shards[target].relPaths.push(file.relPath);
        shards[target].bytes += file.size;
        shards[target].files += 1;
    }

    for (const dir of emptyDirs.slice().sort()) {
        shards[0].relPaths.push(dir);
    }

    return shards.filter(shard => shard.relPaths.length > 0);
}

/**
 * Cap zstd's thread count per shard so N concurrent compressors do not
 * oversubscribe the box: a literal `-T0` (all cores) becomes
 * `-T<max(1, floor(cpus / shardCount))>`. An explicit thread count the
 * operator chose (`-T8`) is left alone, and so is every other token.
 */
export function rewriteZstdThreadsForShards(
    compressArgs: string,
    shardCount: number,
    cpuCount: number = os.cpus().length
): string {
    const cores = Number.isFinite(cpuCount) && cpuCount > 0 ? cpuCount : 1;
    const perShard = Math.max(
        1,
        Math.floor(cores / Math.max(1, Math.floor(shardCount)))
    );
    return compressArgs
        .split(/\s+/)
        .filter(token => token !== "")
        .map(token => (token === "-T0" ? `-T${perShard}` : token))
        .join(" ");
}

/**
 * A `--files-from` list is newline-separated, so a path containing a newline
 * cannot be expressed. Library paths never do; reject loudly rather than
 * silently splitting one entry into two bogus ones.
 */
export function assertShardablePaths(relPaths: string[]): void {
    for (const relPath of relPaths) {
        if (relPath.includes("\n") || relPath.includes("\r")) {
            throw new Error(
                `Cache path contains a newline and cannot be written to a shard file list: ${JSON.stringify(
                    relPath
                )}`
            );
        }
    }
}

/** Run `worker` over `items` with at most `limit` in flight; rejects with the
 *  first failure after every started worker has settled. */
export async function mapWithConcurrency<T, R>(
    items: T[],
    limit: number,
    worker: (item: T, index: number) => Promise<R>
): Promise<R[]> {
    const results: R[] = new Array(items.length);
    const errors: Error[] = [];
    let next = 0;
    const lanes = Math.max(1, Math.min(limit, items.length));
    const runLane = async (): Promise<void> => {
        while (next < items.length) {
            const index = next++;
            try {
                results[index] = await worker(items[index], index);
            } catch (error) {
                errors.push(error as Error);
                return;
            }
        }
    };
    await Promise.all(Array.from({ length: lanes }, () => runLane()));
    if (errors.length > 0) {
        throw errors[0];
    }
    return results;
}

// ---------------------------------------------------------------------------
// File enumeration.
// ---------------------------------------------------------------------------

function toManifestPath(relativePath: string): string {
    return relativePath.split(path.sep).join("/");
}

function getWorkingDirectory(): string {
    return process.env["GITHUB_WORKSPACE"] ?? process.cwd();
}

/**
 * Enumerate every regular file (and every empty directory) under the resolved
 * cache paths, as forward-slash paths relative to tar's working directory —
 * the same normalization the legacy manifest uses, so a sharded restore lands
 * files exactly where a legacy one would. Directories are walked with stat()
 * (not lstat) so Windows junctions and directory symlinks are descended, as
 * the legacy junction expansion does; a vanished or broken entry is skipped.
 */
export function enumerateCacheEntries(
    cachePaths: string[],
    workspaceRoot: string = getWorkingDirectory()
): CacheEntries {
    const files: CacheFileEntry[] = [];
    const emptyDirs: string[] = [];
    const seen = new Set<string>();

    const relative = (absolutePath: string): string => {
        const rel = path.relative(workspaceRoot, absolutePath);
        return rel === "" ? "." : toManifestPath(rel);
    };

    const walk = (absoluteDir: string): void => {
        let children: fs.Dirent[];
        try {
            children = fs.readdirSync(absoluteDir, { withFileTypes: true });
        } catch {
            return;
        }
        if (children.length === 0) {
            const rel = relative(absoluteDir);
            if (!seen.has(rel)) {
                seen.add(rel);
                emptyDirs.push(rel);
            }
            return;
        }
        for (const child of children) {
            const absoluteChild = path.join(absoluteDir, child.name);
            let stats: fs.Stats;
            try {
                stats = fs.statSync(absoluteChild);
            } catch {
                continue;
            }
            if (stats.isDirectory()) {
                walk(absoluteChild);
            } else {
                const rel = relative(absoluteChild);
                if (!seen.has(rel)) {
                    seen.add(rel);
                    files.push({ relPath: rel, size: stats.size });
                }
            }
        }
    };

    for (const cachePath of cachePaths) {
        const absolutePath = path.resolve(workspaceRoot, cachePath);
        let stats: fs.Stats;
        try {
            stats = fs.statSync(absolutePath);
        } catch {
            continue;
        }
        if (stats.isDirectory()) {
            walk(absolutePath);
        } else {
            const rel = relative(absolutePath);
            if (!seen.has(rel)) {
                seen.add(rel);
                files.push({ relPath: rel, size: stats.size });
            }
        }
    }

    return { files, emptyDirs };
}

// ---------------------------------------------------------------------------
// Create / extract orchestration.
// ---------------------------------------------------------------------------

/** Injectable seams so the orchestration is unit-testable without tar/zstd. */
export interface ShardedArchiveDeps {
    createTarFromFileList: (
        archiveFolder: string,
        fileListName: string,
        archiveName: string,
        compressProgram: string
    ) => Promise<void>;
    extractTar: (archivePath: string) => Promise<void>;
    cpuCount: () => number;
}

const defaultDeps: ShardedArchiveDeps = {
    createTarFromFileList: defaultCreateTarFromFileList,
    extractTar: archivePath =>
        defaultExtractTar(archivePath, CompressionMethod.Zstd),
    cpuCount: () => os.cpus().length
};

function formatMb(bytes: number): string {
    return `${Math.round(bytes / (1024 * 1024))} MB`;
}

function elapsedSeconds(startedAtMs: number): string {
    return ((Date.now() - startedAtMs) / 1000).toFixed(1);
}

async function settleAll(
    tasks: Promise<void>[],
    describeFailure: (index: number, error: Error) => string
): Promise<void> {
    const outcomes = await Promise.allSettled(tasks);
    const failures = outcomes
        .map((outcome, index) => ({ outcome, index }))
        .filter(({ outcome }) => outcome.status === "rejected")
        .map(({ outcome, index }) =>
            describeFailure(
                index,
                (outcome as PromiseRejectedResult).reason as Error
            )
        );
    if (failures.length > 0) {
        throw new Error(failures.join("; "));
    }
}

/**
 * Archive `cachePaths` as `shardCount` size-balanced zstd tar parts inside
 * `archiveFolder`, all created concurrently. Returns the manifest to store at
 * the entry key plus the local part files to upload. Any shard failure rejects
 * after every shard has settled (so no tar is left running while the caller
 * cleans up).
 */
export async function createShardedArchive(
    archiveFolder: string,
    cachePaths: string[],
    shardCount: number,
    deps: ShardedArchiveDeps = defaultDeps
): Promise<ShardedArchiveResult> {
    const workspaceRoot = getWorkingDirectory();

    const enumerateStartedAt = Date.now();
    const { files, emptyDirs } = enumerateCacheEntries(
        cachePaths,
        workspaceRoot
    );
    const rawBytes = files.reduce((sum, file) => sum + file.size, 0);
    core.info(
        `Sharded archive: enumerated ${files.length} files (${formatMb(
            rawBytes
        )}) and ${emptyDirs.length} empty directories in ${elapsedSeconds(
            enumerateStartedAt
        )}s.`
    );

    const assignments = assignFilesToShards(files, emptyDirs, shardCount);
    if (assignments.length === 0) {
        throw new Error(
            "Path Validation Error: no files found under the cache path(s), hence no cache is being saved."
        );
    }
    assertShardablePaths(assignments.flatMap(shard => shard.relPaths));

    const compressArgs = rewriteZstdThreadsForShards(
        getZstdCompressArgs(),
        assignments.length,
        deps.cpuCount()
    );
    const compressProgram = `zstd ${compressArgs}`;
    core.info(
        `Sharded archive: ${assignments.length} shard(s) requested via ${ENV_ARCHIVE_SHARDS}=${shardCount}; compressor per shard: ${compressProgram}`
    );

    for (let index = 0; index < assignments.length; index++) {
        fs.writeFileSync(
            path.join(archiveFolder, shardListName(index)),
            assignments[index].relPaths.join("\n") + "\n"
        );
    }

    const tarStartedAt = Date.now();
    await settleAll(
        assignments.map((_, index) =>
            deps.createTarFromFileList(
                archiveFolder,
                shardListName(index),
                shardPartName(index),
                compressProgram
            )
        ),
        (index, error) => `shard ${index} tar failed: ${error.message}`
    );
    const tarSeconds = elapsedSeconds(tarStartedAt);

    const parts: ShardPart[] = assignments.map((shard, index) => {
        const partPath = path.join(archiveFolder, shardPartName(index));
        return {
            index,
            name: shardPartName(index),
            path: partPath,
            bytes: fs.statSync(partPath).size,
            files: shard.files
        };
    });
    for (const part of parts) {
        const raw = assignments[part.index].bytes;
        core.info(
            `  ${part.name}: ${part.files} files, ${formatMb(
                raw
            )} raw -> ${formatMb(part.bytes)} compressed (${part.bytes} B)`
        );
    }

    const totalBytes = parts.reduce((sum, part) => sum + part.bytes, 0);
    const totalFiles = parts.reduce((sum, part) => sum + part.files, 0);
    core.info(
        `Sharded archive: ${parts.length} part(s), ${formatMb(
            totalBytes
        )} compressed (${totalBytes} B) for ${totalFiles} files, created in ${tarSeconds}s.`
    );

    const manifest: ShardManifest = {
        format: SHARDED_ARCHIVE_FORMAT,
        shards: parts.map(part => ({
            name: part.name,
            bytes: part.bytes,
            files: part.files
        })),
        totalBytes,
        totalFiles,
        createdAt: new Date().toISOString()
    };

    return { manifest, parts };
}

/**
 * Extract every downloaded part concurrently with the legacy extract command
 * (each part is an ordinary zstd tar, so the decompressor is unchanged).
 * Rejects after every extraction has settled if any part failed.
 */
export async function extractShardedArchive(
    partPaths: string[],
    deps: ShardedArchiveDeps = defaultDeps
): Promise<void> {
    const startedAt = Date.now();
    await settleAll(
        partPaths.map(partPath => deps.extractTar(partPath)),
        (index, error) =>
            `part ${path.basename(partPaths[index])} extract failed: ${
                error.message
            }`
    );
    core.info(
        `Sharded archive: extracted ${
            partPaths.length
        } part(s) concurrently in ${elapsedSeconds(startedAt)}s.`
    );
}
