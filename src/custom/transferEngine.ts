// Transfer-engine fallback chain for the single cache archive (cache.tzst).
//
// Single-process Node TLS caps out well below the NIC (~2.5 Gbps / ~330 MB/s on
// a 16xlarge, ~100 MB/s on a 4xlarge) and adding Node concurrency scales
// sub-linearly (one event loop). Native multi-threaded S3 engines break that
// ceiling by using the otherwise-idle cores:
//
//   1. s5cmd  — Go, goroutine worker pool, no GIL/event-loop ceiling.
//   2. aws-cli v2 — CRT (C++) transfer manager, native threads.
//   3. node   — the always-present @actions/lib-storage upload / hand-rolled
//               ranged downloader; the last-resort safety net.
//
// Engines are tried in order; a missing binary is skipped and a failing engine
// falls through to the next, so a transfer never hard-fails just because s5cmd
// or aws-cli is absent or errors. This mirrors the pattern Premier already uses
// for its R2 publish (s5cmd primary, aws-cli fallback).
//
// The engine ONLY moves the single archive file to/from s3://<bucket>/<key>.
// Archive handling (zstd, the junction-following manifest, versionSalt, the
// adaptive part-size floor) is unchanged and lives in cache.ts / backend.ts.
// Because every engine transfers byte-identical archive bytes to/from the same
// content-addressed key, a cache written by any engine is restorable by any
// engine (integrity is the content-hash baked into the key + a size/extract
// check downstream, never the multipart ETag, which legitimately differs
// between engines that pick different part boundaries).
import * as core from "@actions/core";
import { exec } from "@actions/exec";
import * as io from "@actions/io";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

export type TransferDirection = "upload" | "download";
export type TransferEngineName = "s5cmd" | "aws-cli" | "node";

export interface TransferParams {
    /** Bucket the archive lives in (no s3:// prefix). */
    bucket: string;
    /** Full S3 object key, exactly as the node path derives it. */
    key: string;
    /** Local path of the single archive file. */
    archivePath: string;
    /** Custom S3 endpoint (RunsOn / R2 / MinIO); omitted for real AWS S3. */
    endpoint?: string;
    /** AWS region; defaults to "auto" when unset (matches R2 usage). */
    region?: string;
    /** Path-style addressing (custom endpoints). s5cmd infers this from the
     *  endpoint automatically; aws-cli is told explicitly. */
    forcePathStyle?: boolean;
    /** Archive size in bytes when known (upload); used for throughput logging
     *  and part-size selection. Downloads may not know it up front. */
    sizeBytes?: number;
    /** Part size in MiB when known (upload's adaptive effective part size);
     *  passed to the engines so a single large file is multiparted sensibly. */
    partSizeMb?: number;
}

// ============================================================================
// Single-file transfer concurrency & part sizing.
// ============================================================================
//
// The cache is ONE large archive (cache.tzst). What actually parallelizes a
// single large object differs by engine (verified against s5cmd docs, not
// guessed):
//   - s5cmd:  `cp --concurrency N` is "the number of parts that will be
//             uploaded or downloaded in parallel for a single file" (default 5)
//             and `--part-size` (MiB, default 50) is each part's size. The
//             global `--numworkers` pool only limits how many SEPARATE objects
//             run at once ("if you are uploading 100 files ... --numworkers ...
//             limit the number of files concurrently uploaded"), so for our one
//             object it is ~irrelevant, and concurrency is independent of it.
//             We therefore pin --numworkers and tune --concurrency/--part-size.
//   - aws-cli: `default.s3.max_concurrent_requests` is the single-file part
//             parallelism and `default.s3.multipart_chunksize` the part size.
//
// Upload and download are DIFFERENT workloads and get DIFFERENT defaults:
//
//   Upload   — CPU/TLS-bound on the send side (measured ~1.6 MB/s per
//              connection; a 64-vCPU box at 256 parts hit 417 MB/s ~= 66% of a
//              ~5 Gbps NIC). Driving many concurrent TLS PUTs needs cores, so
//              upload part-parallelism SCALES WITH CORES (high floor + ceiling).
//
//   Download — I/O-bound parallel range-GETs of one object (measured ~3.2 MB/s
//              per connection; a 16-vCPU box got only the old cores*4 = 64 parts
//              and sat at 207 MB/s ~= 33% of NIC, connections unsaturated).
//              Local CPU is NOT the bottleneck, so the old cores*4 STARVED small
//              runners. Download part-parallelism is therefore DECOUPLED from
//              vCPU with a high fixed floor (256): at ~3.2 MB/s per connection,
//              256 parts is ~820 MB/s of demand — enough to fill a 5 Gbps
//              (~625 MB/s) NIC with headroom on even a 16-vCPU box; bigger boxes
//              (bigger NIC) scale up to a 512 ceiling, and any runner can
//              override via env.
//
// Peak client memory for a native multipart transfer is ~concurrency * part-size
// (every in-flight part is buffered). Upload keeps the backend's adaptive part
// size (64 MiB for a 44 GB archive => ~700 parts, well under S3's 10k-part cap).
// Download uses a SMALLER 16 MiB part so 256-way concurrency peaks at ~4 GiB
// while still splitting the object into many parts.

// s5cmd global worker pool. A single-object transfer uses ~1 worker, so this is
// pinned (not tuned); kept >= the chosen concurrency as a cheap hedge.
const S5CMD_NUMWORKERS_MIN = 256;

// Upload part-parallelism: cores-scaled (send side is CPU/TLS-bound).
const UPLOAD_CONCURRENCY_MIN = 96;
const UPLOAD_CONCURRENCY_MAX = 384;
const UPLOAD_CONCURRENCY_FACTOR = 8;

// Download part-parallelism: DECOUPLED from cores (I/O-bound range-GETs). High
// fixed floor so a small runner still fills its NIC; scales up for big boxes.
const DOWNLOAD_CONCURRENCY_MIN = 256;
const DOWNLOAD_CONCURRENCY_MAX = 512;
const DOWNLOAD_CONCURRENCY_FACTOR = 8;

// Download multipart part size (MiB). Smaller than the upload part size so the
// concurrency*part-size peak buffer stays bounded (256 * 16 MiB = 4 GiB).
const DOWNLOAD_PART_SIZE_MB = 16;

// S3 rejects a non-final multipart part below 5 MiB (EntityTooSmall); floor
// every engine part size here for safety.
const MIN_PART_SIZE_MB = 5;

// aws-cli's multipart_chunksize fallback when no part size is supplied.
const AWS_CLI_DEFAULT_CHUNK_MB = 64;

// Operator escape hatches (env var names) + a sanity ceiling for overrides.
const ENV_UPLOAD_CONCURRENCY = "CACHE_UPLOAD_CONCURRENCY";
const ENV_DOWNLOAD_CONCURRENCY = "CACHE_DOWNLOAD_CONCURRENCY";
const ENV_DOWNLOAD_PART_SIZE_MB = "CACHE_DOWNLOAD_PART_SIZE";
const CONCURRENCY_HARD_MAX = 1024;

// ---------------------------------------------------------------------------
// STREAMING s5cmd `cat` part-parallelism — DISTINCT from the 256-way `cp`
// download concurrency above, on purpose.
//
// `cat` fetches up to `--concurrency` parts in parallel but must emit them to
// stdout IN ORDER, so a SLOW consumer (the filesystem-bound tar+zstd extract of
// tens of thousands of small files on Windows NTFS+Defender) makes the ordered
// writer buffer every out-of-order-completed part while it waits. At the high
// `cp` download concurrency (256) that buffer + the many idle connections grew
// until the writer truncated the stream — the real-runner failure (s5cmd cat
// exit 1, tar "Unexpected EOF in archive"). So the streamed `cat` path gets its
// OWN small, bounded concurrency + modest part size: the peak buffer is
// ~concurrency * part-size (default 6 * 16 MiB ~= 96 MiB) and only a handful of
// connections idle behind the slow tar. aws-cli's `cp - ` (bounded ring buffer)
// is the PRIMARY streaming engine; this bounded `cat` is the secondary.
const STREAM_S5CMD_CONCURRENCY_DEFAULT = 6;
const STREAM_S5CMD_PART_SIZE_MB_DEFAULT = 16;
// s5cmd global worker pool for the single-object `cat`: only needs to cover the
// in-flight parts, so keep it small (NOT the cp path's 256) so the log/footprint
// match the low-concurrency intent. Pinned >= concurrency as a cheap hedge.
const STREAM_S5CMD_NUMWORKERS_MIN = 16;
const ENV_STREAM_S5CMD_CONCURRENCY = "CACHE_STREAM_S5CMD_CONCURRENCY";
const ENV_STREAM_S5CMD_PART_SIZE_MB = "CACHE_STREAM_S5CMD_PART_SIZE";

function scaleToCores(
    cpuCount: number,
    min: number,
    max: number,
    factor: number
): number {
    const cores = Number.isFinite(cpuCount) && cpuCount > 0 ? cpuCount : 1;
    return Math.min(max, Math.max(min, Math.floor(cores) * factor));
}

/** Parse a positive integer env value; undefined/blank/non-positive -> undefined. */
function parseEnvInt(value: string | undefined): number | undefined {
    if (value === undefined || value.trim() === "") {
        return undefined;
    }
    const parsed = Number(value);
    if (!Number.isFinite(parsed) || parsed <= 0) {
        return undefined;
    }
    return Math.floor(parsed);
}

/**
 * UPLOAD single-file part-parallelism (`s5cmd cp --concurrency` / aws-cli
 * `max_concurrent_requests`). Scales with cores because the send side is
 * CPU/TLS-bound; env-overridable via CACHE_UPLOAD_CONCURRENCY (floor 96,
 * ceiling 384, cores*8).
 */
export function computeUploadConcurrency(
    cpuCount: number = os.cpus().length,
    env: NodeJS.ProcessEnv = process.env
): number {
    const override = parseEnvInt(env[ENV_UPLOAD_CONCURRENCY]);
    if (override !== undefined) {
        return Math.min(override, CONCURRENCY_HARD_MAX);
    }
    return scaleToCores(
        cpuCount,
        UPLOAD_CONCURRENCY_MIN,
        UPLOAD_CONCURRENCY_MAX,
        UPLOAD_CONCURRENCY_FACTOR
    );
}

/**
 * DOWNLOAD single-file part-parallelism. DECOUPLED from cores (I/O-bound
 * range-GETs) with a high fixed floor so a small runner saturates its NIC;
 * scales up for big boxes and is env-overridable via CACHE_DOWNLOAD_CONCURRENCY
 * (floor 256, ceiling 512, cores*8).
 */
export function computeDownloadConcurrency(
    cpuCount: number = os.cpus().length,
    env: NodeJS.ProcessEnv = process.env
): number {
    const override = parseEnvInt(env[ENV_DOWNLOAD_CONCURRENCY]);
    if (override !== undefined) {
        return Math.min(override, CONCURRENCY_HARD_MAX);
    }
    return scaleToCores(
        cpuCount,
        DOWNLOAD_CONCURRENCY_MIN,
        DOWNLOAD_CONCURRENCY_MAX,
        DOWNLOAD_CONCURRENCY_FACTOR
    );
}

/** Direction-aware single-file part parallelism (both native engines). */
export function computeTransferConcurrency(
    direction: TransferDirection,
    cpuCount: number = os.cpus().length,
    env: NodeJS.ProcessEnv = process.env
): number {
    return direction === "upload"
        ? computeUploadConcurrency(cpuCount, env)
        : computeDownloadConcurrency(cpuCount, env);
}

/** Floor a part size at S3's 5 MiB multipart minimum and round to whole MiB. */
function clampPartSizeMb(mb: number): number {
    return Math.max(MIN_PART_SIZE_MB, Math.round(mb));
}

/**
 * Multipart part size (whole MiB) for a direction, or undefined to let the
 * engine use its own default. UPLOAD uses the backend's adaptive part size
 * (params.partSizeMb) so the 10k-part cap and per-part overhead stay respected.
 * DOWNLOAD uses a fixed small size (bounds concurrency*part-size memory),
 * independent of the upload-side size and overridable via CACHE_DOWNLOAD_PART_SIZE.
 */
export function computePartSizeMb(
    direction: TransferDirection,
    params: TransferParams,
    env: NodeJS.ProcessEnv = process.env
): number | undefined {
    if (direction === "upload") {
        return params.partSizeMb && params.partSizeMb > 0
            ? clampPartSizeMb(params.partSizeMb)
            : undefined;
    }
    const override = parseEnvInt(env[ENV_DOWNLOAD_PART_SIZE_MB]);
    return clampPartSizeMb(override ?? DOWNLOAD_PART_SIZE_MB);
}

/**
 * STREAMING s5cmd `cat` part-parallelism. A distinct, LOW, bounded value —
 * deliberately NOT the 256-way `cp` download concurrency — because `cat`'s
 * ordered writer buffers out-of-order parts behind a slow tar consumer; a small
 * concurrency keeps that buffer to a few hundred MB and few idle connections
 * (the fix for the observed truncated-stream / premature-EOF failure). Default
 * 6, env-overridable via CACHE_STREAM_S5CMD_CONCURRENCY (clamped to the sanity
 * ceiling; blank/non-positive overrides ignored).
 */
export function computeStreamS5cmdConcurrency(
    env: NodeJS.ProcessEnv = process.env
): number {
    const override = parseEnvInt(env[ENV_STREAM_S5CMD_CONCURRENCY]);
    if (override !== undefined) {
        return Math.min(override, CONCURRENCY_HARD_MAX);
    }
    return STREAM_S5CMD_CONCURRENCY_DEFAULT;
}

/**
 * STREAMING s5cmd `cat` part size (whole MiB). Modest by default (16 MiB) so the
 * concurrency*part-size ordered-writer buffer stays small; env-overridable via
 * CACHE_STREAM_S5CMD_PART_SIZE and floored at S3's 5 MiB multipart minimum.
 */
export function computeStreamS5cmdPartSizeMb(
    env: NodeJS.ProcessEnv = process.env
): number {
    const override = parseEnvInt(env[ENV_STREAM_S5CMD_PART_SIZE_MB]);
    return clampPartSizeMb(override ?? STREAM_S5CMD_PART_SIZE_MB_DEFAULT);
}

/** Region for the engines; "auto" when unset (S3-compatible endpoints/R2). */
export function resolveRegion(region?: string): string {
    return region && region.length > 0 ? region : "auto";
}

function s3Uri(params: TransferParams): string {
    return `s3://${params.bucket}/${params.key}`;
}

/**
 * Build the full s5cmd argv: `[global flags] cp [cp flags] <src> <dst>`.
 * s5cmd auto-selects path-style addressing whenever a custom --endpoint-url is
 * given, so no explicit path-style flag is needed (there isn't one). Note this
 * means a hypothetical custom endpoint that requires virtual-host addressing
 * (params.forcePathStyle === false) is NOT handled here — s5cmd would still
 * force path-style for it — but that is not a RunsOn scenario (RunsOn / R2 /
 * MinIO custom endpoints are all path-style), so it is left intentionally
 * unaddressed rather than adding a code path that can never run.
 */
export function buildS5cmdArgs(
    direction: TransferDirection,
    params: TransferParams,
    cpuCount: number = os.cpus().length,
    env: NodeJS.ProcessEnv = process.env
): string[] {
    const concurrency = computeTransferConcurrency(direction, cpuCount, env);
    // --numworkers only limits how many SEPARATE objects run at once (we move
    // exactly one), so it is pinned; keep it >= concurrency as a cheap hedge.
    const numworkers = Math.max(S5CMD_NUMWORKERS_MIN, concurrency);
    // Global flags precede the subcommand. --stat prints an end-of-run summary;
    // --log error drops per-object success chatter.
    const args: string[] = [
        "--numworkers",
        String(numworkers),
        "--stat",
        "--log",
        "error"
    ];
    if (params.endpoint) {
        args.push("--endpoint-url", params.endpoint);
    }
    // cp flags: --concurrency is the per-object part parallelism that actually
    // multiparts a single large archive; --part-size (MiB) sizes each part.
    args.push("cp", "--concurrency", String(concurrency));
    const partSizeMb = computePartSizeMb(direction, params, env);
    if (partSizeMb !== undefined) {
        args.push("--part-size", String(partSizeMb));
    }
    if (direction === "upload") {
        args.push(params.archivePath, s3Uri(params));
    } else {
        args.push(s3Uri(params), params.archivePath);
    }
    return args;
}

/**
 * `aws configure set default.s3.*` argv batches to run before the cp. These
 * tuning knobs have no CLI flag — they live only in the aws config file — so
 * they must be set this way first. Concurrency and chunk size mirror the s5cmd
 * reasoning: max_concurrent_requests is aws-cli's single-file part parallelism
 * (upload cores-scaled, download core-decoupled with a high fixed floor), and
 * multipart_chunksize is its part size (download's smaller size bounds the
 * concurrency*chunk peak buffer, exactly like the s5cmd path).
 */
export function buildAwsCliConfigureArgs(
    direction: TransferDirection,
    params: TransferParams,
    cpuCount: number = os.cpus().length,
    env: NodeJS.ProcessEnv = process.env
): string[][] {
    const concurrency = computeTransferConcurrency(direction, cpuCount, env);
    const chunkMb =
        computePartSizeMb(direction, params, env) ?? AWS_CLI_DEFAULT_CHUNK_MB;
    const batches: string[][] = [
        [
            "configure",
            "set",
            "default.s3.max_concurrent_requests",
            String(concurrency)
        ],
        ["configure", "set", "default.s3.max_queue_size", "100000"],
        ["configure", "set", "default.s3.multipart_threshold", "64MB"],
        ["configure", "set", "default.s3.multipart_chunksize", `${chunkMb}MB`]
    ];
    if (params.forcePathStyle) {
        batches.push([
            "configure",
            "set",
            "default.s3.addressing_style",
            "path"
        ]);
    }
    return batches;
}

/** `aws s3 cp <src> <dst> --endpoint-url <ep> --only-show-errors`. */
export function buildAwsCliCpArgs(
    direction: TransferDirection,
    params: TransferParams
): string[] {
    const args = ["s3", "cp"];
    if (direction === "upload") {
        args.push(params.archivePath, s3Uri(params));
    } else {
        args.push(s3Uri(params), params.archivePath);
    }
    if (params.endpoint) {
        args.push("--endpoint-url", params.endpoint);
    }
    args.push("--only-show-errors");
    return args;
}

// ============================================================================
// STREAMING RESTORE command builders (download straight to stdout, no scratch
// file). These feed the streamed `<downloader> | tar -xf -` pipeline in
// streamingRestore.ts, where the download and the tar+zstd extraction OVERLAP
// (~2x faster restore) instead of running serially (download-to-file THEN
// extract). The operands are plain argv — the s3 URI is never interpolated into
// a shell string — so the pipeline stays injection-safe.
// ============================================================================

/**
 * s5cmd streaming-download argv: `[global flags] cat --concurrency N
 * --part-size M s3://bucket/key`. `cat` writes the object to stdout using
 * concurrent multipart, but with a DISTINCT LOW streaming concurrency/part-size
 * (computeStreamS5cmdConcurrency / computeStreamS5cmdPartSizeMb) — deliberately
 * NOT the 256-way `cp` download tuning.
 *
 * WHY LOW: `cat` fetches up to `--concurrency` parts in parallel but must emit
 * them to stdout IN ORDER, so a SLOW consumer (the filesystem-bound tar+zstd
 * extract) makes the ordered writer buffer every out-of-order-completed part. At
 * the 256-way `cp` download concurrency that buffer + idle-connection set grew
 * until the writer truncated the stream (observed on a real runner: s5cmd cat
 * exit 1, tar "Unexpected EOF"). The low default (6 * 16 MiB ~= 96 MiB peak)
 * bounds the buffer and keeps few idle connections; override with
 * CACHE_STREAM_S5CMD_CONCURRENCY / CACHE_STREAM_S5CMD_PART_SIZE. aws-cli's
 * inherently bounded `cp - ` stream is the PRIMARY streaming engine — this
 * bounded `cat` is the secondary (CACHE_STREAM_RESTORE=0 disables streaming).
 *
 * NOTE: unlike the `cp` path this omits `--stat`: for `cat` the object bytes go
 * to stdout, and a stats line printed to stdout would corrupt the tar stream.
 */
export function buildS5cmdCatArgs(
    params: TransferParams,
    env: NodeJS.ProcessEnv = process.env
): string[] {
    const concurrency = computeStreamS5cmdConcurrency(env);
    const partSizeMb = computeStreamS5cmdPartSizeMb(env);
    // --numworkers only limits how many SEPARATE objects run at once (we move
    // exactly one), so it is pinned small (>= concurrency) — NOT the cp path's
    // 256 — so the footprint matches the low-concurrency intent.
    const numworkers = Math.max(STREAM_S5CMD_NUMWORKERS_MIN, concurrency);
    const args: string[] = [
        "--numworkers",
        String(numworkers),
        "--log",
        "error"
    ];
    if (params.endpoint) {
        args.push("--endpoint-url", params.endpoint);
    }
    args.push("cat", "--concurrency", String(concurrency));
    args.push("--part-size", String(partSizeMb));
    args.push(s3Uri(params));
    return args;
}

/**
 * aws-cli streaming-download argv: `s3 cp s3://bucket/key - [--endpoint-url ep]
 * --only-show-errors`. The `-` destination streams the object to stdout through
 * aws-cli's bounded internal ring buffer (a single streaming GET, not
 * multipart), which gives a LOWER, inherently bounded peak RAM than s5cmd cat's
 * ordered writer — so this is the memory-safer streaming engine and the natural
 * second choice. `--only-show-errors` keeps stdout pure object bytes (progress
 * chatter, if any, goes to stderr). Path-style vs virtual-host addressing and
 * the region come from the scoped `aws configure`/env the caller sets up first.
 */
export function buildAwsCliStreamCpArgs(params: TransferParams): string[] {
    const args = ["s3", "cp", s3Uri(params), "-"];
    if (params.endpoint) {
        args.push("--endpoint-url", params.endpoint);
    }
    args.push("--only-show-errors");
    return args;
}

function sanitizeEnv(env: NodeJS.ProcessEnv): { [key: string]: string } {
    const sanitized: { [key: string]: string } = {};
    for (const [key, value] of Object.entries(env)) {
        if (value !== undefined) {
            sanitized[key] = value;
        }
    }
    return sanitized;
}

/** A per-transfer child-process env plus a cleanup for its temp aws files. */
export interface EngineEnv {
    /** Env passed to the `s5cmd` / `aws` child process. */
    env: { [key: string]: string };
    /** Best-effort removal of the per-transfer temp aws config/credentials files. */
    cleanup: () => void;
}

// s5cmd / aws-cli inherit AWS credentials from the same default chain the
// S3Client uses (env vars, shared file, or — on RunsOn, where backend.ts unsets
// the static keys — the EC2 IAM instance profile). We pin AWS_REGION so a
// missing region resolves to "auto" for S3-compatible endpoints, and we point
// AWS_CONFIG_FILE / AWS_SHARED_CREDENTIALS_FILE at PER-TRANSFER temp files so
// aws-cli's `configure set default.s3.*` writes land in a throwaway scoped
// config instead of persistently mutating the shared ~/.aws/config [default]
// profile (a side-effect two concurrent `aws` runs would also race on); the
// subsequent `aws s3 cp` reads that same scoped file. Isolating these files
// does NOT change which credentials the engines use — env-var creds take
// priority over files, and the RunsOn IAM instance profile is discovered via
// IMDS (not a file) — so the engine credential source stays identical to the
// S3Client's. Only static creds living solely in the default shared file (a
// non-RunsOn dev setup, where env-var creds are the norm) would be bypassed.
export function buildEngineEnv(params: TransferParams): EngineEnv {
    const unique = `${process.pid}-${Date.now()}-${Math.random()
        .toString(36)
        .slice(2)}`;
    const configFile = path.join(os.tmpdir(), `runs-on-aws-config-${unique}`);
    const credentialsFile = path.join(
        os.tmpdir(),
        `runs-on-aws-credentials-${unique}`
    );
    const env = sanitizeEnv({
        ...process.env,
        AWS_REGION: resolveRegion(params.region),
        AWS_CONFIG_FILE: configFile,
        AWS_SHARED_CREDENTIALS_FILE: credentialsFile
    });
    const cleanup = (): void => {
        for (const file of [configFile, credentialsFile]) {
            try {
                fs.rmSync(file, { force: true });
            } catch {
                /* best-effort temp cleanup */
            }
        }
    };
    return { env, cleanup };
}

async function runS5cmd(
    direction: TransferDirection,
    params: TransferParams,
    execPath: string
): Promise<void> {
    const { env, cleanup } = buildEngineEnv(params);
    try {
        await exec(`"${execPath}"`, buildS5cmdArgs(direction, params), { env });
    } finally {
        cleanup();
    }
}

async function runAwsCli(
    direction: TransferDirection,
    params: TransferParams,
    execPath: string
): Promise<void> {
    const { env, cleanup } = buildEngineEnv(params);
    try {
        for (const configureArgs of buildAwsCliConfigureArgs(
            direction,
            params
        )) {
            await exec(`"${execPath}"`, configureArgs, { env, silent: true });
        }
        await exec(`"${execPath}"`, buildAwsCliCpArgs(direction, params), {
            env
        });
    } finally {
        cleanup();
    }
}

/** Resolve a binary on PATH cross-platform (io.which honors PATHEXT on Windows). */
export async function findExecutable(
    name: string
): Promise<string | undefined> {
    try {
        const resolved = await io.which(name, false);
        return resolved || undefined;
    } catch {
        return undefined;
    }
}

function logThroughput(
    engine: TransferEngineName,
    direction: TransferDirection,
    params: TransferParams,
    startedAtMs: number
): void {
    const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 0.001);
    let bytes = params.sizeBytes ?? 0;
    if (bytes <= 0) {
        try {
            bytes = fs.statSync(params.archivePath).size;
        } catch {
            bytes = 0;
        }
    }
    const mb = bytes / (1024 * 1024);
    const mbPerSec = (mb / elapsedSec).toFixed(1);
    core.info(
        `Cache ${direction} via ${engine}: ${bytes} B (~${Math.round(
            mb
        )} MB) in ${elapsedSec.toFixed(1)}s = ${mbPerSec} MB/s`
    );
}

/** Injectable seams so the selection/fallthrough logic is unit-testable
 *  without real s5cmd/aws-cli binaries or S3. */
export interface TransferEngineDeps {
    findExecutable: (name: string) => Promise<string | undefined>;
    runS5cmd: (
        direction: TransferDirection,
        params: TransferParams,
        execPath: string
    ) => Promise<void>;
    runAwsCli: (
        direction: TransferDirection,
        params: TransferParams,
        execPath: string
    ) => Promise<void>;
}

const defaultDeps: TransferEngineDeps = {
    findExecutable,
    runS5cmd,
    runAwsCli
};

/**
 * Move the single archive to/from s3://bucket/key using the first available,
 * working engine: s5cmd -> aws-cli -> node. `nodeFallback` performs the
 * existing Node transfer (lib-storage upload for "upload", the presigned ranged
 * downloader for "download") and is the guaranteed last resort. Returns the
 * engine that actually completed the transfer.
 *
 * `verifyNativeDownload` (downloads only) is an optional integrity check run
 * after a NATIVE engine (s5cmd/aws-cli) reports success: s5cmd/aws-cli exit 0
 * is otherwise trusted blindly, so the caller can compare the on-disk size
 * against the object's HeadObject ContentLength here. Throwing from it is
 * treated exactly like an engine failure — the chain falls through to the next
 * engine (never a hard fail). The node fallback validates its own byte count,
 * and uploads never expose a partial object, so neither needs this hook.
 */
export async function transferArchive(
    direction: TransferDirection,
    params: TransferParams,
    nodeFallback: () => Promise<void>,
    deps: TransferEngineDeps = defaultDeps,
    verifyNativeDownload?: () => Promise<void>
): Promise<TransferEngineName> {
    const startedAtMs = Date.now();

    // Engine 1: s5cmd.
    const s5cmdPath = await deps.findExecutable("s5cmd");
    if (s5cmdPath) {
        try {
            core.info(`Cache ${direction}: engine s5cmd (${s5cmdPath}).`);
            await deps.runS5cmd(direction, params, s5cmdPath);
            if (direction === "download" && verifyNativeDownload) {
                await verifyNativeDownload();
            }
            logThroughput("s5cmd", direction, params, startedAtMs);
            return "s5cmd";
        } catch (error) {
            core.warning(
                `s5cmd ${direction} failed (${
                    (error as Error).message
                }); falling back to aws-cli.`
            );
        }
    }

    // Engine 2: aws-cli.
    const awsPath = await deps.findExecutable("aws");
    if (awsPath) {
        try {
            core.info(`Cache ${direction}: engine aws-cli (${awsPath}).`);
            await deps.runAwsCli(direction, params, awsPath);
            if (direction === "download" && verifyNativeDownload) {
                await verifyNativeDownload();
            }
            logThroughput("aws-cli", direction, params, startedAtMs);
            return "aws-cli";
        } catch (error) {
            core.warning(
                `aws-cli ${direction} failed (${
                    (error as Error).message
                }); falling back to node.`
            );
        }
    }

    // Engine 3: node — always present, so this always completes or throws the
    // node error (never a "no engine available" failure).
    core.info(
        `Cache ${direction}: engine node (${
            direction === "upload" ? "lib-storage" : "http-client"
        }).`
    );
    await nodeFallback();
    logThroughput("node", direction, params, startedAtMs);
    return "node";
}
