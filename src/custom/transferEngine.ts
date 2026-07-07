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

// s5cmd's global worker pool. Scale to cores so the goroutine fan-out actually
// uses the box, with a floor so tiny runners still parallelize and a ceiling
// matching s5cmd's own default.
const S5CMD_MIN_WORKERS = 32;
const S5CMD_MAX_WORKERS = 256;

// aws-cli's max_concurrent_requests. Same core-scaling idea; the CRT default of
// 10 is the flat-26-MB/s bottleneck Premier measured, so lift it well above.
const AWS_CLI_MIN_CONCURRENCY = 16;
const AWS_CLI_MAX_CONCURRENCY = 256;

function scaleToCores(
    cpuCount: number,
    min: number,
    max: number,
    factor = 4
): number {
    const cores = Number.isFinite(cpuCount) && cpuCount > 0 ? cpuCount : 1;
    return Math.min(max, Math.max(min, Math.floor(cores) * factor));
}

/** s5cmd `--numworkers`, scaled to cores (floor 32, ceiling 256). */
export function computeS5cmdWorkers(
    cpuCount: number = os.cpus().length
): number {
    return scaleToCores(cpuCount, S5CMD_MIN_WORKERS, S5CMD_MAX_WORKERS);
}

/** aws-cli `max_concurrent_requests`, scaled to cores (floor 16, ceiling 256). */
export function computeAwsCliConcurrency(
    cpuCount: number = os.cpus().length
): number {
    return scaleToCores(
        cpuCount,
        AWS_CLI_MIN_CONCURRENCY,
        AWS_CLI_MAX_CONCURRENCY
    );
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
    cpuCount: number = os.cpus().length
): string[] {
    const workers = computeS5cmdWorkers(cpuCount);
    // Global flags precede the subcommand. --stat prints an end-of-run summary;
    // --log error drops per-object success chatter.
    const args: string[] = [
        "--numworkers",
        String(workers),
        "--stat",
        "--log",
        "error"
    ];
    if (params.endpoint) {
        args.push("--endpoint-url", params.endpoint);
    }
    // cp flags: --concurrency is the per-object part parallelism (what actually
    // multiparts a single large archive across the worker pool); --part-size is
    // in MiB.
    args.push("cp", "--concurrency", String(workers));
    if (params.partSizeMb && params.partSizeMb > 0) {
        args.push(
            "--part-size",
            String(Math.max(5, Math.round(params.partSizeMb)))
        );
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
 * they must be set this way first.
 */
export function buildAwsCliConfigureArgs(
    params: TransferParams,
    cpuCount: number = os.cpus().length
): string[][] {
    const concurrency = computeAwsCliConcurrency(cpuCount);
    const chunkMb =
        params.partSizeMb && params.partSizeMb > 0
            ? Math.max(5, Math.round(params.partSizeMb))
            : 64;
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
        for (const configureArgs of buildAwsCliConfigureArgs(params)) {
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
