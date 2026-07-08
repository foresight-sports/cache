// Streamed cache restore: overlap the download and the extraction instead of
// running them serially.
//
// OLD (file-based, serial): `s5cmd/aws cp s3://…/cache.tzst <file>` writes the
// whole ~44 GB archive to NVMe scratch, THEN `tar --use-compress-program
// "zstd -d --long=30" -xf <file>` reads it back and extracts. Measured ~3.5 min
// download + ~3.5 min extract = ~7 min, and it round-trips 44 GB through disk.
//
// NEW (streamed, overlapped): `<downloader to stdout> | tar
// --use-compress-program "zstd -d --long=30" -xf -`. The downloader streams the
// object to stdout while tar+zstd extract it on the fly, so the two phases run
// concurrently (~1.8-2x, ~7 min -> ~3.5-4 min) and no scratch file is written.
//
// The pipe is wired in NODE (downloader.stdout -> tar.stdin), NOT via a shell
// pipe or a FIFO: `mkfifo`'s MSYS-emulated FIFOs are unusable by native Windows
// exes, and `bash -c "… | …"` would mean interpolating the s3 URL / dest path
// into a shell string. Both child processes are spawned with argv arrays and
// shell:false, so nothing is ever parsed by a shell (injection-safe).
//
// Streaming is OPT-IN (enable with CACHE_STREAM_RESTORE=1/true/yes/on); the
// default is the file-based download-to-file + extract path, which is the fast
// route once the archive is staged on NVMe via CACHE_ARCHIVE_DIR. When enabled,
// the PRIMARY streaming engine is aws-cli `s3 cp - |
// tar`: its download uses a bounded internal ring buffer, so it stays robust
// even when the tar consumer is slow (the fs-bound untar of tens of thousands
// of small files), which is exactly the case that truncated the s5cmd path on a
// real runner. The SECONDARY streaming engine is s5cmd `cat | tar`, run at a
// distinct LOW, bounded streaming concurrency (NOT the 256-way cp download
// concurrency) so its ordered writer buffers only a few hundred MB behind a slow
// consumer instead of growing until it truncates. On ANY streaming failure
// (downloader non-zero exit, tar/zstd error, broken pipe) the chain tries the
// next streaming engine and ultimately signals the caller to fall back to the
// proven file-based download-to-file + extract path (which also re-runs the
// s5cmd/aws/node cp engines). The node presigned-URL engine is intentionally NOT
// streamed — it stays file-based as the always-present safety net.
// Unset (or any non-truthy value) keeps the file-based path entirely; only an
// explicitly truthy CACHE_STREAM_RESTORE (1/true/yes/on) enables streaming.
import * as core from "@actions/core";
import { exec } from "@actions/exec";
import { spawn } from "child_process";

import {
    buildAwsCliConfigureArgs,
    buildAwsCliStreamCpArgs,
    buildEngineEnv,
    buildS5cmdCatArgs,
    findExecutable,
    TransferEngineName,
    TransferParams
} from "./transferEngine";
import { buildExtractTarStreamCommand } from "./utils/uncompressedTar";

/** A spawn-ready child process: resolved executable + argv + env. */
export interface SpawnSpec {
    command: string;
    args: string[];
    env: { [key: string]: string };
}

/** Env var that opts INTO streaming restore (default is file-based). */
export const ENV_STREAM_RESTORE = "CACHE_STREAM_RESTORE";

/**
 * Streaming restore is OFF by default. It runs ONLY when `CACHE_STREAM_RESTORE`
 * is explicitly truthy — `1`/`true`/`yes`/`on` (case-insensitive). Unset, blank,
 * or any other value (including 0/false/no/off) uses the file-based
 * download-to-file + extract path, which is the fast route once the archive is
 * staged on NVMe via CACHE_ARCHIVE_DIR. Streaming stays an opt-in experiment
 * because the aws-cli stdout stream caps ~95 MB/s on real runners.
 */
export function isStreamRestoreEnabled(
    env: NodeJS.ProcessEnv = process.env
): boolean {
    const value = (env[ENV_STREAM_RESTORE] ?? "").trim().toLowerCase();
    return (
        value === "1" ||
        value === "true" ||
        value === "yes" ||
        value === "on"
    );
}

/**
 * Run `downloader | tar` as a Node-wired pipeline: spawn both children (argv
 * arrays, shell:false — no shell parsing) and pipe downloader.stdout ->
 * tar.stdin with Node handling backpressure. Resolves only when BOTH children
 * exit 0 (integrity for the streamed path comes from the downloader completing
 * AND tar+zstd extracting end-to-end); rejects on any non-zero exit, spawn
 * error, or broken pipe so the caller can fall through cleanly.
 */
export async function runPipeline(
    downloader: SpawnSpec,
    tar: SpawnSpec
): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        const stderrChunks: string[] = [];
        let settled = false;

        let dlDone = false;
        let tarDone = false;
        let dlCode: number | null = null;
        let tarCode: number | null = null;

        const settleReject = (err: Error): void => {
            if (settled) {
                return;
            }
            settled = true;
            reject(err);
        };

        const maybeSettle = (): void => {
            if (!dlDone || !tarDone || settled) {
                return;
            }
            if (dlCode === 0 && tarCode === 0) {
                settled = true;
                resolve();
                return;
            }
            const detail = stderrChunks.join("").trim().slice(0, 2000);
            settleReject(
                new Error(
                    `streamed restore pipeline failed (downloader exit ${dlCode}, tar exit ${tarCode})` +
                        (detail ? `: ${detail}` : "")
                )
            );
        };

        // tar is the consumer; spawn it first so its stdin exists before we pipe.
        const tarProc = spawn(tar.command, tar.args, {
            env: tar.env,
            stdio: ["pipe", "inherit", "pipe"],
            windowsHide: true
        });
        const dlProc = spawn(downloader.command, downloader.args, {
            env: downloader.env,
            stdio: ["ignore", "pipe", "pipe"],
            windowsHide: true
        });

        // downloader stdout -> tar stdin (Node pipe, honors backpressure).
        dlProc.stdout?.pipe(tarProc.stdin as NodeJS.WritableStream);

        dlProc.stderr?.on("data", (chunk: Buffer) =>
            stderrChunks.push(`[downloader] ${chunk.toString()}`)
        );
        tarProc.stderr?.on("data", (chunk: Buffer) =>
            stderrChunks.push(`[tar] ${chunk.toString()}`)
        );

        // A broken pipe (tar died -> downloader's stdout write EPIPEs, or vice
        // versa) is not the real error; the non-zero exit code below is. Swallow
        // the stream 'error' so it does not become an unhandled exception.
        tarProc.stdin?.on("error", () => undefined);
        dlProc.stdout?.on("error", () => undefined);

        dlProc.on("error", (err: Error) => {
            dlDone = true;
            dlCode = dlCode ?? -1;
            if (!tarDone) {
                tarProc.kill("SIGKILL");
            }
            settleReject(err);
        });
        tarProc.on("error", (err: Error) => {
            tarDone = true;
            tarCode = tarCode ?? -1;
            if (!dlDone) {
                dlProc.kill("SIGKILL");
            }
            settleReject(err);
        });

        dlProc.on("close", (code: number | null) => {
            dlDone = true;
            dlCode = code;
            maybeSettle();
        });
        tarProc.on("close", (code: number | null) => {
            tarDone = true;
            tarCode = code;
            // tar exited (likely on error) before the downloader finished:
            // stop the transfer so it does not keep pulling the whole object.
            if ((code ?? -1) !== 0 && !dlDone) {
                dlProc.kill("SIGKILL");
            }
            maybeSettle();
        });
    });
}

/** Run scoped `aws configure set …` batches before the streaming cp. */
async function runAwsConfigure(
    batches: string[][],
    env: { [key: string]: string },
    execPath: string
): Promise<void> {
    for (const configureArgs of batches) {
        await exec(`"${execPath}"`, configureArgs, { env, silent: true });
    }
}

/** Injectable seams so the engine selection / fallthrough is unit-testable
 *  without real s5cmd/aws/tar binaries, spawned processes, or S3. */
export interface StreamRestoreDeps {
    findExecutable: (name: string) => Promise<string | undefined>;
    buildExtractCommand: () => Promise<SpawnSpec>;
    runPipeline: (downloader: SpawnSpec, tar: SpawnSpec) => Promise<void>;
    runAwsConfigure: (
        batches: string[][],
        env: { [key: string]: string },
        execPath: string
    ) => Promise<void>;
}

const defaultDeps: StreamRestoreDeps = {
    findExecutable,
    buildExtractCommand: buildExtractTarStreamCommand,
    runPipeline,
    runAwsConfigure
};

/**
 * Attempt a streamed restore of s3://bucket/key straight into the workspace,
 * overlapping download and extraction. Tries the PRIMARY `aws s3 cp - | tar`
 * (bounded ring buffer — robust with a slow, fs-bound tar consumer) first, then
 * the SECONDARY `s5cmd cat | tar` at a LOW bounded streaming concurrency; each
 * streaming engine that is missing or fails falls through to the next. Returns
 * the engine that completed the restore, or THROWS if no streaming engine
 * succeeded — the backend wrapper turns that throw into a clean fall-back to the
 * file-based download+extract path. The node presigned-URL engine is
 * deliberately absent here (it is the file-based safety net, not a streaming
 * engine).
 */
export async function streamedRestore(
    params: TransferParams,
    deps: StreamRestoreDeps = defaultDeps
): Promise<TransferEngineName> {
    const startedAtMs = Date.now();
    const logElapsed = (engine: TransferEngineName): void => {
        const elapsedSec = Math.max((Date.now() - startedAtMs) / 1000, 0.001);
        core.info(
            `Cache restored via streamed ${engine} (download|extract overlapped) in ${elapsedSec.toFixed(
                1
            )}s.`
        );
    };

    // Engine 1 (PRIMARY): aws s3 cp - | tar. aws-cli streams the object through a
    // bounded internal ring buffer (a single streaming GET, not many concurrent
    // parts), so it stays robust when the fs-bound tar consumer is slow — the
    // case that truncated the s5cmd cat path on a real runner. It only needs to
    // sustain ~200 MB/s to hide under the fs-bound extract, which it does.
    const awsPath = await deps.findExecutable("aws");
    if (awsPath) {
        const { env, cleanup } = buildEngineEnv(params);
        try {
            core.info(
                `Cache download: streaming engine aws-cli cp - | tar (${awsPath}).`
            );
            // Multipart knobs are largely inert for a stdout stream (single GET),
            // but addressing_style=path (set when forcePathStyle) is required for
            // RunsOn/R2/MinIO path-style endpoints, so run the scoped configure.
            await deps.runAwsConfigure(
                buildAwsCliConfigureArgs("download", params),
                env,
                awsPath
            );
            const tar = await deps.buildExtractCommand();
            await deps.runPipeline(
                {
                    command: awsPath,
                    args: buildAwsCliStreamCpArgs(params),
                    env
                },
                tar
            );
            logElapsed("aws-cli");
            return "aws-cli";
        } catch (error) {
            core.warning(
                `aws-cli streamed restore failed (${
                    (error as Error).message
                }); trying s5cmd cat stream.`
            );
        } finally {
            cleanup();
        }
    }

    // Engine 2 (SECONDARY): s5cmd cat | tar, run at a DISTINCT LOW streaming
    // concurrency (buildS5cmdCatArgs uses computeStreamS5cmdConcurrency, NOT the
    // 256-way cp download concurrency) so cat's ordered writer buffers only a few
    // hundred MB behind a slow consumer instead of growing until it truncates.
    const s5cmdPath = await deps.findExecutable("s5cmd");
    if (s5cmdPath) {
        const { env, cleanup } = buildEngineEnv(params);
        try {
            core.info(
                `Cache download: streaming engine s5cmd cat | tar (${s5cmdPath}).`
            );
            const tar = await deps.buildExtractCommand();
            await deps.runPipeline(
                { command: s5cmdPath, args: buildS5cmdCatArgs(params), env },
                tar
            );
            logElapsed("s5cmd");
            return "s5cmd";
        } catch (error) {
            core.warning(
                `s5cmd streamed restore failed (${
                    (error as Error).message
                }); falling back to file-based restore.`
            );
        } finally {
            cleanup();
        }
    }

    throw new Error(
        "no streaming restore engine available (aws-cli/s5cmd missing or failed)"
    );
}
