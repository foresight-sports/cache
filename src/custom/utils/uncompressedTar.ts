import {
    ArchiveToolType,
    cacheUtils as utils,
    CompressionMethod,
    ManifestFilename,
    SystemTarPathOnWindows
} from "../../actionsCacheShims.js";
import * as core from "@actions/core";
import type { ExecOptions } from "@actions/exec";
import { exec } from "@actions/exec";
import * as io from "@actions/io";
import { existsSync, writeFileSync } from "fs";
import * as path from "path";

interface TarToolInfo {
    path: string;
    type: ArchiveToolType;
}

export const ENV_ZSTD_COMPRESS_ARGS = "CACHE_ZSTD_COMPRESS_ARGS";
export const DEFAULT_ZSTD_COMPRESS_ARGS = "-T0 -3 --long=30";
// Each token must look like a zstd option: no shell metacharacters, no paths.
const ZSTD_ARG_TOKEN = /^-{1,2}[A-Za-z0-9][A-Za-z0-9=,.-]*$/;

/**
 * zstd flags used when creating the archive. `CACHE_ZSTD_COMPRESS_ARGS` (e.g.
 * `-T0 -1` to trade a few percent of size for a much faster save, or `-T0 -3`
 * to drop the 1 GiB long-range window) replaces the default when every
 * whitespace-separated token is a plain option; anything else is ignored with a
 * warning so a typo can never break a save. The decompressor always passes
 * --long=30 and therefore reads either archive shape.
 */
export function getZstdCompressArgs(
    env: NodeJS.ProcessEnv = process.env
): string {
    const raw = (env[ENV_ZSTD_COMPRESS_ARGS] ?? "").trim();
    if (raw === "") {
        return DEFAULT_ZSTD_COMPRESS_ARGS;
    }
    const tokens = raw.split(/\s+/);
    if (tokens.every(token => ZSTD_ARG_TOKEN.test(token))) {
        return tokens.join(" ");
    }
    core.warning(
        `${ENV_ZSTD_COMPRESS_ARGS}='${raw}' contains a non-option token; using the default '${DEFAULT_ZSTD_COMPRESS_ARGS}'.`
    );
    return DEFAULT_ZSTD_COMPRESS_ARGS;
}

export function getZstdCompressProgram(
    env: NodeJS.ProcessEnv = process.env
): string {
    return `zstd ${getZstdCompressArgs(env)}`;
}

async function getTarTool(): Promise<TarToolInfo> {
    switch (process.platform) {
        case "win32": {
            const gnuTar = await utils.getGnuTarPathOnWindows();
            if (gnuTar) {
                return {
                    path: gnuTar,
                    type: ArchiveToolType.GNU
                };
            }
            if (existsSync(SystemTarPathOnWindows)) {
                return {
                    path: SystemTarPathOnWindows,
                    type: ArchiveToolType.BSD
                };
            }
            break;
        }
        case "darwin": {
            const gnuTar = await io.which("gtar", false);
            if (gnuTar) {
                return { path: gnuTar, type: ArchiveToolType.GNU };
            }
            return {
                path: await io.which("tar", true),
                type: ArchiveToolType.BSD
            };
        }
        default:
            break;
    }

    return {
        path: await io.which("tar", true),
        type: ArchiveToolType.GNU
    };
}

function getWorkingDirectory(): string {
    return process.env["GITHUB_WORKSPACE"] ?? process.cwd();
}

function normalizeForTar(targetPath: string): string {
    return targetPath.replace(new RegExp(`\\${path.sep}`, "g"), "/");
}

function appendPlatformSpecificArgs(tool: TarToolInfo, args: string[]): void {
    if (tool.type === ArchiveToolType.GNU) {
        if (process.platform === "win32") {
            args.push("--force-local");
        } else if (process.platform === "darwin") {
            args.push("--delay-directory-restore");
        }
    }
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

function getExecEnv(): { [key: string]: string } {
    return sanitizeEnv({ ...process.env, MSYS: "winsymlinks:nativestrict" });
}

// createTar shells out to `zstd` (the no-compression fast path compresses with
// multithreaded zstd). If zstd isn't on PATH the tar invocation fails and the
// cache is silently skipped upstream — surface one clear warning so the cause is
// obvious in the log instead of a generic tar error. Emitted at most once.
let zstdMissingWarned = false;
async function warnIfZstdMissing(): Promise<void> {
    if (zstdMissingWarned) {
        return;
    }
    const zstdPath = await io.which("zstd", false);
    if (!zstdPath) {
        zstdMissingWarned = true;
        core.warning(
            "zstd not found on PATH — cache disabled for this run. Install zstd to enable caching."
        );
    }
}

async function runTar(
    tool: TarToolInfo,
    args: string[],
    options?: ExecOptions & { cwd?: string }
): Promise<void> {
    await exec(`"${tool.path}"`, args, options);
}

export async function createTar(
    archiveFolder: string,
    sourceDirectories: string[],
    compressionMethod: CompressionMethod
): Promise<void> {
    const tool = await getTarTool();
    await warnIfZstdMissing();
    const cacheFileName = utils.getCacheFileName(compressionMethod);
    const normalizedArchiveName = normalizeForTar(cacheFileName);
    const normalizedManifestPath = normalizeForTar(
        path.join(archiveFolder, ManifestFilename)
    );
    const workingDirectory = normalizeForTar(getWorkingDirectory());

    writeFileSync(normalizedManifestPath, sourceDirectories.join("\n"));

    const compressProgram = getZstdCompressProgram();
    core.info(`Cache archive compressor: ${compressProgram}`);
    const args = [
        "--posix",
        // Multithreaded zstd (-T0 = all cores) at the fast level 3, with long-range
        // matching (--long=30 = 1 GiB window) by default; overridable per job via
        // CACHE_ZSTD_COMPRESS_ARGS. tar splits this value on whitespace and runs it
        // as the compression filter. The decompressor in extractTar/listTar stays
        // `zstd -d --long=30`, which reads archives produced with or without --long.
        "--use-compress-program",
        compressProgram,
        "-cf",
        normalizedArchiveName,
        "--exclude",
        normalizedArchiveName,
        "-P",
        "-C",
        workingDirectory,
        "--files-from",
        ManifestFilename
    ];

    appendPlatformSpecificArgs(tool, args);

    await runTar(tool, args, {
        cwd: archiveFolder,
        env: getExecEnv()
    });
}

/**
 * Create ONE shard of a sharded archive: tar exactly the paths listed in
 * `fileListName` (one per line, relative to the workspace, no recursion) into
 * `archiveName`, both inside `archiveFolder`, compressing with the given
 * `zstd ...` program. Same tar tool, `--posix`, `-P -C <workspace>` and
 * platform args as createTar; `--no-recursion` because the caller has already
 * enumerated every file (and empty directory) explicitly so it can balance the
 * shards by size. Several of these run concurrently.
 */
export async function createTarFromFileList(
    archiveFolder: string,
    fileListName: string,
    archiveName: string,
    compressProgram: string
): Promise<void> {
    const tool = await getTarTool();
    await warnIfZstdMissing();
    const workingDirectory = normalizeForTar(getWorkingDirectory());

    const args = [
        "--posix",
        "--no-recursion",
        "--use-compress-program",
        compressProgram,
        "-cf",
        normalizeForTar(archiveName),
        "-P",
        "-C",
        workingDirectory,
        "--files-from",
        normalizeForTar(fileListName)
    ];

    appendPlatformSpecificArgs(tool, args);

    await runTar(tool, args, {
        cwd: archiveFolder,
        env: getExecEnv()
    });
}

export async function extractTar(
    archivePath: string,
    _compressionMethod: CompressionMethod
): Promise<void> {
    void _compressionMethod;
    const tool = await getTarTool();
    const workingDirectory = normalizeForTar(getWorkingDirectory());

    await io.mkdirP(workingDirectory);

    const args = [
        // Decompress with zstd (long-range window must match the create side).
        // `zstd -d` is used rather than `unzstd` because it is the form proven on the
        // Windows Git tar bundle used by the runner (the toolkit's own zstd path and
        // the observed restore log both invoke `zstd -d`), and it is equally valid on
        // Linux/macOS.
        "--use-compress-program",
        "zstd -d --long=30",
        "-xf",
        normalizeForTar(archivePath),
        "-P",
        "-C",
        workingDirectory
    ];

    appendPlatformSpecificArgs(tool, args);

    await runTar(tool, args, {
        env: getExecEnv()
    });
}

/** A spawn-ready tar invocation (command + argv + env). Structurally matches
 *  streamingRestore.SpawnSpec so it can be piped without a cross-module type
 *  import (keeps uncompressedTar free of a streamingRestore dependency). */
export interface TarStreamCommand {
    command: string;
    args: string[];
    env: { [key: string]: string };
}

// Core tar argv for reading the COMPRESSED archive from stdin (`-xf -`) instead
// of a file, decompressing via the exact same `zstd -d --long=30` filter as the
// file-based extractTar. The `--long=30` window MUST match the create side; it
// is preserved unchanged here (no save-side/format change). Split out (pure, no
// I/O) so the streaming extract args are unit-testable without a real tar/zstd.
export function buildStreamExtractCoreArgs(workingDirectory: string): string[] {
    return [
        "--use-compress-program",
        "zstd -d --long=30",
        "-xf",
        "-",
        "-P",
        "-C",
        workingDirectory
    ];
}

/**
 * Build the tar command that extracts the archive from STDIN, for the streamed
 * `<downloader> | tar -xf -` restore pipeline. Same tar tool, same
 * `zstd -d --long=30` decompressor, same `-P -C <workspace>` target as the
 * file-based extractTar — only the input source changes (stdin, not a file), so
 * no scratch archive is written or read. Returns a spawn-ready spec; the caller
 * (streamingRestore.runPipeline) wires the downloader's stdout into this tar's
 * stdin via Node stream piping (no shell pipe, no FIFO).
 */
export async function buildExtractTarStreamCommand(): Promise<TarStreamCommand> {
    const tool = await getTarTool();
    const workingDirectory = normalizeForTar(getWorkingDirectory());

    await io.mkdirP(workingDirectory);

    const args = buildStreamExtractCoreArgs(workingDirectory);
    appendPlatformSpecificArgs(tool, args);

    return { command: tool.path, args, env: getExecEnv() };
}

export async function listTar(
    archivePath: string,
    _compressionMethod: CompressionMethod
): Promise<void> {
    void _compressionMethod;
    const tool = await getTarTool();

    // Same zstd decompressor as extractTar so debug listing works on the
    // now-compressed archive.
    const args = [
        "--use-compress-program",
        "zstd -d --long=30",
        "-tf",
        normalizeForTar(archivePath),
        "-P"
    ];

    appendPlatformSpecificArgs(tool, args);

    await runTar(tool, args, {
        env: getExecEnv()
    });
}
