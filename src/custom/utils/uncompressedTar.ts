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

    const args = [
        "--posix",
        // Multithreaded zstd (-T0 = all cores) at the fast level 3, with long-range
        // matching (--long=30 = 1 GiB window). tar splits this value on whitespace
        // and runs it as the compression filter. This replaces the previous raw
        // (uncompressed) tar so the payload is both smaller and produced in parallel.
        // The matching decompressor in extractTar/listTar uses `zstd -d --long=30`.
        "--use-compress-program",
        "zstd -T0 -3 --long=30",
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
