import {
    CompressionMethod,
    ManifestFilename,
    SystemTarPathOnWindows,
} from '@actions/cache/lib/internal/constants';
import * as cacheUtils from '@actions/cache/lib/internal/cacheUtils';
import {
    createTar as defaultCreateTar,
    extractTar as defaultExtractTar,
} from '@actions/cache/lib/internal/tar';

import * as tc from '@actions/tool-cache';
import * as core from '@actions/core';
import * as exec from '@actions/exec';
import * as io from '@actions/io';
import * as os from 'os';
import { writeFileSync } from 'fs';
import * as path from 'path';
import fs from 'fs';

const IS_WINDOWS = process.platform === 'win32';
const BYTES_PER_MEGABYTE = 1024 * 1024;

interface TarResolution {
    path: string;
    useForceLocal: boolean;
}

function toPosixPath(target: string): string {
    return target.replace(/\\/g, '/');
}

/**
 * Try to find pigz in PATH
 */
async function findPigz(): Promise<string | null> {
    try {
        const found = await io.which('pigz', false);
        return found || null;
    } catch {
        return null;
    }
}

/**
 * OS-specific installation logic
 */
async function installPigz(): Promise<void> {
    const platform = os.platform();

    core.info(`Attempting to install pigz on ${platform}...`);

    try {
        if (platform === 'linux') {
            await exec.exec('sudo apt-get update');
            await exec.exec('sudo apt-get install -y pigz');
        } else if (platform === 'darwin') {
            const brew = await io.which('brew', false);
            if (!brew) throw new Error('Homebrew not found');
            await exec.exec('brew install pigz');
        } else if (platform === 'win32') {
            // expect the binaries to be in GITHUB_WORKSPACE/.tool-cache/
            const githubWorkspace = process.env['GITHUB_WORKSPACE'];

            if (!githubWorkspace) {
                throw new Error('GITHUB_WORKSPACE is not defined');
            }

            const pigzDir = path.join(githubWorkspace, '.tool-cache');
            // if directory doesn't exist, throw error. Don't use io.<> here

            core.info(`Checking for pigz directory at: ${pigzDir}`);

            if (!fs.existsSync(pigzDir)) {
                throw new Error(`Expected pigz directory does not exist: ${pigzDir}`);
            }

            core.info(`Checking for pigz.exe in: ${pigzDir}`);
            const pigzPath = path.join(pigzDir, 'pigz.exe');

            if (!fs.existsSync(pigzPath)) {
                throw new Error(`pigz.exe not found at expected location: ${pigzPath}`);
            }

            core.info('Adding pigz to the tool cache...');

            // add dir to tool cache
            const toolPath = await tc.cacheDir(pigzDir, 'pigz', 'latest');

            core.info(`pigz added to tool cache at: ${toolPath}`);
            core.addPath(pigzDir);
        } else {
            throw new Error(`Unsupported platform: ${platform}`);
        }

        core.info('pigz installation attempt complete.');
    } catch (e: any) {
        core.warning(`Failed to install pigz: ${e.message}`);
    }
}

/**
 * Ensure pigz is installed and accessible.
 * Returns the path or null if we must fall back to tar/gzip.
 */
export async function ensurePigz(): Promise<string | null> {
    // Step 1: check PATH first
    let pigzPath = await findPigz();
    if (pigzPath) {
        core.info(`pigz found at: ${pigzPath}`);
        return pigzPath;
    }

    core.info('pigz not found — attempting installation…');

    // Step 2: attempt installation
    await installPigz();

    // Step 3: check again
    pigzPath = await findPigz();
    if (pigzPath) {
        core.info(`pigz successfully installed at: ${pigzPath}`);
        return pigzPath;
    }

    // Step 4: fall back to tar/gzip
    core.warning('pigz could not be installed; falling back to tar/gzip.');
    return null;
}

async function resolveTar(): Promise<TarResolution> {
    if (IS_WINDOWS) {
        const gnuTar = await cacheUtils.getGnuTarPathOnWindows();
        if (gnuTar) {
            return { path: gnuTar, useForceLocal: true };
        }
        return { path: SystemTarPathOnWindows, useForceLocal: false };
    }

    const tarPath = await io.which('tar', true);
    return { path: tarPath, useForceLocal: false };
}

function getWorkingDirectory(): string {
    return (process.env['GITHUB_WORKSPACE'] ?? process.cwd()).replace(
        new RegExp(`\\${path.sep}`, 'g'),
        '/'
    );
}

/**
 * Create a tar archive using pigz for gzip compression when available.
 * Falls back to the default @actions/cache tar implementation otherwise.
 */
export async function createTarWithPigz(
    archiveFolder: string,
    cachePaths: string[],
    compressionMethod: CompressionMethod
): Promise<void> {
    // pigz only makes sense for gzip; for zstd variants use the default implementation.
    if (compressionMethod !== CompressionMethod.Gzip) {
        core.warning(
            `Compression method is not gzip but ${compressionMethod}; delegating to default createTar.`
        );
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod);
    }

    // Ensure pigz is installed. If not, just use the default tar implementation.
    const pigzPath = await ensurePigz();
    if (!pigzPath) {
        core.warning('pigz is not available; delegating to default createTar.');
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod);
    }

    core.info('Using pigz for gzip compression when creating cache tarball.');

    // Write manifest.txt in the archiveFolder, mirroring the default createTar behavior
    writeFileSync(
        path.join(archiveFolder, ManifestFilename),
        cachePaths.join('\n')
    );

    // Compute the archive filename exactly as actions/cache would
    const cacheFileName = cacheUtils.getCacheFileName(compressionMethod);

    // Normalize to forward slashes for tar
    const cacheFileNameForTar = cacheFileName.replace(
        new RegExp(`\\${path.sep}`, 'g'),
        '/'
    );
    const workingDirectory = getWorkingDirectory();
    const totalBytes = await calculateCachePathsSize(
        cachePaths,
        workingDirectory
    );

    const threadCount = Math.max(os.cpus().length, 1);
    core.info(`pigz threads: ${threadCount}`);
    core.info(
        `Compressing cache inputs to '${cacheFileNameForTar}' using ${threadCount} threads (pigz=${pigzPath}).`
    );
    const tarResolution = await resolveTar();
    const normalizedWorkspace = toPosixPath(workingDirectory);
    const archiveTarget = bashQuote(cacheFileNameForTar);
    const tarParts: string[] = [
        bashQuote(tarResolution.path),
        '--posix',
        '-cf',
        '-',
        '--exclude',
        archiveTarget,
        '-P',
        '-C',
        bashQuote(normalizedWorkspace),
        '--files-from',
        bashQuote(ManifestFilename),
    ];
    if (tarResolution.useForceLocal) {
        tarParts.splice(1, 0, '--force-local');
    }
    const pigzParts = [
        bashQuote(pigzPath),
        '--fast',
        '-p',
        threadCount.toString(),
    ];
    const command = `${tarParts.join(' ')} | ${pigzParts.join(
        ' '
    )} > ${archiveTarget}`;
    core.debug(`Running tar with pigz: ${command}`);

    const startTime = process.hrtime.bigint();
    let exitCode = 0;
    try {
        exitCode = await exec.exec('bash', ['-c', command], {
            cwd: archiveFolder,
            env: {
                ...(process.env as object),
                MSYS: 'winsymlinks:nativestrict',
            },
            ignoreReturnCode: true,
        });
    } catch (error: any) {
        core.warning(
            `tar with pigz failed (${error?.message}); falling back to default createTar.`
        );
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod);
    }

    const elapsedSeconds = hrtimeSeconds(startTime);
    core.info(
        `Compressed cache inputs in ${formatSeconds(
            elapsedSeconds
        )} seconds (exit=${exitCode}).`
    );
    const compressionThroughput = formatThroughput(totalBytes, elapsedSeconds);
    if (compressionThroughput) {
        core.info(`Compress throughput: ${compressionThroughput} MB/s`);
    }

    const archiveFullPath = path.join(archiveFolder, cacheFileName);
    const archiveStats = await safeLstat(archiveFullPath);
    if (archiveStats) {
        core.info(`Archive size: ${archiveStats.size} bytes`);
    }

    if (exitCode !== 0) {
        core.warning(
            'tar with pigz reported a non-zero exit code; falling back to default createTar.'
        );
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod);
    }
}

export async function extractTarWithPigz(
    archivePath: string,
    compressionMethod: CompressionMethod
): Promise<void> {
    // pigz only makes sense for gzip; for zstd variants use the default implementation.
    if (compressionMethod !== CompressionMethod.Gzip) {
        core.warning(
            `Compression method is not gzip but ${compressionMethod}; delegating to default extractTar.`
        );
        return defaultExtractTar(archivePath, compressionMethod);
    }

    const pigzPath = await ensurePigz();
    if (!pigzPath) {
        core.warning('pigz is not available; delegating to default extractTar.');
        return defaultExtractTar(archivePath, compressionMethod);
    }

    core.info('Using pigz for gzip decompression when extracting cache tarball.');

    const threadCount = Math.max(os.cpus().length, 1);
    core.info(`pigz threads: ${threadCount}`);
    const tarResolution = await resolveTar();
    const workingDirectory = getWorkingDirectory();
    core.info(
        `Decompressing archive '${archivePath}' to '${workingDirectory}' using ${threadCount} threads (pigz=${pigzPath}).`
    );
    await io.mkdirP(workingDirectory);
    const normalizedArchivePath = toPosixPath(archivePath);
    const archiveStats = await safeLstat(archivePath);
    const archiveBytes = archiveStats?.size ?? 0;

    const pigzParts = [
        bashQuote(pigzPath),
        '-d',
        '-p',
        threadCount.toString(),
        '-c',
        bashQuote(normalizedArchivePath),
    ];
    const tarParts = [
        bashQuote(tarResolution.path),
        '-xf',
        '-',
        '-P',
        '-C',
        bashQuote(toPosixPath(workingDirectory)),
    ];
    if (tarResolution.useForceLocal) {
        tarParts.splice(1, 0, '--force-local');
    }

    const command = `${pigzParts.join(' ')} | ${tarParts.join(' ')}`;
    core.debug(`Running tar with pigz: ${command}`);

    const startTime = process.hrtime.bigint();
    let exitCode = 0;
    try {
        exitCode = await exec.exec('bash', ['-c', command], {
            env: {
                ...(process.env as object),
                MSYS: 'winsymlinks:nativestrict',
            },
            ignoreReturnCode: true,
        });
    } catch (error: any) {
        core.warning(
            `tar with pigz failed (${error?.message}); falling back to default extractTar.`
        );
        return defaultExtractTar(archivePath, compressionMethod);
    }

    const elapsedSeconds = hrtimeSeconds(startTime);
    core.info(
        `Decompressed archive in ${formatSeconds(
            elapsedSeconds
        )} seconds (exit=${exitCode}).`
    );
    const decompressionThroughput = formatThroughput(
        archiveBytes,
        elapsedSeconds
    );
    if (decompressionThroughput) {
        core.info(`Decompress throughput: ${decompressionThroughput} MB/s`);
    }

    if (exitCode !== 0) {
        core.warning(
            'tar with pigz reported a non-zero exit code; falling back to default extractTar.'
        );
        return defaultExtractTar(archivePath, compressionMethod);
    }
}

function bashQuote(p: string): string {
    // Escape single quotes by replacing ' with '\''
    // Then wrap the entire string in single quotes
    return `'${p.replace(/'/g, "'\\''")}'`;
}

function hrtimeSeconds(start: bigint): number {
    const diff = Number(process.hrtime.bigint() - start);
    return diff / 1_000_000_000;
}

function formatSeconds(seconds: number): string {
    return seconds.toFixed(3);
}

function formatThroughput(bytes: number, seconds: number): string | null {
    if (bytes <= 0 || seconds <= 0) {
        return null;
    }
    const mbPerSecond = bytes / seconds / BYTES_PER_MEGABYTE;
    return mbPerSecond.toFixed(2);
}

async function safeLstat(target: string): Promise<fs.Stats | null> {
    try {
        return await fs.promises.lstat(target);
    } catch (error: any) {
        core.debug(`Unable to stat path '${target}': ${error?.message ?? error}`);
        return null;
    }
}

async function getPathBytes(target: string): Promise<number> {
    const stats = await safeLstat(target);
    if (!stats) {
        return 0;
    }

    if (stats.isSymbolicLink() || !stats.isDirectory()) {
        return stats.size;
    }

    let total = 0;
    let entries: string[] = [];
    try {
        entries = await fs.promises.readdir(target);
    } catch (error: any) {
        core.debug(
            `Unable to read directory '${target}': ${error?.message ?? error}`
        );
        return 0;
    }

    for (const entry of entries) {
        total += await getPathBytes(path.join(target, entry));
    }
    return total;
}

async function calculateCachePathsSize(
    cachePaths: string[],
    workspace: string
): Promise<number> {
    let total = 0;
    for (const rawPath of cachePaths) {
        const trimmed = rawPath.trim();
        if (!trimmed || trimmed.startsWith('!')) {
            continue;
        }

        const absolutePath = path.isAbsolute(trimmed)
            ? trimmed
            : path.join(workspace, trimmed);
        total += await getPathBytes(absolutePath);
    }
    return total;
}
