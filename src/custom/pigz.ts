import { CompressionMethod, ManifestFilename, SystemTarPathOnWindows } from '@actions/cache/lib/internal/constants'
import * as cacheUtils from '@actions/cache/lib/internal/cacheUtils'
import {
    createTar as defaultCreateTar,
    extractTar as defaultExtractTar
} from '@actions/cache/lib/internal/tar'

import * as tc from '@actions/tool-cache'
import * as core from '@actions/core'
import * as exec from '@actions/exec'
import * as io from '@actions/io'
import * as os from 'os'
import { writeFileSync } from 'fs'
import * as path from 'path'

const IS_WINDOWS = process.platform === 'win32'

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

    core.info(`Attempting to install pigz on ${platform}...`)

    try {
        if (platform === 'linux') {
            await exec.exec('sudo apt-get update');
            await exec.exec('sudo apt-get install -y pigz');
        } else if (platform === 'darwin') {
            const brew = await io.which('brew', false);
            if (!brew) throw new Error('Homebrew not found');
            await exec.exec('brew install pigz');
        } else if (platform === 'win32') {
            // no package manager available, download binary directly from a trusted source
            const pigzUrl = core.getInput('pigz-download-url');
            if (!pigzUrl) {
                throw new Error('pigz-download-url input is not set');
            }
            const downloadPath = path.join(os.tmpdir(), 'pigz.exe');
            await tc.downloadTool(pigzUrl, downloadPath);
            // make a copy and rename it to unpigz for unzippping
            const pigzPath = path.join(os.tmpdir(), 'pigz.exe');
            await io.cp(downloadPath, pigzPath);
            // add to PATH
            core.addPath(os.tmpdir());
        } else {
            throw new Error(`Unsupported platform: ${platform}`);
        }

        core.info('pigz installation attempt complete.');
    } catch (err: any) {
        core.warning(`Failed to install pigz: ${err.message}`);
    }
}

/**
 * Ensure pigz is installed and accessible.
 * Returns the path or null if we must fall back to tar/gzip.
 */
export async function ensurePigz(): Promise<string | null> {
    // Step 1: check PATH first
    let pigzPath = await findPigz()
    if (pigzPath) {
        core.info(`pigz found at: ${pigzPath}`)
        return pigzPath
    }

    core.info('pigz not found — attempting installation…')

    // Step 2: attempt installation
    await installPigz()

    // Step 3: check again
    pigzPath = await findPigz()
    if (pigzPath) {
        core.info(`pigz successfully installed at: ${pigzPath}`)
        return pigzPath
    }

    // Step 4: fall back to tar/gzip
    core.warning('pigz could not be installed; falling back to tar/gzip.')
    return null
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
        core.warning('Compression method is not gzip; delegating to default createTar.')
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod)
    }

    // Ensure pigz is installed. If not, just use the default tar implementation.
    const pigzPath = await ensurePigz()
    if (!pigzPath) {
        core.warning('pigz is not available; delegating to default createTar.')
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod)
    }

    core.info('Using pigz for gzip compression when creating cache tarball.')

    // Write manifest.txt in the archiveFolder, mirroring the default createTar behavior
    writeFileSync(
        path.join(archiveFolder, ManifestFilename),
        cachePaths.join('\n')
    )

    // Compute the archive filename exactly as actions/cache would
    const cacheFileName = cacheUtils.getCacheFileName(compressionMethod)

    // Normalize to forward slashes for tar
    const cacheFileNameForTar = cacheFileName.replace(new RegExp(`\\${path.sep}`, 'g'), '/')

    // Same working directory semantics as internal tar.ts
    const workingDirectory = (process.env['GITHUB_WORKSPACE'] ?? process.cwd()).replace(
        new RegExp(`\\${path.sep}`, 'g'),
        '/'
    )

    const pigz = IS_WINDOWS ? pigzPath : 'pigz'

    // Build tar command string using pigz as the compressor
    // Equivalent to:
    //   tar --posix -cf <archive> --exclude <archive> -P -C <workspace> --files-from manifest.txt --use-compress-program pigz
    const parts: string[] = [
        `"${pigz}"`,
        '--posix',
        '-cf',
        cacheFileNameForTar,
        '--exclude',
        cacheFileNameForTar,
        '-P',
        '-C',
        workingDirectory,
        '--files-from',
        ManifestFilename,
        '--use-compress-program',
        'pigz'
    ]

    const command = parts.join(' ')
    core.debug(`Running tar with pigz: ${command}`)

    try {
        await exec.exec(command, undefined, {
            cwd: archiveFolder,
            env: {
                ...(process.env as object),
                MSYS: 'winsymlinks:nativestrict'
            }
        })
    } catch (error: any) {
        // If anything goes wrong with pigz/tar, fall back to the default implementation
        core.warning(`tar with pigz failed (${error?.message}); falling back to default createTar.`)
        return defaultCreateTar(archiveFolder, cachePaths, compressionMethod)
    }
}

export async function extractTarWithPigz(
    archivePath: string,
    compressionMethod: CompressionMethod
): Promise<void> {
    // pigz only makes sense for gzip; for zstd variants use the default implementation.
    if (compressionMethod !== CompressionMethod.Gzip) {
        core.warning('Compression method is not gzip; delegating to default extractTar.');
        return defaultExtractTar(archivePath, compressionMethod);
    }

    // Ensure pigz is installed. If not, just use the default tar implementation.
    const pigzPath = await ensurePigz();
    if (!pigzPath) {
        core.warning('pigz is not available; delegating to default extractTar.');
        return defaultExtractTar(archivePath, compressionMethod);
    }

    core.info('Using pigz for gzip decompression when extracting cache tarball.');

    const pigz = IS_WINDOWS ? pigzPath : 'pigz';
    const unpigz = // get unpigz path
        IS_WINDOWS
            ? pigzPath.replace('pigz.exe', 'unpigz.exe')
            : 'unpigz';

    // Build tar command string using pigz as the decompressor
    // Equivalent to:
    //   tar --posix -xf <archive> --use-compress-program pigz
    const parts: string[] = [
        `"${unpigz}"`,
        '--posix',
        '-xf',
        `"${archivePath}"`,
        '--use-compress-program',
        'pigz'
    ];

    const command = parts.join(' ');
    core.debug(`Running tar with pigz: ${command}`);

    try {
        await exec.exec(command, undefined, {
            env: {
                ...(process.env as object),
                MSYS: 'winsymlinks:nativestrict'
            }
        });
    } catch (error: any) {
        // If anything goes wrong with pigz/tar, fall back to the default implementation
        core.warning(`tar with pigz failed (${error?.message}); falling back to default extractTar.`);
        return defaultExtractTar(archivePath, compressionMethod);
    }
}