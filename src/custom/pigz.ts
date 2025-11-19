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
import fs from 'fs'

const IS_WINDOWS = process.platform === 'win32'

interface TarResolution {
    path: string
    useForceLocal: boolean
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

async function findUnpigz(): Promise<string | null> {
    try {
        const found = await io.which('unpigz', false);
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

            core.info(`Checking for unpigz.exe in: ${pigzDir}`);
            const unpigzPath = path.join(pigzDir, 'unpigz.exe');

            if (!fs.existsSync(unpigzPath)) {
                throw new Error(`unpigz.exe not found at expected location: ${unpigzPath}`);
            }

            core.info('Adding pigz to the tool cache...');

            // add dir to tool cache
            const toolPath = await tc.cacheDir(
                pigzDir,
                'pigz',
                'latest'
            );

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

async function ensureUnpigz(): Promise<string | null> {
    let unpigzPath = await findUnpigz()
    if (unpigzPath) {
        core.info(`unpigz found at: ${unpigzPath}`)
        return unpigzPath
    }

    core.info('unpigz not found — attempting installation…')
    await installPigz()

    unpigzPath = await findUnpigz()
    if (unpigzPath) {
        core.info(`unpigz successfully installed at: ${unpigzPath}`)
        return unpigzPath
    }

    core.warning('unpigz could not be installed; falling back to tar/gzip.')
    return null
}

async function resolveTar(): Promise<TarResolution> {
    if (IS_WINDOWS) {
        const gnuTar = await cacheUtils.getGnuTarPathOnWindows()
        if (gnuTar) {
            return { path: gnuTar, useForceLocal: true }
        }
        return { path: SystemTarPathOnWindows, useForceLocal: false }
    }

    const tarPath = await io.which('tar', true)
    return { path: tarPath, useForceLocal: false }
}

function getWorkingDirectory(): string {
    return (process.env['GITHUB_WORKSPACE'] ?? process.cwd()).replace(
        new RegExp(`\\${path.sep}`, 'g'),
        '/'
    )
}

async function createProgramWrapper(
    executable: string,
    args: string[],
    label: string
): Promise<string> {
    const runnerTemp = process.env['RUNNER_TEMP']?.trim()
    const baseTempDir = runnerTemp && runnerTemp.length > 0 ? runnerTemp : os.tmpdir()
    await io.mkdirP(baseTempDir)
    const tempDir = await fs.promises.mkdtemp(
        path.join(baseTempDir, `${label}-wrapper-`)
    )

    if (IS_WINDOWS) {
        const wrapperPath = path.join(tempDir, `${label}-wrapper.cmd`)
        const content = `@echo off\r\n"${executable}" ${args.join(' ')} %*\r\n`
        await fs.promises.writeFile(wrapperPath, content, {
            encoding: 'utf8'
        })
        return wrapperPath
    }

    const wrapperPath = path.join(tempDir, `${label}-wrapper.sh`)
    const script = `#!/bin/sh\n"${executable}" ${args.join(' ')} "$@"\n`
    await fs.promises.writeFile(wrapperPath, script, {
        encoding: 'utf8'
    })
    await fs.promises.chmod(wrapperPath, 0o755)
    return wrapperPath
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
    const workingDirectory = getWorkingDirectory()

    const threadCount = Math.max(os.cpus().length, 1)
    const pigzWrapperPath = await createProgramWrapper(
        pigzPath,
        ['--fast', '-p', threadCount.toString()],
        'pigz'
    )
    const compressProgramPath = pigzWrapperPath.replace(
        new RegExp(`\\${path.sep}`, 'g'),
        '/'
    )
    const pigzProgram = `"${compressProgramPath}"`
    const tarResolution = await resolveTar()

    // Build tar command string using pigz as the compressor
    // Equivalent to:
    //   tar --posix -cf <archive> --exclude <archive> -P -C <workspace> --files-from manifest.txt --use-compress-program pigz
    const parts: string[] = [
        `"${tarResolution.path}"`,
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
        pigzProgram
    ]

    if (tarResolution.useForceLocal) {
        parts.push('--force-local')
    }

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

    const unpigzPath = await ensureUnpigz()
    if (!unpigzPath) {
        core.warning('unpigz is not available; delegating to default extractTar.')
        return defaultExtractTar(archivePath, compressionMethod)
    }

    core.info('Using unpigz for gzip decompression when extracting cache tarball.')

    const threadCount = Math.max(os.cpus().length, 1)
    const unpigzWrapperPath = await createProgramWrapper(
        unpigzPath,
        ['-p', threadCount.toString()],
        'unpigz'
    );
    const decompressorProgramPath = unpigzWrapperPath.replace(
        new RegExp(`\\${path.sep}`, 'g'),
        '/'
    );
    const decompressorProgram = `"${decompressorProgramPath}"`;
    const tarResolution = await resolveTar();
    const workingDirectory = getWorkingDirectory();
    await io.mkdirP(workingDirectory);
    const normalizedArchivePath = archivePath.replace(new RegExp(`\\${path.sep}`, 'g'), '/');

    // Build tar command string using unpigz as the decompressor
    // Equivalent to:
    //   tar -xf <archive> -P -C <workspace> --use-compress-program "unpigz -p ..."
    const parts: string[] = [
        `"${tarResolution.path}"`,
        '-xf',
        normalizedArchivePath,
        '-P',
        '-C',
        workingDirectory,
        '--use-compress-program',
        decompressorProgram
    ];

    if (tarResolution.useForceLocal) {
        parts.push('--force-local');
    }

    const command = parts.join(' ');
    core.debug(`Running tar with unpigz: ${command}`);

    try {
        await exec.exec(command, undefined, {
            env: {
                ...(process.env as object),
                MSYS: 'winsymlinks:nativestrict'
            }
        });
    } catch (error: any) {
        // If anything goes wrong with unpigz/tar, fall back to the default implementation
        core.warning(`tar with unpigz failed (${error?.message}); falling back to default extractTar.`);
        return defaultExtractTar(archivePath, compressionMethod);
    }
}