// Configurable staging directory for the scratch `cache.tzst` archive.
//
// The file-based restore/save writes the whole (tens-of-GB) archive to a temp
// dir before extracting / uploading. Upstream stages it under
// `utils.createTempDirectory()` -> `RUNNER_TEMP`
// (`C:\actions-runner\_work\_temp`), which on our runners is the C: gp3 EBS
// root (~400 MB/s, ~3000 IOPS, ~1 ms write latency). 256-way concurrent
// positional writes to that one 61 GB file stall the download well below the
// NIC. Instance-store NVMe (Z:, ~10x lower latency, no gp3 cap) is available,
// but the consuming workflow only junctions Library/Builds/Temp, not
// RUNNER_TEMP, so the archive still lands on gp3.
//
// `CACHE_ARCHIVE_DIR` lets the caller point the archive staging at any
// directory (e.g. NVMe). When set and non-empty, the archive is written under a
// unique subdirectory INSIDE `CACHE_ARCHIVE_DIR`; when unset/blank, behavior is
// byte-for-byte the upstream default (a UUID dir under RUNNER_TEMP). A
// missing/unwritable value falls back to the default with a warning, so a bad
// value can never break a restore or save.
import * as core from "@actions/core";
import { randomUUID } from "crypto";
import { mkdir, rm } from "fs/promises";
import * as path from "path";

import { cacheUtils as utils } from "../../actionsCacheShims.js";

/**
 * Env var: absolute path of a directory to stage the scratch `cache.tzst`
 * archive under (e.g. fast instance-store NVMe), overriding the default
 * `RUNNER_TEMP` location for both restore (download) and save (createTar).
 */
export const ENV_ARCHIVE_DIR = "CACHE_ARCHIVE_DIR";

export interface ArchiveStagingDir {
    /** Directory the `cache.tzst` archive should be written into. */
    dir: string;
    /**
     * True when `dir` is a unique subdir created under `CACHE_ARCHIVE_DIR` — the
     * caller removes the whole subdir on cleanup. False when `dir` is the
     * default `RUNNER_TEMP` location, in which case the caller keeps upstream's
     * file-only unlink (byte-for-byte default behavior).
     */
    isCustom: boolean;
}

/**
 * Resolve the directory that will hold the scratch `cache.tzst` archive.
 *
 * Default (`CACHE_ARCHIVE_DIR` unset/blank): delegate to
 * `utils.createTempDirectory()` — a unique UUID dir under `RUNNER_TEMP` — so the
 * behavior is byte-for-byte the upstream default.
 *
 * Override (`CACHE_ARCHIVE_DIR` set): create a unique UUID subdirectory INSIDE
 * `CACHE_ARCHIVE_DIR` (recursive mkdir, so concurrent caches never collide) and
 * return it. If that directory cannot be created (missing/unwritable path),
 * warn and fall back to the default temp dir so a bad value can never break a
 * restore or save.
 */
export async function createArchiveStagingDirectory(
    env: NodeJS.ProcessEnv = process.env
): Promise<ArchiveStagingDir> {
    const configured = (env[ENV_ARCHIVE_DIR] ?? "").trim();
    if (!configured) {
        // Default: byte-for-byte upstream — a UUID dir under RUNNER_TEMP.
        return { dir: await utils.createTempDirectory(), isCustom: false };
    }

    // Override: a unique subdir inside CACHE_ARCHIVE_DIR so concurrent caches
    // never collide, staged on the caller's fast disk (e.g. NVMe).
    const dir = path.join(configured, randomUUID());
    try {
        await mkdir(dir, { recursive: true });
        core.debug(`Staging cache archive under CACHE_ARCHIVE_DIR: ${dir}`);
        return { dir, isCustom: true };
    } catch (error) {
        core.warning(
            `CACHE_ARCHIVE_DIR="${configured}" is unusable ` +
                `(${(error as Error).message}); falling back to the default ` +
                `archive staging directory.`
        );
        return { dir: await utils.createTempDirectory(), isCustom: false };
    }
}

/**
 * Remove a custom staging directory (the whole unique subdir, archive file
 * included) so neither the multi-GB archive nor the empty scratch dir leaks on
 * the fast disk. Best effort — never throws. Only call this for
 * `isCustom === true` dirs; the default RUNNER_TEMP path keeps upstream's
 * file-only unlink.
 */
export async function removeArchiveStagingDirectory(
    dir: string
): Promise<void> {
    if (!dir) {
        return;
    }
    try {
        await rm(dir, { recursive: true, force: true });
    } catch (error) {
        core.debug(`Failed to delete archive staging dir ${dir}: ${error}`);
    }
}
