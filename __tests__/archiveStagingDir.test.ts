import { afterEach, beforeEach, describe, expect, test } from "@jest/globals";
import { existsSync } from "fs";
import { mkdtemp, rm, writeFile } from "fs/promises";
import { tmpdir } from "os";
import * as path from "path";

import {
    createArchiveStagingDirectory,
    ENV_ARCHIVE_DIR,
    removeArchiveStagingDirectory
} from "../src/custom/utils/archiveStagingDir";

// The default (unset) path delegates to utils.createTempDirectory(), which
// reads process.env.RUNNER_TEMP. Point it at a scratch dir so the default is
// deterministic and assertable, and restore the real value afterwards.
const originalRunnerTemp = process.env.RUNNER_TEMP;

describe("createArchiveStagingDirectory (CACHE_ARCHIVE_DIR override)", () => {
    let scratch: string; // stands in for RUNNER_TEMP (the default location)
    let nvme: string; // stands in for a fast disk (CACHE_ARCHIVE_DIR target)
    const created: string[] = [];

    beforeEach(async () => {
        scratch = await mkdtemp(path.join(tmpdir(), "runner-temp-"));
        nvme = await mkdtemp(path.join(tmpdir(), "cache-archive-dir-"));
        process.env.RUNNER_TEMP = scratch;
    });

    afterEach(async () => {
        if (originalRunnerTemp === undefined) {
            delete process.env.RUNNER_TEMP;
        } else {
            process.env.RUNNER_TEMP = originalRunnerTemp;
        }
        for (const dir of [scratch, nvme, ...created]) {
            await rm(dir, { recursive: true, force: true }).catch(
                () => undefined
            );
        }
        created.length = 0;
    });

    test("CACHE_ARCHIVE_DIR set -> unique subdir + archive path are under it", async () => {
        const staging = await createArchiveStagingDirectory({
            [ENV_ARCHIVE_DIR]: nvme
        } as NodeJS.ProcessEnv);
        created.push(staging.dir);

        expect(staging.isCustom).toBe(true);
        // The staging dir is a UNIQUE subdirectory directly inside the override.
        expect(path.dirname(staging.dir)).toBe(nvme);
        expect(staging.dir).not.toBe(nvme);
        // It was actually created on disk (recursive mkdir).
        expect(existsSync(staging.dir)).toBe(true);
        // The archive file the caller writes lives under CACHE_ARCHIVE_DIR too.
        const archivePath = path.join(staging.dir, "cache.tzst");
        expect(
            path
                .resolve(archivePath)
                .startsWith(path.resolve(nvme) + path.sep)
        ).toBe(true);
    });

    test("CACHE_ARCHIVE_DIR set -> concurrent calls never collide (unique subdirs)", async () => {
        const a = await createArchiveStagingDirectory({
            [ENV_ARCHIVE_DIR]: nvme
        } as NodeJS.ProcessEnv);
        const b = await createArchiveStagingDirectory({
            [ENV_ARCHIVE_DIR]: nvme
        } as NodeJS.ProcessEnv);
        created.push(a.dir, b.dir);
        expect(a.dir).not.toBe(b.dir);
        expect(path.dirname(a.dir)).toBe(nvme);
        expect(path.dirname(b.dir)).toBe(nvme);
    });

    test("CACHE_ARCHIVE_DIR unset -> unchanged default (UUID dir under RUNNER_TEMP)", async () => {
        const staging = await createArchiveStagingDirectory(
            {} as NodeJS.ProcessEnv
        );
        created.push(staging.dir);

        expect(staging.isCustom).toBe(false);
        // Byte-for-byte upstream: the dir is created under RUNNER_TEMP.
        expect(
            path
                .resolve(staging.dir)
                .startsWith(path.resolve(scratch) + path.sep)
        ).toBe(true);
        expect(existsSync(staging.dir)).toBe(true);
    });

    test("CACHE_ARCHIVE_DIR blank -> treated as unset (default path)", async () => {
        const staging = await createArchiveStagingDirectory({
            [ENV_ARCHIVE_DIR]: "   "
        } as NodeJS.ProcessEnv);
        created.push(staging.dir);

        expect(staging.isCustom).toBe(false);
        expect(
            path
                .resolve(staging.dir)
                .startsWith(path.resolve(scratch) + path.sep)
        ).toBe(true);
    });

    test("bad/unwritable CACHE_ARCHIVE_DIR -> graceful fallback to default (no throw)", async () => {
        // A path whose parent is a regular FILE cannot be mkdir'd (ENOTDIR),
        // so the override is unusable and must fall back to the default dir.
        const asFile = path.join(nvme, "not-a-dir");
        await writeFile(asFile, "x");
        const badDir = path.join(asFile, "sub");

        const staging = await createArchiveStagingDirectory({
            [ENV_ARCHIVE_DIR]: badDir
        } as NodeJS.ProcessEnv);
        created.push(staging.dir);

        // Fell back to the RUNNER_TEMP default — a bad value never breaks it.
        expect(staging.isCustom).toBe(false);
        expect(
            path
                .resolve(staging.dir)
                .startsWith(path.resolve(scratch) + path.sep)
        ).toBe(true);
        expect(existsSync(staging.dir)).toBe(true);
    });
});

describe("removeArchiveStagingDirectory (custom-dir cleanup)", () => {
    test("removes the whole staging subdir, archive file included", async () => {
        const nvme = await mkdtemp(path.join(tmpdir(), "cache-archive-dir-"));
        const staging = await createArchiveStagingDirectory({
            [ENV_ARCHIVE_DIR]: nvme
        } as NodeJS.ProcessEnv);
        await writeFile(path.join(staging.dir, "cache.tzst"), "payload");
        expect(existsSync(staging.dir)).toBe(true);

        await removeArchiveStagingDirectory(staging.dir);
        expect(existsSync(staging.dir)).toBe(false);

        await rm(nvme, { recursive: true, force: true }).catch(
            () => undefined
        );
    });

    test("is a no-op for an empty path and never throws on a missing dir", async () => {
        await expect(
            removeArchiveStagingDirectory("")
        ).resolves.toBeUndefined();
        await expect(
            removeArchiveStagingDirectory(
                path.join(tmpdir(), "does-not-exist-" + Date.now())
            )
        ).resolves.toBeUndefined();
    });
});
