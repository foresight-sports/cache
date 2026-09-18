import {
    afterAll,
    afterEach,
    beforeAll,
    beforeEach,
    describe,
    expect,
    jest,
    test
} from "@jest/globals";
import { createHash } from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import {
    SHARDED_ARCHIVE_FORMAT,
    ShardManifest
} from "../src/custom/shardedArchive";

// cache.ts pulls in backend.ts, which reads its S3 configuration at import
// time; pin the env before the (dynamic) import. Nothing here touches S3: the
// download / hash / extract seams of the sharded restore are all injected.
const originalEnv = {
    bucket: process.env.RUNS_ON_S3_BUCKET_CACHE,
    region: process.env.RUNS_ON_AWS_REGION,
    repository: process.env.GITHUB_REPOSITORY
};
process.env.RUNS_ON_S3_BUCKET_CACHE = "test-bucket";
process.env.RUNS_ON_AWS_REGION = "us-east-1";
process.env.GITHUB_REPOSITORY = "owner/repo";

const cache = await import("../src/custom/cache");

afterAll(() => {
    process.env.RUNS_ON_S3_BUCKET_CACHE = originalEnv.bucket;
    process.env.RUNS_ON_AWS_REGION = originalEnv.region;
    process.env.GITHUB_REPOSITORY = originalEnv.repository;
});

const sha256Of = (data: Buffer | string): string =>
    createHash("sha256").update(data).digest("hex");

const entryKey = "lib-abc";
const archiveLocation = `s3://test-bucket/cache/owner/repo/v/${entryKey}`;
const prefixLocation = "s3://test-bucket/cache/owner/repo/v";

/** In-memory objects the fake download serves, by full S3 location. */
type FakeObjects = Map<string, Buffer>;

function makeDeps(objects: FakeObjects): cache.ShardedRestoreDeps & {
    downloaded: string[];
    extracted: string[][];
} {
    const downloaded: string[] = [];
    const extracted: string[][] = [];
    const deps: cache.ShardedRestoreDeps & {
        downloaded: string[];
        extracted: string[][];
    } = {
        downloaded,
        extracted,
        downloadCache: jest.fn(async (location: string, partPath: string) => {
            downloaded.push(location);
            const body = objects.get(location);
            if (body === undefined) {
                throw new Error(`NoSuchKey: ${location}`);
            }
            fs.writeFileSync(partPath, body);
        }) as cache.ShardedRestoreDeps["downloadCache"],
        hashFile: jest.fn(async (filePath: string) =>
            sha256Of(fs.readFileSync(filePath))
        ) as cache.ShardedRestoreDeps["hashFile"],
        listPart: jest.fn(
            async () => undefined
        ) as cache.ShardedRestoreDeps["listPart"],
        extractParts: jest.fn(async (partPaths: string[]) => {
            extracted.push(partPaths);
        }) as cache.ShardedRestoreDeps["extractParts"],
        // Default: every file the manifest recorded is present afterwards.
        countRestoredFiles: jest.fn(
            () => Number.MAX_SAFE_INTEGER
        ) as cache.ShardedRestoreDeps["countRestoredFiles"],
        clearRestoredPaths: jest.fn(
            () => undefined
        ) as cache.ShardedRestoreDeps["clearRestoredPaths"]
    };
    return deps;
}

describe("restoreShardedArchive", () => {
    let staging: string;
    let partPaths: string[];
    let stdoutLines: string[];
    let stdoutSpy: jest.SpiedFunction<typeof process.stdout.write>;

    beforeAll(() => {
        stdoutSpy = jest.spyOn(process.stdout, "write");
    });

    beforeEach(() => {
        staging = fs.mkdtempSync(path.join(os.tmpdir(), "shards-restore-"));
        partPaths = [
            path.join(staging, "part-00.tzst"),
            path.join(staging, "part-01.tzst")
        ];
        stdoutLines = [];
        stdoutSpy.mockImplementation(((chunk: unknown) => {
            stdoutLines.push(String(chunk));
            return true;
        }) as typeof process.stdout.write);
    });

    afterEach(() => {
        stdoutSpy.mockReset();
        fs.rmSync(staging, { recursive: true, force: true });
    });

    afterAll(() => {
        stdoutSpy.mockRestore();
    });

    const partA = Buffer.alloc(64, "gen-a:0");
    const partB = Buffer.alloc(64, "gen-a:1");

    const generationManifest = (): ShardManifest => ({
        format: SHARDED_ARCHIVE_FORMAT,
        generation: "gen-a",
        shards: [
            {
                name: "part-00.tzst",
                key: `${entryKey}.shards/gen-a/part-00.tzst`,
                bytes: partA.length,
                files: 2,
                sha256: sha256Of(partA)
            },
            {
                name: "part-01.tzst",
                key: `${entryKey}.shards/gen-a/part-01.tzst`,
                bytes: partB.length,
                files: 3,
                sha256: sha256Of(partB)
            }
        ],
        totalBytes: partA.length + partB.length,
        totalFiles: 5,
        createdAt: "2026-01-01T00:00:00.000Z"
    });

    test("downloads exactly the keys the manifest lists, verifies sha256, then extracts", async () => {
        const objects: FakeObjects = new Map([
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`, partA],
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`, partB],
            // Same-sized parts of a newer, interrupted generation and of the
            // legacy layout: none of these may be touched.
            [
                `${prefixLocation}/${entryKey}.shards/gen-b/part-00.tzst`,
                Buffer.alloc(64, "gen-b:0")
            ],
            [
                `${prefixLocation}/${entryKey}.shards/part-00.tzst`,
                Buffer.alloc(64, "legacy")
            ]
        ]);
        const deps = makeDeps(objects);

        await cache.restoreShardedArchive(
            generationManifest(),
            archiveLocation,
            entryKey,
            partPaths,
            ["Library"],
            undefined,
            deps
        );

        expect(deps.downloaded.slice().sort()).toEqual([
            `${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`,
            `${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`
        ]);
        expect(deps.hashFile).toHaveBeenCalledTimes(2);
        expect(deps.extracted).toEqual([partPaths]);
        expect(fs.readFileSync(partPaths[0]).equals(partA)).toBe(true);
        expect(fs.readFileSync(partPaths[1]).equals(partB)).toBe(true);
        expect(stdoutLines.join("")).toMatch(/generation gen-a/);
        expect(stdoutLines.join("")).toMatch(/sha256 hashing/);
        expect(stdoutLines.join("")).not.toMatch(/not verified/);
    });

    test("a sha256 mismatch fails the restore as a DownloadValidationError before extracting", async () => {
        const objects: FakeObjects = new Map([
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`, partA],
            // Right size, wrong bytes: the size check passes, the digest must not.
            [
                `${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`,
                Buffer.alloc(64, "gen-b:1")
            ]
        ]);
        const deps = makeDeps(objects);

        const failure = await cache
            .restoreShardedArchive(
                generationManifest(),
                archiveLocation,
                entryKey,
                partPaths,
                ["Library"],
                undefined,
                deps
            )
            .then(
                () => undefined,
                (error: Error) => error
            );
        expect(failure).toBeInstanceOf(cache.DownloadValidationError);
        expect(failure?.name).toBe("DownloadValidationError");
        expect(failure?.message).toMatch(
            /part-01\.tzst has sha256 [0-9a-f]{64} but the manifest recorded/
        );
        expect(deps.extracted).toEqual([]);
    });

    test("a size mismatch is still a DownloadValidationError", async () => {
        const objects: FakeObjects = new Map([
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`, partA],
            [
                `${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`,
                Buffer.concat([partB, Buffer.from("x")])
            ]
        ]);
        const deps = makeDeps(objects);
        await expect(
            cache.restoreShardedArchive(
                generationManifest(),
                archiveLocation,
                entryKey,
                partPaths,
                ["Library"],
                undefined,
                deps
            )
        ).rejects.toMatchObject({
            name: "DownloadValidationError",
            message: expect.stringMatching(
                /is 65 B but the manifest recorded 64 B/
            )
        });
        expect(deps.extracted).toEqual([]);
    });

    test("a legacy manifest (no generation / key / sha256) restores via the old names, size-checked only", async () => {
        const objects: FakeObjects = new Map([
            [`${archiveLocation}.shards/part-00.tzst`, partA],
            [`${archiveLocation}.shards/part-01.tzst`, partB]
        ]);
        const deps = makeDeps(objects);
        const legacy: ShardManifest = {
            format: SHARDED_ARCHIVE_FORMAT,
            shards: [
                { name: "part-00.tzst", bytes: partA.length, files: 2 },
                { name: "part-01.tzst", bytes: partB.length, files: 3 }
            ],
            totalBytes: partA.length + partB.length,
            totalFiles: 5,
            createdAt: "2026-01-01T00:00:00.000Z"
        };

        await cache.restoreShardedArchive(
            legacy,
            archiveLocation,
            entryKey,
            partPaths,
            ["Library"],
            undefined,
            deps
        );

        expect(deps.downloaded.slice().sort()).toEqual([
            `${archiveLocation}.shards/part-00.tzst`,
            `${archiveLocation}.shards/part-01.tzst`
        ]);
        expect(deps.hashFile).not.toHaveBeenCalled();
        expect(deps.extracted).toEqual([partPaths]);
        const output = stdoutLines.join("");
        expect(output).toMatch(/legacy manifest without a generation/);
        expect(output).toMatch(
            /part-00\.tzst: manifest records no sha256; content not verified/
        );
        expect(output).toMatch(
            /part-01\.tzst: manifest records no sha256; content not verified/
        );
    });

    test("a manifest whose key points outside the entry is refused before any download", async () => {
        const deps = makeDeps(new Map());
        const manifest = generationManifest();
        manifest.shards[1].key = "other-entry.shards/gen-a/part-01.tzst";
        await expect(
            cache.restoreShardedArchive(
                manifest,
                archiveLocation,
                entryKey,
                partPaths,
                ["Library"],
                undefined,
                deps
            )
        ).rejects.toThrow(/references a part outside the entry/);
        expect(deps.downloadCache).not.toHaveBeenCalled();
        expect(deps.extracted).toEqual([]);
    });
    test("an extract failure empties the cache paths before the error propagates", async () => {
        const objects: FakeObjects = new Map([
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`, partA],
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`, partB]
        ]);
        const deps = makeDeps(objects);
        (deps.extractParts as jest.Mock).mockImplementation(async () => {
            throw new Error(
                "part part-01.tzst extract failed: The process '/usr/bin/tar' failed with exit code 2"
            );
        });

        await expect(
            cache.restoreShardedArchive(
                generationManifest(),
                archiveLocation,
                entryKey,
                partPaths,
                ["Library"],
                undefined,
                deps
            )
        ).rejects.toThrow(/part-01.tzst extract failed/);

        expect(deps.clearRestoredPaths).toHaveBeenCalledTimes(1);
        expect(deps.clearRestoredPaths).toHaveBeenCalledWith(["Library"]);
        expect(deps.countRestoredFiles).not.toHaveBeenCalled();
        expect(stdoutLines.join("")).toMatch(/clean miss/);
    });

    test("fewer files than the manifest recorded is a failure that also empties the cache paths", async () => {
        const objects: FakeObjects = new Map([
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`, partA],
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`, partB]
        ]);
        const deps = makeDeps(objects);
        (deps.countRestoredFiles as jest.Mock).mockImplementation(() => 4);

        await expect(
            cache.restoreShardedArchive(
                generationManifest(),
                archiveLocation,
                entryKey,
                partPaths,
                ["Library"],
                undefined,
                deps
            )
        ).rejects.toThrow(
            /incomplete: 4 regular files exist under Library but the manifest recorded 5/
        );

        expect(deps.extracted).toEqual([partPaths]);
        expect(deps.countRestoredFiles).toHaveBeenCalledWith(["Library"]);
        expect(deps.clearRestoredPaths).toHaveBeenCalledWith(["Library"]);
    });

    test("a complete restore verifies the file count and leaves the tree alone", async () => {
        const objects: FakeObjects = new Map([
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-00.tzst`, partA],
            [`${prefixLocation}/${entryKey}.shards/gen-a/part-01.tzst`, partB]
        ]);
        const deps = makeDeps(objects);
        (deps.countRestoredFiles as jest.Mock).mockImplementation(() => 5);

        await cache.restoreShardedArchive(
            generationManifest(),
            archiveLocation,
            entryKey,
            partPaths,
            ["Library"],
            undefined,
            deps
        );

        expect(deps.clearRestoredPaths).not.toHaveBeenCalled();
        expect(stdoutLines.join("")).toMatch(/verified 5 files/);
    });
});
