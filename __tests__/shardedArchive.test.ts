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
    assertShardablePaths,
    assignFilesToShards,
    CacheFileEntry,
    createShardedArchive,
    enumerateCacheEntries,
    ENV_ARCHIVE_SHARDS,
    extractShardedArchive,
    getArchiveShardCount,
    hashFileSha256,
    isShardPartObjectKey,
    mapWithConcurrency,
    newShardGeneration,
    parseShardManifest,
    resolveShardPartLocation,
    rewriteZstdThreadsForShards,
    SHARDED_ARCHIVE_FORMAT,
    ShardedArchiveDeps,
    shardGenerationKeyPrefix,
    shardListName,
    shardPartKey,
    shardPartName,
    shardsKeyPrefix
} from "../src/custom/shardedArchive";

const sha256Of = (data: Buffer | string): string =>
    createHash("sha256").update(data).digest("hex");

// ============================================================================
// CACHE_ARCHIVE_SHARDS parsing (2..64; anything else = legacy single archive).
// ============================================================================
describe("getArchiveShardCount", () => {
    test("unset / blank / 0 / 1 mean a single archive", () => {
        expect(getArchiveShardCount({})).toBe(1);
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: "" })).toBe(1);
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: "   " })).toBe(1);
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: "0" })).toBe(1);
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: "1" })).toBe(1);
    });

    test("accepts integers in 2..64 (trimmed)", () => {
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: "2" })).toBe(2);
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: " 8 " })).toBe(8);
        expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: "64" })).toBe(64);
    });

    test("rejects non-integers and out-of-range values as legacy", () => {
        for (const bad of ["65", "1000", "-4", "4.5", "four", "8x", "0x8"]) {
            expect(getArchiveShardCount({ [ENV_ARCHIVE_SHARDS]: bad })).toBe(1);
        }
    });
});

// ============================================================================
// Shard assignment: balanced by bytes, deterministic, empty dirs in shard 0.
// ============================================================================
describe("assignFilesToShards", () => {
    const files: CacheFileEntry[] = [
        { relPath: "Library/a", size: 100 },
        { relPath: "Library/b", size: 90 },
        { relPath: "Library/c", size: 50 },
        { relPath: "Library/d", size: 40 },
        { relPath: "Library/e", size: 30 },
        { relPath: "Library/f", size: 10 }
    ];

    test("largest file goes first and each file lands on the lightest shard", () => {
        const shards = assignFilesToShards(files, [], 3);
        expect(shards).toHaveLength(3);
        // 100 -> s0 (100), 90 -> s1 (90), 50 -> s2 (50), 40 -> s2 (90),
        // 30 -> s1 (tie 90/90, lowest index; 120), 10 -> s2 (100).
        expect(shards[0].relPaths).toEqual(["Library/a"]);
        expect(shards[1].relPaths).toEqual(["Library/b", "Library/e"]);
        expect(shards[2].relPaths).toEqual([
            "Library/c",
            "Library/d",
            "Library/f"
        ]);
        expect(shards.map(shard => shard.bytes)).toEqual([100, 120, 100]);
        expect(shards.map(shard => shard.files)).toEqual([1, 2, 3]);
    });

    test("balances a skewed distribution so no shard is far above the mean", () => {
        const many: CacheFileEntry[] = [];
        for (let index = 0; index < 500; index++) {
            many.push({
                relPath: `f/${index}`,
                size: ((index * 7919) % 997) + 1
            });
        }
        const shards = assignFilesToShards(many, [], 4);
        const total = many.reduce((sum, file) => sum + file.size, 0);
        const mean = total / 4;
        for (const shard of shards) {
            expect(Math.abs(shard.bytes - mean)).toBeLessThan(mean * 0.02);
        }
        expect(shards.reduce((sum, shard) => sum + shard.files, 0)).toBe(500);
        expect(shards.reduce((sum, shard) => sum + shard.bytes, 0)).toBe(total);
    });

    test("is deterministic regardless of input order", () => {
        const shuffled = files.slice().reverse();
        expect(assignFilesToShards(shuffled, [], 3)).toEqual(
            assignFilesToShards(files, [], 3)
        );
        // Equal sizes: tie broken by path, so still deterministic.
        const ties: CacheFileEntry[] = [
            { relPath: "z", size: 5 },
            { relPath: "a", size: 5 },
            { relPath: "m", size: 5 }
        ];
        expect(assignFilesToShards(ties, [], 2)).toEqual(
            assignFilesToShards(ties.slice().reverse(), [], 2)
        );
        // a -> s0, m -> s1, z -> s0 (tie, lowest index).
        expect(assignFilesToShards(ties, [], 2)[0].relPaths).toEqual([
            "a",
            "z"
        ]);
    });

    test("empty directories go to shard 0 and do not count as files", () => {
        const shards = assignFilesToShards(
            files,
            ["Library/empty2", "Library/empty1"],
            2
        );
        expect(shards[0].relPaths.slice(-2)).toEqual([
            "Library/empty1",
            "Library/empty2"
        ]);
        expect(shards[0].files).toBe(3);
        expect(shards[1].relPaths).not.toContain("Library/empty1");
    });

    test("drops shards that received nothing (fewer entries than shards)", () => {
        const shards = assignFilesToShards(files.slice(0, 2), [], 8);
        expect(shards).toHaveLength(2);
        expect(assignFilesToShards([], [], 4)).toEqual([]);
        // Only empty dirs: they all live in one (the first) shard.
        expect(assignFilesToShards([], ["x"], 4)).toEqual([
            { relPaths: ["x"], bytes: 0, files: 0 }
        ]);
    });
});

// ============================================================================
// zstd thread rewrite: -T0 -> -T<cores / shards>; an explicit -T8 stays.
// ============================================================================
describe("rewriteZstdThreadsForShards", () => {
    test("replaces -T0 with the per-shard core share", () => {
        expect(rewriteZstdThreadsForShards("-T0 -3 --long=30", 4, 16)).toBe(
            "-T4 -3 --long=30"
        );
        expect(rewriteZstdThreadsForShards("-T0 -1", 8, 64)).toBe("-T8 -1");
    });

    test("never goes below one thread", () => {
        expect(rewriteZstdThreadsForShards("-T0 -3", 16, 4)).toBe("-T1 -3");
        expect(rewriteZstdThreadsForShards("-T0 -3", 4, 0)).toBe("-T1 -3");
    });

    test("leaves an operator-chosen thread count and other tokens alone", () => {
        expect(rewriteZstdThreadsForShards("-T8 -3 --long=30", 4, 16)).toBe(
            "-T8 -3 --long=30"
        );
        expect(rewriteZstdThreadsForShards("-3 --long=30", 4, 16)).toBe(
            "-3 --long=30"
        );
        expect(rewriteZstdThreadsForShards("  -T0   -1 ", 2, 8)).toBe("-T4 -1");
    });
});

// ============================================================================
// Manifest detection: JSON with the format field vs anything else.
// ============================================================================
describe("parseShardManifest", () => {
    const manifest = {
        format: SHARDED_ARCHIVE_FORMAT,
        shards: [
            { name: "part-00.tzst", bytes: 10, files: 2 },
            { name: "part-01.tzst", bytes: 20, files: 3 }
        ],
        totalBytes: 30,
        totalFiles: 5,
        createdAt: "2026-01-01T00:00:00.000Z"
    };

    test("accepts a well-formed manifest (string or Buffer)", () => {
        expect(parseShardManifest(JSON.stringify(manifest))).toEqual(manifest);
        expect(
            parseShardManifest(Buffer.from(JSON.stringify(manifest), "utf8"))
        ).toEqual(manifest);
    });

    test("rejects binary (a zstd archive) and non-manifest JSON", () => {
        // zstd magic 0x28 B5 2F FD followed by junk: never JSON.
        const zstdBytes = Buffer.from([
            0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x00, 0x00
        ]);
        expect(parseShardManifest(zstdBytes)).toBeUndefined();
        expect(parseShardManifest("")).toBeUndefined();
        expect(parseShardManifest("not json")).toBeUndefined();
        expect(parseShardManifest("[]")).toBeUndefined();
        expect(parseShardManifest("null")).toBeUndefined();
        expect(
            parseShardManifest(JSON.stringify({ shards: [] }))
        ).toBeUndefined();
        expect(
            parseShardManifest(
                JSON.stringify({ ...manifest, format: "sharded-tzst-v2" })
            )
        ).toBeUndefined();
    });

    test("keeps generation, per-part key and sha256 when present", () => {
        const digest = sha256Of("part-00");
        const modern = {
            ...manifest,
            generation: "20260915T101112123Z-0badf00d",
            shards: [
                {
                    name: "part-00.tzst",
                    key: "my-key.shards/20260915T101112123Z-0badf00d/part-00.tzst",
                    bytes: 10,
                    files: 2,
                    sha256: digest
                },
                {
                    name: "part-01.tzst",
                    key: "my-key.shards/20260915T101112123Z-0badf00d/part-01.tzst",
                    bytes: 20,
                    files: 3,
                    sha256: digest
                }
            ]
        };
        expect(parseShardManifest(JSON.stringify(modern))).toEqual(modern);
        // A legacy manifest parses without the new fields at all.
        const legacy = parseShardManifest(JSON.stringify(manifest));
        expect(legacy).toBeDefined();
        expect("generation" in legacy!).toBe(false);
        expect("key" in legacy!.shards[0]).toBe(false);
        expect("sha256" in legacy!.shards[0]).toBe(false);
    });

    test("rejects malformed generation, key or sha256 fields", () => {
        const withGeneration = (generation: unknown): string =>
            JSON.stringify({ ...manifest, generation });
        expect(parseShardManifest(withGeneration(42))).toBeUndefined();
        expect(parseShardManifest(withGeneration("a/b"))).toBeUndefined();
        expect(parseShardManifest(withGeneration(""))).toBeUndefined();

        const withKey = (key: unknown): string =>
            JSON.stringify({
                ...manifest,
                shards: [{ name: "part-00.tzst", key, bytes: 1, files: 1 }]
            });
        expect(parseShardManifest(withKey(7))).toBeUndefined();
        expect(
            parseShardManifest(withKey("/abs.shards/g/part-00.tzst"))
        ).toBeUndefined();
        expect(
            parseShardManifest(withKey("k.shards/../part-00.tzst"))
        ).toBeUndefined();
        expect(
            parseShardManifest(withKey("k.shards/g//part-00.tzst"))
        ).toBeUndefined();
        expect(
            parseShardManifest(withKey("k.shards\\g\\part-00.tzst"))
        ).toBeUndefined();
        expect(parseShardManifest(withKey("k/g/part-00.tzst"))).toBeUndefined();
        // Key must end with the entry's own part name.
        expect(
            parseShardManifest(withKey("k.shards/g/part-01.tzst"))
        ).toBeUndefined();
        expect(
            parseShardManifest(withKey("k.shards/g/part-00.tzst"))
        ).toBeDefined();

        const withSha = (sha256: unknown): string =>
            JSON.stringify({
                ...manifest,
                shards: [{ name: "part-00.tzst", bytes: 1, files: 1, sha256 }]
            });
        expect(parseShardManifest(withSha(123))).toBeUndefined();
        expect(parseShardManifest(withSha("abc"))).toBeUndefined();
        expect(
            parseShardManifest(withSha(sha256Of("x").toUpperCase()))
        ).toBeUndefined();
        expect(parseShardManifest(withSha(sha256Of("x")))).toBeDefined();
    });

    test("rejects malformed shard entries (bad names, missing sizes)", () => {
        expect(
            parseShardManifest(JSON.stringify({ ...manifest, shards: [] }))
        ).toBeUndefined();
        expect(
            parseShardManifest(
                JSON.stringify({
                    ...manifest,
                    shards: [{ name: "../evil.tzst", bytes: 1, files: 1 }]
                })
            )
        ).toBeUndefined();
        expect(
            parseShardManifest(
                JSON.stringify({
                    ...manifest,
                    shards: [{ name: "part-00.tzst", files: 1 }]
                })
            )
        ).toBeUndefined();
    });
});

describe("shard object naming", () => {
    test("part keys are recognized and never picked as an entry", () => {
        expect(
            isShardPartObjectKey("cache/o/r/v/key.shards/part-00.tzst")
        ).toBe(true);
        expect(
            isShardPartObjectKey(
                "cache/o/r/v/key.shards/20260915T101112123Z-0badf00d/part-00.tzst"
            )
        ).toBe(true);
        expect(isShardPartObjectKey("cache/o/r/v/key")).toBe(false);
        expect(isShardPartObjectKey("cache/o/r/v/key.shards")).toBe(false);
    });

    test("generation ids are unique, sortable timestamps and key-safe", () => {
        const fixed = new Date("2026-09-15T10:11:12.123Z");
        const a = newShardGeneration(fixed);
        const b = newShardGeneration(fixed);
        expect(a).toMatch(/^20260915T101112123Z-[0-9a-f]{8}$/);
        expect(b).toMatch(/^20260915T101112123Z-[0-9a-f]{8}$/);
        expect(a).not.toBe(b);
        expect(newShardGeneration()).toMatch(/^\d{8}T\d{9}Z-[0-9a-f]{8}$/);
    });

    test("generation-scoped part keys nest under the entry's .shards/ prefix", () => {
        expect(shardsKeyPrefix("my-key")).toBe("my-key.shards/");
        expect(shardGenerationKeyPrefix("my-key", "g1")).toBe(
            "my-key.shards/g1/"
        );
        expect(shardPartKey("my-key", "g1", 0)).toBe(
            "my-key.shards/g1/part-00.tzst"
        );
        expect(shardPartKey("my-key", "g1", 12)).toBe(
            "my-key.shards/g1/part-12.tzst"
        );
        expect(
            isShardPartObjectKey(`p/${shardPartKey("my-key", "g1", 0)}`)
        ).toBe(true);
    });

    test("resolves part locations from the manifest key, or the legacy name", () => {
        const location = "s3://bucket/cache/o/r/v/my-key";
        // Modern manifest: exactly the recorded key under the S3 prefix.
        expect(
            resolveShardPartLocation(location, "my-key", {
                name: "part-00.tzst",
                key: "my-key.shards/g1/part-00.tzst",
                bytes: 1,
                files: 1
            })
        ).toBe("s3://bucket/cache/o/r/v/my-key.shards/g1/part-00.tzst");
        // Entry keys may contain slashes; the key is still relative to the prefix.
        expect(
            resolveShardPartLocation("s3://bucket/cache/o/r/v/a/b", "a/b", {
                name: "part-01.tzst",
                key: "a/b.shards/g1/part-01.tzst",
                bytes: 1,
                files: 1
            })
        ).toBe("s3://bucket/cache/o/r/v/a/b.shards/g1/part-01.tzst");
        // Legacy manifest (no key): the pre-generation naming.
        expect(
            resolveShardPartLocation(location, "my-key", {
                name: "part-02.tzst",
                bytes: 1,
                files: 1
            })
        ).toBe("s3://bucket/cache/o/r/v/my-key.shards/part-02.tzst");
        // A key that is not under this entry's own .shards/ prefix is refused.
        expect(() =>
            resolveShardPartLocation(location, "my-key", {
                name: "part-00.tzst",
                key: "other-key.shards/g1/part-00.tzst",
                bytes: 1,
                files: 1
            })
        ).toThrow(/outside the entry/);
        expect(() =>
            resolveShardPartLocation(location, "wrong-key", {
                name: "part-00.tzst",
                key: "wrong-key.shards/g1/part-00.tzst",
                bytes: 1,
                files: 1
            })
        ).toThrow(/does not end with the entry key/);
    });

    test("hashFileSha256 streams the file and matches node:crypto", async () => {
        const dir = fs.mkdtempSync(path.join(os.tmpdir(), "shard-hash-"));
        try {
            const body = Buffer.alloc(3 * 1024 * 1024 + 17, 0xab);
            const file = path.join(dir, "part-00.tzst");
            fs.writeFileSync(file, body);
            await expect(hashFileSha256(file)).resolves.toBe(sha256Of(body));
        } finally {
            fs.rmSync(dir, { recursive: true, force: true });
        }
    });

    test("part and list names are zero-padded and stable", () => {
        expect(shardPartName(0)).toBe("part-00.tzst");
        expect(shardPartName(7)).toBe("part-07.tzst");
        expect(shardPartName(63)).toBe("part-63.tzst");
        expect(shardListName(3)).toBe("shard-03.txt");
    });

    test("paths with a newline are rejected with a clear error", () => {
        expect(() => assertShardablePaths(["ok", "also/ok"])).not.toThrow();
        expect(() => assertShardablePaths(["bad\nname"])).toThrow(/newline/);
        expect(() => assertShardablePaths(["bad\rname"])).toThrow(/newline/);
    });
});

describe("mapWithConcurrency", () => {
    test("runs at most `limit` workers at once and preserves order", async () => {
        let inFlight = 0;
        let peak = 0;
        const results = await mapWithConcurrency(
            [1, 2, 3, 4, 5],
            2,
            async value => {
                inFlight++;
                peak = Math.max(peak, inFlight);
                await new Promise(resolve => setTimeout(resolve, 5));
                inFlight--;
                return value * 10;
            }
        );
        expect(results).toEqual([10, 20, 30, 40, 50]);
        expect(peak).toBe(2);
    });

    test("rejects with the first failure", async () => {
        await expect(
            mapWithConcurrency([1, 2, 3], 2, async value => {
                if (value === 2) throw new Error("boom");
                return value;
            })
        ).rejects.toThrow("boom");
    });
});

// ============================================================================
// Enumeration + orchestration against a real temp tree (tar/zstd injected).
// ============================================================================
describe("enumerateCacheEntries", () => {
    let workspace: string;

    beforeAll(() => {
        workspace = fs.mkdtempSync(path.join(os.tmpdir(), "shards-ws-"));
        fs.mkdirSync(path.join(workspace, "Library", "sub"), {
            recursive: true
        });
        fs.mkdirSync(path.join(workspace, "Library", "empty"), {
            recursive: true
        });
        fs.writeFileSync(
            path.join(workspace, "Library", "big.bin"),
            Buffer.alloc(300)
        );
        fs.writeFileSync(
            path.join(workspace, "Library", "sub", "small.txt"),
            "12345"
        );
        fs.writeFileSync(path.join(workspace, "loose.txt"), "ab");
    });

    afterAll(() => {
        fs.rmSync(workspace, { recursive: true, force: true });
    });

    test("lists regular files with sizes and empty dirs as forward-slash relative paths", () => {
        const { files, emptyDirs } = enumerateCacheEntries(
            ["Library", "loose.txt", "does-not-exist"],
            workspace
        );
        expect(
            files.slice().sort((a, b) => (a.relPath < b.relPath ? -1 : 1))
        ).toEqual([
            { relPath: "Library/big.bin", size: 300 },
            { relPath: "Library/sub/small.txt", size: 5 },
            { relPath: "loose.txt", size: 2 }
        ]);
        expect(emptyDirs).toEqual(["Library/empty"]);
        for (const file of files) {
            expect(file.relPath).not.toContain("\\");
            expect(path.isAbsolute(file.relPath)).toBe(false);
        }
    });

    test("does not list the same file twice for overlapping inputs", () => {
        const { files } = enumerateCacheEntries(
            ["Library", "Library/big.bin"],
            workspace
        );
        expect(
            files.filter(file => file.relPath === "Library/big.bin")
        ).toHaveLength(1);
    });
});

describe("createShardedArchive / extractShardedArchive (injected tar)", () => {
    let workspace: string;
    let archiveFolder: string;
    const originalWorkspace = process.env.GITHUB_WORKSPACE;
    const originalShards = process.env[ENV_ARCHIVE_SHARDS];

    beforeEach(() => {
        workspace = fs.mkdtempSync(path.join(os.tmpdir(), "shards-ws-"));
        archiveFolder = fs.mkdtempSync(path.join(os.tmpdir(), "shards-stage-"));
        fs.mkdirSync(path.join(workspace, "Library", "sub"), {
            recursive: true
        });
        fs.mkdirSync(path.join(workspace, "Library", "empty"), {
            recursive: true
        });
        fs.writeFileSync(
            path.join(workspace, "Library", "a.bin"),
            Buffer.alloc(400)
        );
        fs.writeFileSync(
            path.join(workspace, "Library", "b.bin"),
            Buffer.alloc(300)
        );
        fs.writeFileSync(
            path.join(workspace, "Library", "sub", "c.bin"),
            Buffer.alloc(200)
        );
        fs.writeFileSync(
            path.join(workspace, "Library", "sub", "d.bin"),
            Buffer.alloc(100)
        );
        process.env.GITHUB_WORKSPACE = workspace;
    });

    afterEach(() => {
        if (originalWorkspace === undefined) {
            delete process.env.GITHUB_WORKSPACE;
        } else {
            process.env.GITHUB_WORKSPACE = originalWorkspace;
        }
        if (originalShards === undefined) {
            delete process.env[ENV_ARCHIVE_SHARDS];
        } else {
            process.env[ENV_ARCHIVE_SHARDS] = originalShards;
        }
        fs.rmSync(workspace, { recursive: true, force: true });
        fs.rmSync(archiveFolder, { recursive: true, force: true });
    });

    const makeDeps = (
        createImpl?: ShardedArchiveDeps["createTarFromFileList"],
        extractImpl?: ShardedArchiveDeps["extractTar"]
    ): ShardedArchiveDeps => ({
        createTarFromFileList: jest.fn(
            createImpl ??
                (async (folder, listName, archiveName) => {
                    // Fake "tar": write a part whose size is the list length.
                    const list = fs.readFileSync(
                        path.join(folder, listName),
                        "utf8"
                    );
                    fs.writeFileSync(
                        path.join(folder, archiveName),
                        Buffer.alloc(list.length)
                    );
                })
        ) as ShardedArchiveDeps["createTarFromFileList"],
        extractTar: jest.fn(
            extractImpl ?? (async () => undefined)
        ) as ShardedArchiveDeps["extractTar"],
        cpuCount: () => 8
    });

    test("writes balanced file lists, runs every shard, and builds the manifest", async () => {
        const deps = makeDeps();
        const { manifest, parts } = await createShardedArchive(
            archiveFolder,
            ["Library"],
            2,
            "my-key",
            deps,
            "gen-a"
        );

        expect(deps.createTarFromFileList).toHaveBeenCalledTimes(2);
        // Per-shard zstd threads: 8 cores / 2 shards = -T4 (from the -T0 default).
        expect(deps.createTarFromFileList).toHaveBeenCalledWith(
            archiveFolder,
            "shard-00.txt",
            "part-00.tzst",
            "zstd -T4 -3 --long=30"
        );

        const list0 = fs
            .readFileSync(path.join(archiveFolder, "shard-00.txt"), "utf8")
            .trim()
            .split("\n");
        const list1 = fs
            .readFileSync(path.join(archiveFolder, "shard-01.txt"), "utf8")
            .trim()
            .split("\n");
        // 400 -> s0, 300 -> s1, 200 -> s1 (500), 100 -> s0 (500); empty dir -> s0.
        expect(list0).toEqual([
            "Library/a.bin",
            "Library/sub/d.bin",
            "Library/empty"
        ]);
        expect(list1).toEqual(["Library/b.bin", "Library/sub/c.bin"]);

        expect(parts.map(part => part.name)).toEqual([
            "part-00.tzst",
            "part-01.tzst"
        ]);
        expect(parts.map(part => part.files)).toEqual([2, 2]);
        expect(parts.map(part => part.key)).toEqual([
            "my-key.shards/gen-a/part-00.tzst",
            "my-key.shards/gen-a/part-01.tzst"
        ]);
        for (const part of parts) {
            expect(fs.existsSync(part.path)).toBe(true);
            expect(part.bytes).toBe(fs.statSync(part.path).size);
            expect(part.bytes).toBeGreaterThan(0);
            // The digest is of the part file as it sits on disk after tar.
            expect(part.sha256).toBe(sha256Of(fs.readFileSync(part.path)));
        }

        expect(manifest.format).toBe(SHARDED_ARCHIVE_FORMAT);
        expect(manifest.generation).toBe("gen-a");
        expect(manifest.shards).toEqual(
            parts.map(part => ({
                name: part.name,
                key: part.key,
                bytes: part.bytes,
                files: part.files,
                sha256: part.sha256
            }))
        );
        for (const shard of manifest.shards) {
            expect(shard.key).toBe(`my-key.shards/gen-a/${shard.name}`);
            expect(shard.sha256).toMatch(/^[0-9a-f]{64}$/);
        }
        expect(manifest.totalBytes).toBe(parts[0].bytes + parts[1].bytes);
        expect(manifest.totalFiles).toBe(4);
        expect(Date.parse(manifest.createdAt)).not.toBeNaN();
        // The manifest must round-trip through the detector used on restore.
        expect(parseShardManifest(JSON.stringify(manifest))).toEqual(manifest);
    });

    test("uses fewer shards than requested when there are fewer files", async () => {
        const deps = makeDeps();
        const { manifest } = await createShardedArchive(
            archiveFolder,
            ["Library/a.bin"],
            8,
            "my-key",
            deps
        );
        expect(deps.createTarFromFileList).toHaveBeenCalledTimes(1);
        expect(manifest.shards).toHaveLength(1);
    });

    test("mints a fresh generation per save so two saves never share part keys", async () => {
        const first = await createShardedArchive(
            archiveFolder,
            ["Library"],
            2,
            "my-key",
            makeDeps()
        );
        const second = await createShardedArchive(
            archiveFolder,
            ["Library"],
            2,
            "my-key",
            makeDeps()
        );
        expect(first.manifest.generation).toMatch(/^\d{8}T\d{9}Z-[0-9a-f]{8}$/);
        expect(second.manifest.generation).not.toBe(first.manifest.generation);
        const firstKeys = new Set(
            first.manifest.shards.map(shard => shard.key)
        );
        for (const shard of second.manifest.shards) {
            expect(firstKeys.has(shard.key)).toBe(false);
            expect(shard.key).toBe(
                `my-key.shards/${second.manifest.generation}/${shard.name}`
            );
        }
        await expect(
            createShardedArchive(
                archiveFolder,
                ["Library"],
                2,
                "my-key",
                makeDeps(),
                "bad/gen"
            )
        ).rejects.toThrow(/Invalid shard generation/);
    });

    test("fails the save when any shard's tar fails, after all shards settle", async () => {
        let started = 0;
        const deps = makeDeps(async (folder, _listName, archiveName) => {
            started++;
            if (archiveName === "part-01.tzst") {
                throw new Error("zstd: not found");
            }
            fs.writeFileSync(path.join(folder, archiveName), "x");
        });
        await expect(
            createShardedArchive(archiveFolder, ["Library"], 2, "my-key", deps)
        ).rejects.toThrow(/shard 1 tar failed: zstd: not found/);
        expect(started).toBe(2);
    });

    test("rejects when the cache paths hold no files at all", async () => {
        await expect(
            createShardedArchive(
                archiveFolder,
                ["missing"],
                2,
                "my-key",
                makeDeps()
            )
        ).rejects.toThrow(/no files found/);
    });

    test("extracts every part concurrently and aggregates failures", async () => {
        const okDeps = makeDeps();
        await extractShardedArchive(
            ["/s/part-00.tzst", "/s/part-01.tzst"],
            okDeps
        );
        expect(okDeps.extractTar).toHaveBeenCalledTimes(2);
        expect(okDeps.extractTar).toHaveBeenCalledWith("/s/part-00.tzst");
        expect(okDeps.extractTar).toHaveBeenCalledWith("/s/part-01.tzst");

        const badDeps = makeDeps(undefined, async partPath => {
            if (partPath.endsWith("part-01.tzst")) {
                throw new Error("Unexpected EOF in archive");
            }
        });
        await expect(
            extractShardedArchive(
                ["/s/part-00.tzst", "/s/part-01.tzst"],
                badDeps
            )
        ).rejects.toThrow(/part part-01.tzst extract failed: Unexpected EOF/);
        expect(badDeps.extractTar).toHaveBeenCalledTimes(2);
    });
});
