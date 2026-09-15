import {
    DeleteObjectsCommand,
    GetObjectCommand,
    HeadObjectCommand,
    ListObjectsV2Command,
    S3Client
} from "@aws-sdk/client-s3";
import {
    afterAll,
    afterEach,
    beforeAll,
    describe,
    expect,
    jest,
    test
} from "@jest/globals";
import { createHash } from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Readable } from "stream";

import {
    ENV_SHARD_RETENTION_HOURS,
    LEGACY_SHARD_GENERATION,
    parseShardManifest,
    SHARDED_ARCHIVE_FORMAT,
    ShardManifest,
    ShardPart,
    shardPartKey,
    shardPartName
} from "../src/custom/shardedArchive";

// backend.ts reads its S3 configuration at import time, so pin the env before
// the (dynamic) import; the S3Client it builds is real but never hits the
// network because every send() below is intercepted on the prototype.
const originalEnv = {
    bucket: process.env.RUNS_ON_S3_BUCKET_CACHE,
    region: process.env.RUNS_ON_AWS_REGION,
    repository: process.env.GITHUB_REPOSITORY
};
process.env.RUNS_ON_S3_BUCKET_CACHE = "test-bucket";
process.env.RUNS_ON_AWS_REGION = "us-east-1";
process.env.GITHUB_REPOSITORY = "owner/repo";

const backend = await import("../src/custom/backend");

type SendMock = jest.SpiedFunction<S3Client["send"]>;
let send: SendMock;

beforeAll(() => {
    send = jest.spyOn(S3Client.prototype, "send") as unknown as SendMock;
});

afterEach(() => {
    send.mockReset();
});

afterAll(() => {
    send.mockRestore();
    process.env.RUNS_ON_S3_BUCKET_CACHE = originalEnv.bucket;
    process.env.RUNS_ON_AWS_REGION = originalEnv.region;
    process.env.GITHUB_REPOSITORY = originalEnv.repository;
});

const at = (iso: string): Date => new Date(iso);

// ============================================================================
// Entry lookup ignores shard part objects when picking the newest key.
// ============================================================================
describe("selectNewestArchiveObject", () => {
    test("picks the most recently modified non-part object", () => {
        expect(
            backend.selectNewestArchiveObject([
                { Key: "p/key-old", LastModified: at("2026-01-01T00:00:00Z") },
                { Key: "p/key-new", LastModified: at("2026-02-01T00:00:00Z") },
                { Key: "p/key-mid", LastModified: at("2026-01-15T00:00:00Z") }
            ])
        ).toBe("p/key-new");
    });

    test("never returns a `.shards/` part even when it is the newest object", () => {
        expect(
            backend.selectNewestArchiveObject([
                { Key: "p/key", LastModified: at("2026-01-01T00:00:00Z") },
                {
                    Key: "p/key.shards/part-00.tzst",
                    LastModified: at("2026-03-01T00:00:00Z")
                },
                {
                    Key: "p/key.shards/part-01.tzst",
                    LastModified: at("2026-03-01T00:00:01Z")
                }
            ])
        ).toBe("p/key");
    });

    test("never returns a generation-scoped part (`.shards/<gen>/part-NN`) either", () => {
        expect(
            backend.selectNewestArchiveObject([
                { Key: "p/key", LastModified: at("2026-01-01T00:00:00Z") },
                {
                    Key: "p/key.shards/20260915T101112123Z-0badf00d/part-00.tzst",
                    LastModified: at("2026-03-01T00:00:00Z")
                },
                {
                    Key: "p/key.shards/20260915T101112123Z-0badf00d/part-01.tzst",
                    LastModified: at("2026-03-01T00:00:01Z")
                },
                // Orphan of an interrupted later save: still never an entry.
                {
                    Key: "p/key.shards/20260916T000000000Z-deadbeef/part-00.tzst",
                    LastModified: at("2026-03-02T00:00:00Z")
                }
            ])
        ).toBe("p/key");
    });

    test("returns undefined when only orphan parts exist (interrupted save)", () => {
        expect(
            backend.selectNewestArchiveObject([
                {
                    Key: "p/key.shards/part-00.tzst",
                    LastModified: at("2026-03-01T00:00:00Z")
                }
            ])
        ).toBeUndefined();
        expect(backend.selectNewestArchiveObject([])).toBeUndefined();
    });
});

describe("getCacheEntry", () => {
    const paths = ["Library"];
    const versionOptions = {
        compressionMethod: "zstd",
        enableCrossOsArchive: false
    };

    test("ignores `.shards/` objects and returns the manifest key as the hit", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                const prefix = command.input.Prefix as string;
                return {
                    Contents: [
                        {
                            Key: `${prefix}-abc`,
                            LastModified: at("2026-01-01T00:00:00Z")
                        },
                        {
                            Key: `${prefix}-abc.shards/gen-a/part-00.tzst`,
                            LastModified: at("2026-01-02T00:00:00Z")
                        },
                        {
                            Key: `${prefix}-abc.shards/gen-a/part-01.tzst`,
                            LastModified: at("2026-01-02T00:00:01Z")
                        },
                        {
                            Key: `${prefix}-abc.shards/part-00.tzst`,
                            LastModified: at("2026-01-02T00:00:02Z")
                        }
                    ]
                };
            }
            throw new Error(`unexpected command ${String(command)}`);
        });

        const entry = await backend.getCacheEntry(
            ["lib-"],
            paths,
            versionOptions
        );
        expect(entry.cacheKey).toBe("lib--abc");
        expect(entry.archiveLocation).toMatch(
            /^s3:\/\/test-bucket\/cache\/owner\/repo\/[0-9a-f]{64}\/lib--abc$/
        );
        expect(entry.archiveLocation).not.toContain(".shards/");
    });

    test("falls through to the next restore key when a prefix lists only parts", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                const prefix = command.input.Prefix as string;
                if (prefix.endsWith("/primary")) {
                    return {
                        Contents: [
                            {
                                Key: `${prefix}.shards/gen-a/part-00.tzst`,
                                LastModified: at("2026-01-02T00:00:00Z")
                            }
                        ]
                    };
                }
                return {
                    Contents: [
                        {
                            Key: `${prefix}-x`,
                            LastModified: at("2025-12-01T00:00:00Z")
                        }
                    ]
                };
            }
            throw new Error("unexpected command");
        });

        const entry = await backend.getCacheEntry(
            ["primary", "fallback"],
            paths,
            versionOptions
        );
        expect(entry.cacheKey).toBe("fallback-x");
        expect(send).toHaveBeenCalledTimes(2);
    });
});

// ============================================================================
// Manifest probe: HEAD, then GET only a small body, then parse.
// ============================================================================
describe("getShardManifest", () => {
    const location = "s3://test-bucket/cache/owner/repo/abc/my-key";
    const manifest = {
        format: SHARDED_ARCHIVE_FORMAT,
        shards: [{ name: "part-00.tzst", bytes: 123, files: 4 }],
        totalBytes: 123,
        totalFiles: 4,
        createdAt: "2026-01-01T00:00:00.000Z"
    };

    test("returns a generation manifest with per-part keys and digests intact", async () => {
        const digest = createHash("sha256").update("part").digest("hex");
        const modern = {
            format: SHARDED_ARCHIVE_FORMAT,
            generation: "20260915T101112123Z-0badf00d",
            shards: [
                {
                    name: "part-00.tzst",
                    key: "my-key.shards/20260915T101112123Z-0badf00d/part-00.tzst",
                    bytes: 123,
                    files: 4,
                    sha256: digest
                }
            ],
            totalBytes: 123,
            totalFiles: 4,
            createdAt: "2026-01-01T00:00:00.000Z"
        };
        const body = JSON.stringify(modern);
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                return { ContentLength: body.length };
            }
            if (command instanceof GetObjectCommand) {
                return { Body: Readable.from([Buffer.from(body, "utf8")]) };
            }
            throw new Error("unexpected command");
        });

        await expect(backend.getShardManifest(location)).resolves.toEqual(
            modern
        );
    });

    test("returns the manifest for a small JSON object with the sharded format", async () => {
        const body = JSON.stringify(manifest);
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                expect(command.input.Key).toBe("cache/owner/repo/abc/my-key");
                return { ContentLength: body.length };
            }
            if (command instanceof GetObjectCommand) {
                return { Body: Readable.from([Buffer.from(body, "utf8")]) };
            }
            throw new Error("unexpected command");
        });

        await expect(backend.getShardManifest(location)).resolves.toEqual(
            manifest
        );
    });

    test("treats a large object as a legacy archive without fetching it", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                return { ContentLength: 60 * 1024 * 1024 * 1024 };
            }
            throw new Error("GetObject must not be issued for a large object");
        });

        await expect(
            backend.getShardManifest(location)
        ).resolves.toBeUndefined();
        expect(send).toHaveBeenCalledTimes(1);
    });

    test("treats a small binary object as a legacy archive", async () => {
        const zstdBytes = Buffer.from([
            0x28, 0xb5, 0x2f, 0xfd, 0x24, 0x00, 0x01, 0x00
        ]);
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                return { ContentLength: zstdBytes.length };
            }
            if (command instanceof GetObjectCommand) {
                return { Body: Readable.from([zstdBytes]) };
            }
            throw new Error("unexpected command");
        });

        await expect(
            backend.getShardManifest(location)
        ).resolves.toBeUndefined();
    });

    test("treats a HEAD failure as legacy so the download path reports it", async () => {
        send.mockImplementation(async () => {
            throw new Error("AccessDenied");
        });
        await expect(
            backend.getShardManifest(location)
        ).resolves.toBeUndefined();
    });
});

// ============================================================================
// Sharded save against an in-memory bucket: immutable generations with
// deferred, protected cleanup of superseded ones.
// ============================================================================
describe("saveShardedCache (in-memory bucket)", () => {
    const key = "lib-abc";
    const paths = ["Library"];
    const saveOptions = {
        compressionMethod: "zstd" as never,
        enableCrossOsArchive: false
    };
    const s3Prefix = `cache/owner/repo/${backend.getCacheVersion(
        paths,
        "zstd" as never,
        false
    )}`;
    const s3Key = `${s3Prefix}/${key}`;
    const HOUR = 3600000;
    const T0 = new Date("2026-09-15T00:00:00Z");

    let staging: string;
    const originalRetention = process.env[ENV_SHARD_RETENTION_HOURS];
    beforeAll(() => {
        staging = fs.mkdtempSync(path.join(os.tmpdir(), "shards-save-"));
    });
    afterEach(() => {
        if (originalRetention === undefined) {
            delete process.env[ENV_SHARD_RETENTION_HOURS];
        } else {
            process.env[ENV_SHARD_RETENTION_HOURS] = originalRetention;
        }
    });
    afterAll(() => {
        fs.rmSync(staging, { recursive: true, force: true });
    });

    interface FakeObject {
        body: Buffer;
        lastModified: Date;
    }

    /** Fake S3: object key -> body + LastModified, on a settable clock.
     *  Records the order of every operation. */
    class FakeBucket {
        readonly objects = new Map<string, FakeObject>();
        readonly log: string[] = [];
        clock = T0;

        /** Move the clock forward by `hours`. */
        advance(hours: number): void {
            this.clock = new Date(this.clock.getTime() + hours * HOUR);
        }

        put(objectKey: string, body: Buffer | string, at?: Date): void {
            this.objects.set(objectKey, {
                body: Buffer.isBuffer(body) ? body : Buffer.from(body, "utf8"),
                lastModified: at ?? this.clock
            });
        }

        deps(hooks?: {
            beforeUpload?: (s3Key: string) => Promise<void> | void;
        }): backend.ShardedSaveDeps {
            return {
                readEntryObject: async objectKey => {
                    this.log.push(`head ${objectKey}`);
                    const object = this.objects.get(objectKey);
                    return object === undefined
                        ? undefined
                        : {
                              body: object.body,
                              lastModified: object.lastModified
                          };
                },
                uploadObject: async (objectKey, archivePath) => {
                    await hooks?.beforeUpload?.(objectKey);
                    this.put(objectKey, fs.readFileSync(archivePath));
                    this.log.push(`put ${objectKey}`);
                },
                putJsonObject: async (objectKey, body) => {
                    this.put(objectKey, body);
                    this.log.push(`put ${objectKey}`);
                },
                listObjects: async prefix => {
                    this.log.push(`list ${prefix}`);
                    return [...this.objects.entries()]
                        .filter(([objectKey]) => objectKey.startsWith(prefix))
                        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
                        .map(([objectKey, object]) => ({
                            key: objectKey,
                            lastModified: object.lastModified
                        }));
                },
                deleteObjectKeys: async keys => {
                    for (const objectKey of keys) {
                        this.objects.delete(objectKey);
                        this.log.push(`delete ${objectKey}`);
                    }
                },
                now: () => this.clock
            };
        }

        manifest(): ShardManifest | undefined {
            const object = this.objects.get(s3Key);
            return object === undefined
                ? undefined
                : parseShardManifest(object.body);
        }

        shardKeys(): string[] {
            return [...this.objects.keys()]
                .filter(objectKey => objectKey.startsWith(`${s3Key}.shards/`))
                .sort();
        }

        deletes(): string[] {
            return this.log
                .filter(line => line.startsWith("delete "))
                .map(line => line.slice("delete ".length))
                .sort();
        }

        /** Every object key a manifest references exists with matching bytes. */
        expectManifestComplete(manifest: ShardManifest | undefined): void {
            expect(manifest).toBeDefined();
            for (const shard of manifest!.shards) {
                const object = this.objects.get(`${s3Prefix}/${shard.key}`);
                expect(object).toBeDefined();
                expect(object!.body.length).toBe(shard.bytes);
                expect(
                    createHash("sha256").update(object!.body).digest("hex")
                ).toBe(shard.sha256);
            }
        }
    }

    /** Build a generation's parts on disk with distinct content per part. */
    function makeGeneration(
        generation: string,
        partCount: number
    ): { parts: ShardPart[]; manifest: ShardManifest } {
        const dir = path.join(staging, generation);
        fs.mkdirSync(dir, { recursive: true });
        const parts: ShardPart[] = [];
        for (let index = 0; index < partCount; index++) {
            // Same size in every generation so a size-only check can't tell
            // generations apart — exactly the case the digest exists for.
            const body = Buffer.alloc(64, `${generation}:${index}`);
            const partPath = path.join(dir, shardPartName(index));
            fs.writeFileSync(partPath, body);
            parts.push({
                index,
                name: shardPartName(index),
                key: shardPartKey(key, generation, index),
                path: partPath,
                bytes: body.length,
                files: 3,
                sha256: createHash("sha256").update(body).digest("hex")
            });
        }
        return {
            parts,
            manifest: {
                format: SHARDED_ARCHIVE_FORMAT,
                generation,
                shards: parts.map(part => ({
                    name: part.name,
                    key: part.key,
                    bytes: part.bytes,
                    files: part.files,
                    sha256: part.sha256
                })),
                totalBytes: parts.reduce((sum, part) => sum + part.bytes, 0),
                totalFiles: parts.length * 3,
                createdAt: "2026-01-01T00:00:00.000Z"
            }
        };
    }

    async function save(
        bucket: FakeBucket,
        generation: string,
        partCount = 2,
        hooks?: Parameters<FakeBucket["deps"]>[0]
    ): Promise<ShardManifest> {
        const { parts, manifest } = makeGeneration(generation, partCount);
        await backend.saveShardedCache(
            key,
            paths,
            parts,
            manifest,
            saveOptions,
            bucket.deps(hooks)
        );
        return manifest;
    }

    test("reads the previous entry first, uploads every part to its manifest key, then the manifest, then retires only old generations", async () => {
        const bucket = new FakeBucket();
        // Left-overs from long ago: a legacy un-generationed part and an
        // orphan of an interrupted save, both far older than the window.
        bucket.put(`${s3Key}.shards/part-00.tzst`, "old", new Date(0));
        bucket.put(
            `${s3Key}.shards/gen-orphan/part-00.tzst`,
            "orphan",
            new Date(0)
        );
        // A young orphan: some other writer may be mid-upload.
        bucket.put(`${s3Key}.shards/gen-young/part-00.tzst`, "young");
        // An unrelated entry that shares the restore-key prefix must survive.
        bucket.put(`${s3Key}-2.shards/gen-x/part-00.tzst`, "x", new Date(0));
        bucket.put(`${s3Key}-2`, "{}", new Date(0));

        const manifest = await save(bucket, "gen-a");

        // Order: inspect the entry -> parts (at the exact keys the manifest
        // records) -> manifest -> listing -> deletes of the old generations.
        expect(bucket.log).toEqual([
            `head ${s3Key}`,
            `put ${s3Prefix}/${key}.shards/gen-a/part-00.tzst`,
            `put ${s3Prefix}/${key}.shards/gen-a/part-01.tzst`,
            `put ${s3Key}`,
            `list ${s3Key}.shards/`,
            `delete ${s3Key}.shards/gen-orphan/part-00.tzst`,
            `delete ${s3Key}.shards/part-00.tzst`
        ]);
        bucket.expectManifestComplete(bucket.manifest());
        expect(bucket.manifest()).toEqual(manifest);
        expect(bucket.shardKeys()).toEqual([
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`,
            `${s3Key}.shards/gen-young/part-00.tzst`
        ]);
        expect(bucket.objects.has(`${s3Key}-2.shards/gen-x/part-00.tzst`)).toBe(
            true
        );
        expect(bucket.objects.has(`${s3Key}-2`)).toBe(true);
    });

    test("race (a): overlapping writers — the finishing writer keeps the other's young part, and both manifests stay complete", async () => {
        const bucket = new FakeBucket();
        // Two old entries exist so that A's cleanup is not held back by the
        // "previous entry is young" rule and has something it may delete
        // (gen-00): only the young-object rule then protects B.
        await save(bucket, "gen-00");
        bucket.advance(1);
        await save(bucket, "gen-0");
        bucket.advance(48);
        bucket.log.length = 0;

        // Writer B starts, uploads part 0, and pauses; writer A then runs to
        // completion (publishes and cleans up) before B resumes.
        let manifestA: ShardManifest | undefined;
        let released = false;
        const manifestB = await save(bucket, "gen-b", 2, {
            beforeUpload: async objectKey => {
                if (objectKey.endsWith("/gen-b/part-01.tzst") && !released) {
                    released = true;
                    manifestA = await save(bucket, "gen-a");
                }
            }
        });

        // A's cleanup ran while B's part 0 was already uploaded: A must not
        // have deleted it (it is young), while gen-00 was old and went
        // (gen-0 is the generation A superseded, so it stays too).
        expect(bucket.deletes()).toEqual([
            `${s3Key}.shards/gen-00/part-00.tzst`,
            `${s3Key}.shards/gen-00/part-01.tzst`
        ]);
        expect(bucket.log).toContain(
            `put ${s3Prefix}/${key}.shards/gen-b/part-00.tzst`
        );

        // B published last: its manifest is the visible one and references
        // only objects that exist; A's manifest is superseded but its objects
        // are all still there for any reader that picked it.
        expect(bucket.manifest()).toEqual(manifestB);
        bucket.expectManifestComplete(manifestB);
        bucket.expectManifestComplete(manifestA);
        expect(bucket.shardKeys()).toEqual([
            `${s3Key}.shards/gen-0/part-00.tzst`,
            `${s3Key}.shards/gen-0/part-01.tzst`,
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`,
            `${s3Key}.shards/gen-b/part-00.tzst`,
            `${s3Key}.shards/gen-b/part-01.tzst`
        ]);
    });

    test("race (b): a reader holding manifest A keeps A's parts while B publishes", async () => {
        const bucket = new FakeBucket();
        const manifestA = await save(bucket, "gen-a");
        // A reader picked manifest A and is downloading its parts...
        const held = bucket.manifest();
        expect(held).toEqual(manifestA);

        // ...while B publishes right away (A still within the window).
        bucket.advance(0.5);
        bucket.log.length = 0;
        const manifestB = await save(bucket, "gen-b");
        expect(bucket.deletes()).toEqual([]);
        expect(bucket.manifest()).toEqual(manifestB);
        bucket.expectManifestComplete(held);

        // Even when A had been current for longer than the window, it is the
        // generation B supersedes and is never deleted in the same save.
        const laterBucket = new FakeBucket();
        const olderA = await save(laterBucket, "gen-a");
        laterBucket.advance(72);
        const laterHeld = laterBucket.manifest();
        laterBucket.log.length = 0;
        await save(laterBucket, "gen-b");
        expect(laterBucket.deletes()).toEqual([]);
        expect(laterHeld).toEqual(olderA);
        laterBucket.expectManifestComplete(laterHeld);
    });

    test("a generation superseded more than the window ago, whose objects are all old, is deleted by the next save", async () => {
        const bucket = new FakeBucket();
        await save(bucket, "gen-a"); // T0
        bucket.advance(1);
        await save(bucket, "gen-b"); // T0 + 1 h: A is previous -> kept
        expect(bucket.deletes()).toEqual([]);

        bucket.advance(25); // B has been current for 25 h
        bucket.log.length = 0;
        const manifestC = await save(bucket, "gen-c");
        // A stopped being current 25 h ago (when B was published) and all of
        // its objects are older than 24 h: gone. B is previous: kept.
        expect(bucket.deletes()).toEqual([
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`
        ]);
        expect(bucket.shardKeys()).toEqual([
            `${s3Key}.shards/gen-b/part-00.tzst`,
            `${s3Key}.shards/gen-b/part-01.tzst`,
            `${s3Key}.shards/gen-c/part-00.tzst`,
            `${s3Key}.shards/gen-c/part-01.tzst`
        ]);
        expect(bucket.manifest()).toEqual(manifestC);
    });

    test("the previous generation is never deleted in the save that supersedes it, and a young previous entry defers everything", async () => {
        const bucket = new FakeBucket();
        bucket.put(
            `${s3Key}.shards/gen-ancient/part-00.tzst`,
            "z",
            new Date(0)
        );
        await save(bucket, "gen-a"); // previous absent: ancient is deleted
        expect(bucket.deletes()).toEqual([
            `${s3Key}.shards/gen-ancient/part-00.tzst`
        ]);

        bucket.put(
            `${s3Key}.shards/gen-ancient2/part-00.tzst`,
            "z",
            new Date(0)
        );
        bucket.advance(2);
        bucket.log.length = 0;
        await save(bucket, "gen-b");
        // A is previous (kept); ancient2 is old but A was published only 2 h
        // ago, so the sweep is deferred wholesale.
        expect(bucket.deletes()).toEqual([]);
        expect(bucket.shardKeys()).toEqual([
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`,
            `${s3Key}.shards/gen-ancient2/part-00.tzst`,
            `${s3Key}.shards/gen-b/part-00.tzst`,
            `${s3Key}.shards/gen-b/part-01.tzst`
        ]);
    });

    test("honors CACHE_SHARD_RETENTION_HOURS for the window", async () => {
        process.env[ENV_SHARD_RETENTION_HOURS] = "2";
        const bucket = new FakeBucket();
        await save(bucket, "gen-a");
        bucket.advance(1);
        await save(bucket, "gen-b");
        bucket.advance(3); // B current for 3 h > 2 h window; A's objects 4 h old
        bucket.log.length = 0;
        await save(bucket, "gen-c");
        expect(bucket.deletes()).toEqual([
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`
        ]);

        // An invalid value falls back to the 24 h default (nothing is old enough).
        process.env[ENV_SHARD_RETENTION_HOURS] = "soon";
        bucket.advance(3);
        bucket.log.length = 0;
        await save(bucket, "gen-d");
        expect(bucket.deletes()).toEqual([]);
    });

    test("legacy un-generationed parts are one pseudo-generation: protected while a legacy manifest is previous, retired by age afterwards", async () => {
        const bucket = new FakeBucket();
        const legacyManifest: ShardManifest = {
            format: SHARDED_ARCHIVE_FORMAT,
            shards: [{ name: "part-00.tzst", bytes: 3, files: 1 }],
            totalBytes: 3,
            totalFiles: 1,
            createdAt: "2026-01-01T00:00:00.000Z"
        };
        bucket.put(`${s3Key}.shards/part-00.tzst`, "old", new Date(0));
        bucket.put(s3Key, JSON.stringify(legacyManifest), new Date(0));

        // The legacy manifest is what this save supersedes: its parts stay.
        await save(bucket, "gen-a");
        expect(bucket.deletes()).toEqual([]);
        expect(bucket.objects.has(`${s3Key}.shards/part-00.tzst`)).toBe(true);
        expect(bucket.log.join("\n")).toContain(`head ${s3Key}`);

        // A day later the legacy parts are neither previous nor young: gone.
        bucket.advance(25);
        bucket.log.length = 0;
        await save(bucket, "gen-b");
        expect(bucket.deletes()).toEqual([`${s3Key}.shards/part-00.tzst`]);
        expect(bucket.shardKeys()).toEqual([
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`,
            `${s3Key}.shards/gen-b/part-00.tzst`,
            `${s3Key}.shards/gen-b/part-01.tzst`
        ]);
    });

    test("a previous single archive gates cleanup by its age but is not a generation", async () => {
        const bucket = new FakeBucket();
        bucket.put(`${s3Key}.shards/gen-old/part-00.tzst`, "o", new Date(0));
        bucket.put(s3Key, Buffer.alloc(2 * 1024 * 1024, 1), new Date(0));
        // The fake hands back the whole body; the real seam would omit it for
        // an object this large, which parses the same way (not a manifest).
        await save(bucket, "gen-a");
        expect(bucket.deletes()).toEqual([
            `${s3Key}.shards/gen-old/part-00.tzst`
        ]);
    });

    test("regression: an interrupted replacement leaves the previous manifest and all of its parts intact, and deletes nothing", async () => {
        const bucket = new FakeBucket();
        const manifestA = await save(bucket, "gen-a");
        const snapshotA = new Map(
            manifestA.shards.map(shard => [
                `${s3Prefix}/${shard.key}`,
                bucket.objects.get(`${s3Prefix}/${shard.key}`)!.body
            ])
        );
        bucket.advance(48);
        bucket.log.length = 0;

        // Generation B: part 0 lands, part 1 dies mid-transfer.
        const generationB = makeGeneration("gen-b", 2);
        await expect(
            backend.saveShardedCache(
                key,
                paths,
                generationB.parts,
                generationB.manifest,
                saveOptions,
                bucket.deps({
                    beforeUpload: objectKey => {
                        if (objectKey.endsWith("/gen-b/part-01.tzst")) {
                            throw new Error("connection reset by peer");
                        }
                    }
                })
            )
        ).rejects.toThrow(/connection reset/);

        // Only B's first part was written; no manifest, no listing, no delete.
        expect(bucket.log).toEqual([
            `head ${s3Key}`,
            `put ${s3Prefix}/${key}.shards/gen-b/part-00.tzst`
        ]);

        // The visible manifest is still A's, byte for byte.
        const visible = bucket.manifest();
        expect(visible).toEqual(manifestA);
        expect(visible?.generation).toBe("gen-a");

        // And every key it references still resolves to A's own bytes — never
        // to B's same-sized part 0 (the pre-generation layout would have
        // overwritten part-00 in place here and passed a size-only check).
        for (const shard of visible!.shards) {
            const objectKey = `${s3Prefix}/${shard.key}`;
            expect(shard.key).toBe(`${key}.shards/gen-a/${shard.name}`);
            const body = bucket.objects.get(objectKey)?.body;
            expect(body).toBeDefined();
            expect(body!.equals(snapshotA.get(objectKey)!)).toBe(true);
            expect(body!.length).toBe(shard.bytes);
            expect(createHash("sha256").update(body!).digest("hex")).toBe(
                shard.sha256
            );
            expect(
                body!.equals(fs.readFileSync(generationB.parts[0].path))
            ).toBe(false);
        }

        // B's orphan exists but a listing never picks it as the entry.
        expect(
            bucket.objects.has(`${s3Prefix}/${key}.shards/gen-b/part-00.tzst`)
        ).toBe(true);
        expect(
            backend.selectNewestArchiveObject(
                [...bucket.objects.keys()].map((objectKey, index) => ({
                    Key: objectKey,
                    LastModified: new Date(2026, 0, 1 + index)
                }))
            )
        ).toBe(s3Key);

        // A complete save C right away replaces A but keeps A (previous) and
        // B's orphan (young); a save D a day later sweeps both.
        bucket.log.length = 0;
        const manifestC = await save(bucket, "gen-c");
        expect(bucket.manifest()).toEqual(manifestC);
        expect(bucket.deletes()).toEqual([]);

        bucket.advance(25);
        bucket.log.length = 0;
        const manifestD = await save(bucket, "gen-d");
        expect(bucket.manifest()).toEqual(manifestD);
        expect(bucket.deletes()).toEqual([
            `${s3Key}.shards/gen-a/part-00.tzst`,
            `${s3Key}.shards/gen-a/part-01.tzst`,
            `${s3Key}.shards/gen-b/part-00.tzst`
        ]);
        expect(bucket.shardKeys()).toEqual([
            `${s3Key}.shards/gen-c/part-00.tzst`,
            `${s3Key}.shards/gen-c/part-01.tzst`,
            `${s3Key}.shards/gen-d/part-00.tzst`,
            `${s3Key}.shards/gen-d/part-01.tzst`
        ]);
    });

    test("a cleanup failure never fails the save (the manifest is already in place)", async () => {
        const bucket = new FakeBucket();
        bucket.put(`${s3Key}.shards/gen-old/part-00.tzst`, "o", new Date(0));
        const deps = bucket.deps();
        deps.listObjects = async () => {
            throw new Error("AccessDenied: s3:ListBucket");
        };
        const { parts, manifest } = makeGeneration("gen-a", 1);
        await expect(
            backend.saveShardedCache(
                key,
                paths,
                parts,
                manifest,
                saveOptions,
                deps
            )
        ).resolves.toBeUndefined();
        expect(bucket.manifest()).toEqual(manifest);
        expect(bucket.objects.has(`${s3Key}.shards/gen-old/part-00.tzst`)).toBe(
            true
        );

        // A failing delete is swallowed the same way.
        const bucket2 = new FakeBucket();
        bucket2.put(`${s3Key}.shards/gen-old/part-00.tzst`, "o", new Date(0));
        const deps2 = bucket2.deps();
        deps2.deleteObjectKeys = async () => {
            throw new Error("AccessDenied: s3:DeleteObject");
        };
        const generationB = makeGeneration("gen-b", 1);
        await expect(
            backend.saveShardedCache(
                key,
                paths,
                generationB.parts,
                generationB.manifest,
                saveOptions,
                deps2
            )
        ).resolves.toBeUndefined();
        expect(bucket2.manifest()).toEqual(generationB.manifest);
    });

    test("an uninspectable previous entry still saves, but defers every deletion", async () => {
        const bucket = new FakeBucket();
        bucket.put(`${s3Key}.shards/gen-old/part-00.tzst`, "o", new Date(0));
        const deps = bucket.deps();
        deps.readEntryObject = async () => {
            throw new Error("AccessDenied: s3:GetObject");
        };
        const { parts, manifest } = makeGeneration("gen-a", 1);
        await backend.saveShardedCache(
            key,
            paths,
            parts,
            manifest,
            saveOptions,
            deps
        );
        expect(bucket.manifest()).toEqual(manifest);
        expect(bucket.deletes()).toEqual([]);
        expect(bucket.objects.has(`${s3Key}.shards/gen-old/part-00.tzst`)).toBe(
            true
        );
    });

    test("refuses a manifest without a generation or a part outside it", async () => {
        const bucket = new FakeBucket();
        const { parts, manifest } = makeGeneration("gen-a", 1);
        await expect(
            backend.saveShardedCache(
                key,
                paths,
                parts,
                { ...manifest, generation: undefined },
                saveOptions,
                bucket.deps()
            )
        ).rejects.toThrow(/no generation/);
        await expect(
            backend.saveShardedCache(
                key,
                paths,
                [{ ...parts[0], key: `${key}.shards/gen-z/part-00.tzst` }],
                manifest,
                saveOptions,
                bucket.deps()
            )
        ).rejects.toThrow(/not under generation gen-a/);
        expect(bucket.objects.size).toBe(0);
        expect(bucket.log).toEqual([]);
    });
});

// ============================================================================
// Previous-entry inspection through the real S3 commands.
// ============================================================================
describe("readPreviousEntryState (S3 commands)", () => {
    const s3Key = "cache/owner/repo/v/lib-abc";
    const stamp = at("2026-09-14T10:00:00Z");

    test("a missing object is `absent`", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                const error = new Error("NotFound");
                error.name = "NotFound";
                (error as { $metadata?: unknown }).$metadata = {
                    httpStatusCode: 404
                };
                throw error;
            }
            throw new Error("unexpected command");
        });
        await expect(backend.readPreviousEntryState(s3Key)).resolves.toEqual({
            kind: "absent"
        });
        expect(send).toHaveBeenCalledTimes(1);
    });

    test("a small manifest is `sharded` with its generation and LastModified, read via HEAD then GET", async () => {
        const body = JSON.stringify({
            format: SHARDED_ARCHIVE_FORMAT,
            generation: "gen-prev",
            shards: [
                {
                    name: "part-00.tzst",
                    key: "lib-abc.shards/gen-prev/part-00.tzst",
                    bytes: 1,
                    files: 1
                }
            ]
        });
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                expect(command.input.Key).toBe(s3Key);
                return { ContentLength: body.length, LastModified: stamp };
            }
            if (command instanceof GetObjectCommand) {
                return {
                    Body: Readable.from([Buffer.from(body, "utf8")]),
                    LastModified: stamp
                };
            }
            throw new Error("unexpected command");
        });
        await expect(backend.readPreviousEntryState(s3Key)).resolves.toEqual({
            kind: "sharded",
            generation: "gen-prev",
            lastModified: stamp
        });
    });

    test("a legacy manifest without a generation maps to the legacy pseudo-generation", async () => {
        const body = JSON.stringify({
            format: SHARDED_ARCHIVE_FORMAT,
            shards: [{ name: "part-00.tzst", bytes: 1, files: 1 }]
        });
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                return { ContentLength: body.length, LastModified: stamp };
            }
            if (command instanceof GetObjectCommand) {
                return { Body: Readable.from([Buffer.from(body, "utf8")]) };
            }
            throw new Error("unexpected command");
        });
        await expect(backend.readPreviousEntryState(s3Key)).resolves.toEqual({
            kind: "sharded",
            generation: LEGACY_SHARD_GENERATION,
            lastModified: stamp
        });
    });

    test("a large object is `single` without being fetched", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof HeadObjectCommand) {
                return {
                    ContentLength: 60 * 1024 * 1024 * 1024,
                    LastModified: stamp
                };
            }
            throw new Error("GetObject must not be issued for a large object");
        });
        await expect(backend.readPreviousEntryState(s3Key)).resolves.toEqual({
            kind: "single",
            lastModified: stamp
        });
        expect(send).toHaveBeenCalledTimes(1);
    });

    test("any other failure is `unknown` (never throws)", async () => {
        send.mockImplementation(async () => {
            throw new Error("AccessDenied");
        });
        await expect(backend.readPreviousEntryState(s3Key)).resolves.toEqual({
            kind: "unknown"
        });
    });
});

// ============================================================================
// Generation cleanup through the real S3 commands (paginated list + batch delete).
// ============================================================================
describe("cleanupOtherShardGenerations (S3 commands)", () => {
    const s3Key = "cache/owner/repo/v/lib-abc";
    const HOUR = 3600000;
    const retention = 24 * HOUR;
    const now = at("2026-09-15T12:00:00Z");
    const hoursAgo = (hours: number): Date =>
        new Date(now.getTime() - hours * HOUR);
    const deps = (): backend.ShardedSaveDeps => ({
        ...backend.defaultShardedSaveDeps,
        now: () => now
    });

    test("lists every page under .shards/ and deletes only generations that are old, not current and not previous", async () => {
        const deleted: string[][] = [];
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                expect(command.input.Bucket).toBe("test-bucket");
                expect(command.input.Prefix).toBe(`${s3Key}.shards/`);
                if (!command.input.ContinuationToken) {
                    return {
                        IsTruncated: true,
                        NextContinuationToken: "page-2",
                        Contents: [
                            {
                                Key: `${s3Key}.shards/gen-old/part-00.tzst`,
                                LastModified: hoursAgo(100)
                            },
                            {
                                Key: `${s3Key}.shards/gen-new/part-00.tzst`,
                                LastModified: hoursAgo(0)
                            },
                            {
                                Key: `${s3Key}.shards/gen-prev/part-00.tzst`,
                                LastModified: hoursAgo(60)
                            }
                        ]
                    };
                }
                expect(command.input.ContinuationToken).toBe("page-2");
                return {
                    IsTruncated: false,
                    Contents: [
                        {
                            Key: `${s3Key}.shards/gen-new/part-01.tzst`,
                            LastModified: hoursAgo(0)
                        },
                        {
                            Key: `${s3Key}.shards/part-00.tzst`,
                            LastModified: hoursAgo(300)
                        },
                        {
                            Key: `${s3Key}.shards/gen-old/part-01.tzst`,
                            LastModified: hoursAgo(99)
                        },
                        {
                            Key: `${s3Key}.shards/gen-inflight/part-00.tzst`,
                            LastModified: hoursAgo(0.2)
                        }
                    ]
                };
            }
            if (command instanceof DeleteObjectsCommand) {
                expect(command.input.Bucket).toBe("test-bucket");
                deleted.push(
                    (command.input.Delete?.Objects ?? []).map(
                        object => object.Key as string
                    )
                );
                return { Deleted: [] };
            }
            throw new Error("unexpected command");
        });

        await expect(
            backend.cleanupOtherShardGenerations(
                s3Key,
                "gen-new",
                {
                    kind: "sharded",
                    generation: "gen-prev",
                    lastModified: hoursAgo(60)
                },
                retention,
                deps()
            )
        ).resolves.toEqual({
            deletedObjects: 3,
            deletedGenerations: 2,
            keptGenerations: 2,
            deferredGenerations: 1
        });
        expect(deleted).toEqual([
            [
                `${s3Key}.shards/gen-old/part-00.tzst`,
                `${s3Key}.shards/gen-old/part-01.tzst`,
                `${s3Key}.shards/part-00.tzst`
            ]
        ]);
    });

    test("batches deletes in groups of 1000", async () => {
        const batches: number[] = [];
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                return {
                    Contents: Array.from({ length: 2005 }, (_, index) => ({
                        Key: `${s3Key}.shards/gen-old/part-${String(
                            index
                        ).padStart(4, "0")}.tzst`,
                        LastModified: hoursAgo(100)
                    }))
                };
            }
            if (command instanceof DeleteObjectsCommand) {
                batches.push(command.input.Delete?.Objects?.length ?? 0);
                return { Deleted: [] };
            }
            throw new Error("unexpected command");
        });
        await expect(
            backend.cleanupOtherShardGenerations(
                s3Key,
                "gen-new",
                { kind: "absent" },
                retention,
                deps()
            )
        ).resolves.toMatchObject({ deletedObjects: 2005 });
        expect(batches).toEqual([1000, 1000, 5]);
    });

    test("issues no delete while the previous entry is within the window", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                return {
                    Contents: [
                        {
                            Key: `${s3Key}.shards/gen-old/part-00.tzst`,
                            LastModified: hoursAgo(100)
                        },
                        {
                            Key: `${s3Key}.shards/gen-new/part-00.tzst`,
                            LastModified: hoursAgo(0)
                        }
                    ]
                };
            }
            throw new Error("DeleteObjects must not be issued");
        });
        await expect(
            backend.cleanupOtherShardGenerations(
                s3Key,
                "gen-new",
                {
                    kind: "sharded",
                    generation: "gen-prev",
                    lastModified: hoursAgo(1)
                },
                retention,
                deps()
            )
        ).resolves.toEqual({
            deletedObjects: 0,
            deletedGenerations: 0,
            keptGenerations: 1,
            deferredGenerations: 1
        });
        expect(send).toHaveBeenCalledTimes(1);
    });

    test("issues no delete when only the current generation exists", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                return {
                    Contents: [
                        {
                            Key: `${s3Key}.shards/gen-new/part-00.tzst`,
                            LastModified: hoursAgo(0)
                        }
                    ]
                };
            }
            throw new Error("DeleteObjects must not be issued");
        });
        await expect(
            backend.cleanupOtherShardGenerations(
                s3Key,
                "gen-new",
                { kind: "absent" },
                retention,
                deps()
            )
        ).resolves.toEqual({
            deletedObjects: 0,
            deletedGenerations: 0,
            keptGenerations: 1,
            deferredGenerations: 0
        });
        expect(send).toHaveBeenCalledTimes(1);
    });

    test("swallows S3 errors (best effort)", async () => {
        send.mockImplementation(async () => {
            throw new Error("AccessDenied");
        });
        await expect(
            backend.cleanupOtherShardGenerations(
                s3Key,
                "gen-new",
                { kind: "absent" },
                retention,
                deps()
            )
        ).resolves.toEqual({
            deletedObjects: 0,
            deletedGenerations: 0,
            keptGenerations: 0,
            deferredGenerations: 0
        });
    });
});
