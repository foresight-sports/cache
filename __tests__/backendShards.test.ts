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
// Sharded save against an in-memory bucket: immutable generations.
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

    let staging: string;
    beforeAll(() => {
        staging = fs.mkdtempSync(path.join(os.tmpdir(), "shards-save-"));
    });
    afterAll(() => {
        fs.rmSync(staging, { recursive: true, force: true });
    });

    /** Fake S3: object key -> body. Records the order of every mutation. */
    class FakeBucket {
        readonly objects = new Map<string, Buffer>();
        readonly log: string[] = [];
        deps(uploadHook?: (s3Key: string) => void): backend.ShardedSaveDeps {
            return {
                uploadObject: async (objectKey, archivePath) => {
                    uploadHook?.(objectKey);
                    this.objects.set(objectKey, fs.readFileSync(archivePath));
                    this.log.push(`put ${objectKey}`);
                },
                putJsonObject: async (objectKey, body) => {
                    this.objects.set(objectKey, Buffer.from(body, "utf8"));
                    this.log.push(`put ${objectKey}`);
                },
                listObjectKeys: async prefix => {
                    this.log.push(`list ${prefix}`);
                    return [...this.objects.keys()]
                        .filter(objectKey => objectKey.startsWith(prefix))
                        .sort();
                },
                deleteObjectKeys: async keys => {
                    for (const objectKey of keys) {
                        this.objects.delete(objectKey);
                        this.log.push(`delete ${objectKey}`);
                    }
                }
            };
        }
        manifest(): ShardManifest | undefined {
            const body = this.objects.get(s3Key);
            return body === undefined ? undefined : parseShardManifest(body);
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

    test("uploads every part to its manifest key, then the manifest, then retires other generations", async () => {
        const bucket = new FakeBucket();
        // Left-overs from before: a legacy un-generationed part and an orphan
        // of an interrupted save.
        bucket.objects.set(`${s3Key}.shards/part-00.tzst`, Buffer.from("old"));
        bucket.objects.set(
            `${s3Key}.shards/gen-orphan/part-00.tzst`,
            Buffer.from("orphan")
        );
        // An unrelated entry that shares the restore-key prefix must survive.
        bucket.objects.set(
            `${s3Key}-2.shards/gen-x/part-00.tzst`,
            Buffer.from("x")
        );
        bucket.objects.set(`${s3Key}-2`, Buffer.from("{}"));

        const { parts, manifest } = makeGeneration("gen-a", 2);
        await backend.saveShardedCache(
            key,
            paths,
            parts,
            manifest,
            saveOptions,
            bucket.deps()
        );

        // Order: parts (at the exact keys the manifest records) -> manifest
        // -> listing -> deletes of everything else under .shards/.
        expect(bucket.log).toEqual([
            `put ${s3Prefix}/${key}.shards/gen-a/part-00.tzst`,
            `put ${s3Prefix}/${key}.shards/gen-a/part-01.tzst`,
            `put ${s3Key}`,
            `list ${s3Key}.shards/`,
            `delete ${s3Key}.shards/gen-orphan/part-00.tzst`,
            `delete ${s3Key}.shards/part-00.tzst`
        ]);
        for (const shard of manifest.shards) {
            expect(bucket.objects.has(`${s3Prefix}/${shard.key}`)).toBe(true);
        }
        expect(bucket.manifest()).toEqual(manifest);
        expect(bucket.objects.has(`${s3Key}-2.shards/gen-x/part-00.tzst`)).toBe(
            true
        );
        expect(bucket.objects.has(`${s3Key}-2`)).toBe(true);
    });

    test("regression: an interrupted replacement leaves the previous manifest and all of its parts intact", async () => {
        const bucket = new FakeBucket();
        const generationA = makeGeneration("gen-a", 2);
        await backend.saveShardedCache(
            key,
            paths,
            generationA.parts,
            generationA.manifest,
            saveOptions,
            bucket.deps()
        );
        const snapshotA = new Map(
            generationA.manifest.shards.map(shard => [
                `${s3Prefix}/${shard.key}`,
                bucket.objects.get(`${s3Prefix}/${shard.key}`)
            ])
        );
        bucket.log.length = 0;

        // Generation B: part 0 lands, part 1 dies mid-transfer.
        const generationB = makeGeneration("gen-b", 2);
        let uploads = 0;
        await expect(
            backend.saveShardedCache(
                key,
                paths,
                generationB.parts,
                generationB.manifest,
                saveOptions,
                bucket.deps(() => {
                    if (++uploads === 2) {
                        throw new Error("connection reset by peer");
                    }
                })
            )
        ).rejects.toThrow(/connection reset/);

        // Only B's first part was written; no manifest, no listing, no delete.
        expect(bucket.log).toEqual([
            `put ${s3Prefix}/${key}.shards/gen-b/part-00.tzst`
        ]);

        // The visible manifest is still A's, byte for byte.
        const visible = bucket.manifest();
        expect(visible).toEqual(generationA.manifest);
        expect(visible?.generation).toBe("gen-a");

        // And every key it references still resolves to A's own bytes — never
        // to B's same-sized part 0 (the pre-generation layout would have
        // overwritten part-00 in place here and passed a size-only check).
        for (const shard of visible!.shards) {
            const objectKey = `${s3Prefix}/${shard.key}`;
            expect(shard.key).toBe(`${key}.shards/gen-a/${shard.name}`);
            const body = bucket.objects.get(objectKey);
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

        // A later complete save (C) replaces A and sweeps A's parts and B's orphan.
        bucket.log.length = 0;
        const generationC = makeGeneration("gen-c", 2);
        await backend.saveShardedCache(
            key,
            paths,
            generationC.parts,
            generationC.manifest,
            saveOptions,
            bucket.deps()
        );
        expect(bucket.manifest()).toEqual(generationC.manifest);
        expect(
            bucket.log.filter(line => line.startsWith("delete")).sort()
        ).toEqual([
            `delete ${s3Key}.shards/gen-a/part-00.tzst`,
            `delete ${s3Key}.shards/gen-a/part-01.tzst`,
            `delete ${s3Key}.shards/gen-b/part-00.tzst`
        ]);
        expect(
            [...bucket.objects.keys()]
                .filter(objectKey => objectKey.startsWith(`${s3Key}.shards/`))
                .sort()
        ).toEqual([
            `${s3Key}.shards/gen-c/part-00.tzst`,
            `${s3Key}.shards/gen-c/part-01.tzst`
        ]);
    });

    test("a cleanup failure never fails the save (the manifest is already in place)", async () => {
        const bucket = new FakeBucket();
        bucket.objects.set(
            `${s3Key}.shards/gen-old/part-00.tzst`,
            Buffer.from("o")
        );
        const deps = bucket.deps();
        deps.listObjectKeys = async () => {
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
    });
});

// ============================================================================
// Generation cleanup through the real S3 commands (paginated list + batch delete).
// ============================================================================
describe("cleanupOtherShardGenerations (S3 commands)", () => {
    const s3Key = "cache/owner/repo/v/lib-abc";

    test("lists every page under .shards/ and deletes only other generations", async () => {
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
                            { Key: `${s3Key}.shards/gen-old/part-00.tzst` },
                            { Key: `${s3Key}.shards/gen-new/part-00.tzst` }
                        ]
                    };
                }
                expect(command.input.ContinuationToken).toBe("page-2");
                return {
                    IsTruncated: false,
                    Contents: [
                        { Key: `${s3Key}.shards/gen-new/part-01.tzst` },
                        { Key: `${s3Key}.shards/part-00.tzst` },
                        { Key: `${s3Key}.shards/gen-old/part-01.tzst` }
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
            backend.cleanupOtherShardGenerations(s3Key, "gen-new")
        ).resolves.toEqual({ deleted: 3, kept: 2 });
        expect(deleted).toEqual([
            [
                `${s3Key}.shards/gen-old/part-00.tzst`,
                `${s3Key}.shards/part-00.tzst`,
                `${s3Key}.shards/gen-old/part-01.tzst`
            ]
        ]);
    });

    test("issues no delete when only the current generation exists", async () => {
        send.mockImplementation(async (command: unknown) => {
            if (command instanceof ListObjectsV2Command) {
                return {
                    Contents: [{ Key: `${s3Key}.shards/gen-new/part-00.tzst` }]
                };
            }
            throw new Error("DeleteObjects must not be issued");
        });
        await expect(
            backend.cleanupOtherShardGenerations(s3Key, "gen-new")
        ).resolves.toEqual({ deleted: 0, kept: 1 });
        expect(send).toHaveBeenCalledTimes(1);
    });

    test("swallows S3 errors (best effort)", async () => {
        send.mockImplementation(async () => {
            throw new Error("AccessDenied");
        });
        await expect(
            backend.cleanupOtherShardGenerations(s3Key, "gen-new")
        ).resolves.toEqual({ deleted: 0, kept: 0 });
    });
});
