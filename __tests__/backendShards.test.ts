import {
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
import { Readable } from "stream";

import { SHARDED_ARCHIVE_FORMAT } from "../src/custom/shardedArchive";

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
                            Key: `${prefix}-abc.shards/part-00.tzst`,
                            LastModified: at("2026-01-02T00:00:00Z")
                        },
                        {
                            Key: `${prefix}-abc.shards/part-01.tzst`,
                            LastModified: at("2026-01-02T00:00:01Z")
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
                                Key: `${prefix}.shards/part-00.tzst`,
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
