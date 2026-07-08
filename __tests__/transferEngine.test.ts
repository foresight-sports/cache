import { beforeEach, describe, expect, jest, test } from "@jest/globals";
import * as os from "os";

import {
    buildAwsCliConfigureArgs,
    buildAwsCliCpArgs,
    buildEngineEnv,
    buildS5cmdArgs,
    computeDownloadConcurrency,
    computePartSizeMb,
    computeTransferConcurrency,
    computeUploadConcurrency,
    resolveRegion,
    transferArchive,
    TransferEngineDeps,
    TransferParams
} from "../src/custom/transferEngine";

// The selection tests inject their own engine runners (no real s5cmd/aws-cli or
// S3), so no module mocking is needed — this suite loads only pure builders +
// the orchestrator. core.info/warning log for real; that noise is harmless.
type AnyMock = ReturnType<typeof jest.fn>;

const baseParams: TransferParams = {
    bucket: "cache-bucket",
    key: "cache/owner/repo/abc123/my-key",
    archivePath: "/tmp/cache.tzst",
    endpoint: "https://s3.example.com",
    region: "us-east-1",
    forcePathStyle: false,
    sizeBytes: 1234,
    partSizeMb: 64
};

const noEnv = {} as NodeJS.ProcessEnv;

describe("upload vs download concurrency (single-file part parallelism)", () => {
    test("upload concurrency scales with cores (CPU/TLS-bound send side)", () => {
        expect(computeUploadConcurrency(1, noEnv)).toBe(96); // floor
        expect(computeUploadConcurrency(16, noEnv)).toBe(128); // 16 * 8
        expect(computeUploadConcurrency(48, noEnv)).toBe(384); // 48*8=384 ceiling
        expect(computeUploadConcurrency(64, noEnv)).toBe(384); // ceiling clamp
        expect(computeUploadConcurrency(0, noEnv)).toBe(96); // degenerate -> floor
    });

    test("download concurrency is DECOUPLED from cores with a high fixed floor", () => {
        // The core fix: a 16-vCPU box no longer gets a starved cores*4 = 64; it
        // gets the 256 floor regardless of vCPU (download is I/O-bound, not
        // CPU-bound), enough to fill a ~5 Gbps NIC at the measured ~3.2 MB/s/conn.
        expect(computeDownloadConcurrency(1, noEnv)).toBe(256); // floor
        expect(computeDownloadConcurrency(16, noEnv)).toBe(256); // floor, NOT 16*anything
        expect(computeDownloadConcurrency(32, noEnv)).toBe(256); // 32*8=256 (still floor)
        expect(computeDownloadConcurrency(64, noEnv)).toBe(512); // big box -> ceiling (64*8)
        expect(computeDownloadConcurrency(1000, noEnv)).toBe(512); // ceiling clamp
    });

    test("download floor is independent of cores (regression guard for 16-vCPU starvation)", () => {
        for (const cores of [1, 2, 4, 8, 12, 16]) {
            expect(
                computeDownloadConcurrency(cores, noEnv)
            ).toBeGreaterThanOrEqual(256);
        }
        // Download is always at least as parallel as upload on the same small box.
        expect(computeDownloadConcurrency(16, noEnv)).toBeGreaterThan(
            computeUploadConcurrency(16, noEnv)
        );
    });

    test("env overrides replace the computed concurrency (clamped to a sanity ceiling)", () => {
        expect(
            computeUploadConcurrency(64, {
                CACHE_UPLOAD_CONCURRENCY: "200"
            } as NodeJS.ProcessEnv)
        ).toBe(200);
        expect(
            computeDownloadConcurrency(16, {
                CACHE_DOWNLOAD_CONCURRENCY: "128"
            } as NodeJS.ProcessEnv)
        ).toBe(128);
        // Absurd overrides are clamped to the hard ceiling (1024) so a typo
        // cannot OOM the runner with in-flight part buffers.
        expect(
            computeDownloadConcurrency(16, {
                CACHE_DOWNLOAD_CONCURRENCY: "99999"
            } as NodeJS.ProcessEnv)
        ).toBe(1024);
        // Non-positive / non-numeric overrides are ignored (fall back to default).
        expect(
            computeDownloadConcurrency(16, {
                CACHE_DOWNLOAD_CONCURRENCY: "0"
            } as NodeJS.ProcessEnv)
        ).toBe(256);
        expect(
            computeUploadConcurrency(16, {
                CACHE_UPLOAD_CONCURRENCY: "abc"
            } as NodeJS.ProcessEnv)
        ).toBe(128);
    });

    test("computeTransferConcurrency dispatches by direction", () => {
        expect(computeTransferConcurrency("upload", 16, noEnv)).toBe(
            computeUploadConcurrency(16, noEnv)
        );
        expect(computeTransferConcurrency("download", 16, noEnv)).toBe(
            computeDownloadConcurrency(16, noEnv)
        );
    });

    test("resolveRegion defaults to auto when unset", () => {
        expect(resolveRegion("us-west-2")).toBe("us-west-2");
        expect(resolveRegion(undefined)).toBe("auto");
        expect(resolveRegion("")).toBe("auto");
    });
});

describe("computePartSizeMb", () => {
    test("upload uses the backend's adaptive part size (params.partSizeMb)", () => {
        expect(
            computePartSizeMb(
                "upload",
                { ...baseParams, partSizeMb: 64 },
                noEnv
            )
        ).toBe(64);
        // Floors at S3's 5 MiB multipart minimum.
        expect(
            computePartSizeMb("upload", { ...baseParams, partSizeMb: 1 }, noEnv)
        ).toBe(5);
        // No part size supplied -> undefined (engine uses its own default).
        expect(
            computePartSizeMb(
                "upload",
                { ...baseParams, partSizeMb: undefined },
                noEnv
            )
        ).toBeUndefined();
    });

    test("download uses a fixed small part size, decoupled from the upload size", () => {
        // Even when params carries an upload-side 64 MiB, download uses its 16 MiB
        // so concurrency*part-size peak buffer stays bounded.
        expect(
            computePartSizeMb(
                "download",
                { ...baseParams, partSizeMb: 64 },
                noEnv
            )
        ).toBe(16);
        expect(
            computePartSizeMb(
                "download",
                { ...baseParams, partSizeMb: undefined },
                noEnv
            )
        ).toBe(16);
    });

    test("download part size is env-overridable and floored at 5 MiB", () => {
        expect(
            computePartSizeMb("download", baseParams, {
                CACHE_DOWNLOAD_PART_SIZE: "32"
            } as NodeJS.ProcessEnv)
        ).toBe(32);
        // A sub-5 MiB override is lifted to the S3 minimum.
        expect(
            computePartSizeMb("download", baseParams, {
                CACHE_DOWNLOAD_PART_SIZE: "2"
            } as NodeJS.ProcessEnv)
        ).toBe(5);
    });

    test("256-way download at 16 MiB keeps the peak in-flight buffer bounded (~4 GiB)", () => {
        const concurrency = computeDownloadConcurrency(16, noEnv); // 256
        const partMb = computePartSizeMb(
            "download",
            baseParams,
            noEnv
        ) as number; // 16
        expect((concurrency * partMb) / 1024).toBeLessThanOrEqual(4); // GiB
    });
});

describe("buildS5cmdArgs", () => {
    test("upload places global flags, then cp, then <local> <s3uri>", () => {
        const args = buildS5cmdArgs("upload", baseParams, 16, noEnv);
        expect(args).toEqual([
            "--numworkers",
            "256", // pinned >= concurrency (single object doesn't use the pool)
            "--stat",
            "--log",
            "error",
            "--endpoint-url",
            "https://s3.example.com",
            "cp",
            "--concurrency",
            "128", // upload cores-scaled: 16*8
            "--part-size",
            "64", // backend's adaptive upload part size
            "/tmp/cache.tzst",
            "s3://cache-bucket/cache/owner/repo/abc123/my-key"
        ]);
    });

    test("download reverses operands and uses the decoupled 256 concurrency + 16 MiB part", () => {
        const args = buildS5cmdArgs("download", baseParams, 16, noEnv);
        const cpIdx = args.indexOf("cp");
        // last two operands, in order
        expect(args.slice(-2)).toEqual([
            "s3://cache-bucket/cache/owner/repo/abc123/my-key",
            "/tmp/cache.tzst"
        ]);
        expect(cpIdx).toBeGreaterThan(-1);
        // A 16-vCPU box downloads at the 256 floor (not the old cores*4 = 64)...
        expect(args[args.indexOf("--concurrency") + 1]).toBe("256");
        // ...with the smaller download part size, and numworkers pinned >= it.
        expect(args[args.indexOf("--part-size") + 1]).toBe("16");
        expect(args[args.indexOf("--numworkers") + 1]).toBe("256");
    });

    test("omits --endpoint-url for real AWS S3; download still emits its own part size", () => {
        const args = buildS5cmdArgs(
            "download",
            { ...baseParams, endpoint: undefined },
            16,
            noEnv
        );
        expect(args).not.toContain("--endpoint-url");
        expect(args).toContain("--numworkers");
        // Download supplies its own part size even when the caller passes none.
        expect(args).toContain("--part-size");
    });

    test("upload omits --part-size when the backend supplies none", () => {
        const args = buildS5cmdArgs(
            "upload",
            { ...baseParams, partSizeMb: undefined },
            16,
            noEnv
        );
        expect(args).not.toContain("--part-size");
    });
});

describe("buildAwsCliConfigureArgs / buildAwsCliCpArgs", () => {
    test("upload configure batches mirror the s5cmd upload concurrency/part size", () => {
        const batches = buildAwsCliConfigureArgs(
            "upload",
            baseParams,
            16,
            noEnv
        );
        const flat = batches.map(b => b.join(" "));
        expect(flat).toContain(
            "configure set default.s3.max_concurrent_requests 128"
        );
        expect(flat).toContain(
            "configure set default.s3.max_queue_size 100000"
        );
        expect(flat).toContain(
            "configure set default.s3.multipart_threshold 64MB"
        );
        expect(flat).toContain(
            "configure set default.s3.multipart_chunksize 64MB"
        );
        // No path-style addressing unless forcePathStyle.
        expect(flat).not.toContain(
            "configure set default.s3.addressing_style path"
        );
    });

    test("download configure batches use the decoupled 256 concurrency + 16 MiB chunk", () => {
        // baseParams carries an upload-side 64 MiB partSizeMb; download ignores it.
        const batches = buildAwsCliConfigureArgs(
            "download",
            baseParams,
            16,
            noEnv
        );
        const flat = batches.map(b => b.join(" "));
        expect(flat).toContain(
            "configure set default.s3.max_concurrent_requests 256"
        );
        expect(flat).toContain(
            "configure set default.s3.multipart_chunksize 16MB"
        );
    });

    test("configure adds path addressing style when forcePathStyle", () => {
        const batches = buildAwsCliConfigureArgs(
            "upload",
            { ...baseParams, forcePathStyle: true },
            16,
            noEnv
        );
        const flat = batches.map(b => b.join(" "));
        expect(flat).toContain(
            "configure set default.s3.addressing_style path"
        );
    });

    test("cp upload/download operand order + endpoint + only-show-errors", () => {
        expect(buildAwsCliCpArgs("upload", baseParams)).toEqual([
            "s3",
            "cp",
            "/tmp/cache.tzst",
            "s3://cache-bucket/cache/owner/repo/abc123/my-key",
            "--endpoint-url",
            "https://s3.example.com",
            "--only-show-errors"
        ]);
        expect(buildAwsCliCpArgs("download", baseParams).slice(2, 4)).toEqual([
            "s3://cache-bucket/cache/owner/repo/abc123/my-key",
            "/tmp/cache.tzst"
        ]);
    });
});

describe("buildEngineEnv scoped aws config/credentials", () => {
    test("points AWS_CONFIG_FILE and AWS_SHARED_CREDENTIALS_FILE at per-transfer temp files", () => {
        const a = buildEngineEnv(baseParams);
        const b = buildEngineEnv(baseParams);

        for (const built of [a, b]) {
            expect(built.env.AWS_CONFIG_FILE).toBeDefined();
            expect(built.env.AWS_SHARED_CREDENTIALS_FILE).toBeDefined();
            expect(built.env.AWS_CONFIG_FILE.startsWith(os.tmpdir())).toBe(
                true
            );
            expect(
                built.env.AWS_SHARED_CREDENTIALS_FILE.startsWith(os.tmpdir())
            ).toBe(true);
            expect(built.env.AWS_REGION).toBe("us-east-1");
            expect(typeof built.cleanup).toBe("function");
            // Cleanup of never-created temp files is a no-op that must not throw.
            expect(() => built.cleanup()).not.toThrow();
        }

        // Distinct files per transfer so two concurrent `aws configure set`
        // writes can never collide on a shared config file.
        expect(a.env.AWS_CONFIG_FILE).not.toBe(b.env.AWS_CONFIG_FILE);
        expect(a.env.AWS_SHARED_CREDENTIALS_FILE).not.toBe(
            b.env.AWS_SHARED_CREDENTIALS_FILE
        );
    });

    test("resolves region to auto when unset", () => {
        const built = buildEngineEnv({ ...baseParams, region: undefined });
        expect(built.env.AWS_REGION).toBe("auto");
    });
});

describe("transferArchive engine selection + fallthrough", () => {
    let runS5cmd: AnyMock;
    let runAwsCli: AnyMock;
    let nodeFallback: AnyMock;

    const makeDeps = (available: {
        s5cmd?: boolean;
        aws?: boolean;
    }): TransferEngineDeps => ({
        findExecutable: jest.fn(async (name: string) => {
            if (name === "s5cmd" && available.s5cmd) return "/usr/bin/s5cmd";
            if (name === "aws" && available.aws) return "/usr/bin/aws";
            return undefined;
        }) as TransferEngineDeps["findExecutable"],
        runS5cmd: runS5cmd as unknown as TransferEngineDeps["runS5cmd"],
        runAwsCli: runAwsCli as unknown as TransferEngineDeps["runAwsCli"]
    });

    beforeEach(() => {
        runS5cmd = jest.fn(async () => undefined);
        runAwsCli = jest.fn(async () => undefined);
        nodeFallback = jest.fn(async () => undefined);
    });

    test("uses s5cmd when present and successful; node never runs", async () => {
        const engine = await transferArchive(
            "upload",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true })
        );
        expect(engine).toBe("s5cmd");
        expect(runS5cmd).toHaveBeenCalledTimes(1);
        expect(runAwsCli).not.toHaveBeenCalled();
        expect(nodeFallback).not.toHaveBeenCalled();
    });

    test("falls through s5cmd -> aws-cli when s5cmd is absent", async () => {
        const engine = await transferArchive(
            "download",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: false, aws: true })
        );
        expect(engine).toBe("aws-cli");
        expect(runS5cmd).not.toHaveBeenCalled();
        expect(runAwsCli).toHaveBeenCalledTimes(1);
        expect(nodeFallback).not.toHaveBeenCalled();
    });

    test("falls through to node when neither native engine is present", async () => {
        const engine = await transferArchive(
            "upload",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: false, aws: false })
        );
        expect(engine).toBe("node");
        expect(nodeFallback).toHaveBeenCalledTimes(1);
    });

    test("falls through on a FAILING engine (non-zero/error), not just a missing one", async () => {
        runS5cmd.mockRejectedValueOnce(new Error("s5cmd exit 1"));
        const engine = await transferArchive(
            "upload",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true })
        );
        expect(engine).toBe("aws-cli");
        expect(runS5cmd).toHaveBeenCalledTimes(1);
        expect(runAwsCli).toHaveBeenCalledTimes(1);
        expect(nodeFallback).not.toHaveBeenCalled();
    });

    test("both native engines failing falls all the way to node (never hard-fails)", async () => {
        runS5cmd.mockRejectedValueOnce(new Error("s5cmd boom"));
        runAwsCli.mockRejectedValueOnce(new Error("aws boom"));
        const engine = await transferArchive(
            "download",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true })
        );
        expect(engine).toBe("node");
        expect(runS5cmd).toHaveBeenCalledTimes(1);
        expect(runAwsCli).toHaveBeenCalledTimes(1);
        expect(nodeFallback).toHaveBeenCalledTimes(1);
    });

    test("s5cmd absent + aws failing still reaches node", async () => {
        runAwsCli.mockRejectedValueOnce(new Error("aws boom"));
        const engine = await transferArchive(
            "upload",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: false, aws: true })
        );
        expect(engine).toBe("node");
        expect(nodeFallback).toHaveBeenCalledTimes(1);
    });

    test("download: a passing verifyNativeDownload keeps the native engine result", async () => {
        const verify = jest.fn(async () => undefined);
        const engine = await transferArchive(
            "download",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true }),
            verify as () => Promise<void>
        );
        expect(engine).toBe("s5cmd");
        expect(verify).toHaveBeenCalledTimes(1);
        expect(nodeFallback).not.toHaveBeenCalled();
    });

    test("download: a failing verifyNativeDownload falls through to the next engine", async () => {
        const verify = jest.fn(async () => undefined);
        // s5cmd's transfer "succeeds" but its integrity check fails; aws-cli's
        // then passes verification.
        verify.mockRejectedValueOnce(new Error("size mismatch"));
        const engine = await transferArchive(
            "download",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true }),
            verify as () => Promise<void>
        );
        expect(engine).toBe("aws-cli");
        expect(runS5cmd).toHaveBeenCalledTimes(1);
        expect(runAwsCli).toHaveBeenCalledTimes(1);
        expect(verify).toHaveBeenCalledTimes(2);
        expect(nodeFallback).not.toHaveBeenCalled();
    });

    test("download: verify failing on every native engine still reaches node (never hard-fails)", async () => {
        const verify = jest.fn(async () => undefined);
        verify.mockRejectedValue(new Error("size mismatch"));
        const engine = await transferArchive(
            "download",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true }),
            verify as () => Promise<void>
        );
        expect(engine).toBe("node");
        // Verified after s5cmd and after aws-cli, but never for the node engine.
        expect(verify).toHaveBeenCalledTimes(2);
        expect(nodeFallback).toHaveBeenCalledTimes(1);
    });

    test("upload: verifyNativeDownload is never invoked (uploads self-validate)", async () => {
        const verify = jest.fn(async () => undefined);
        const engine = await transferArchive(
            "upload",
            baseParams,
            nodeFallback as () => Promise<void>,
            makeDeps({ s5cmd: true, aws: true }),
            verify as () => Promise<void>
        );
        expect(engine).toBe("s5cmd");
        expect(verify).not.toHaveBeenCalled();
    });
});
