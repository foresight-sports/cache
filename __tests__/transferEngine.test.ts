import { beforeEach, describe, expect, jest, test } from "@jest/globals";
import * as os from "os";

import {
    buildAwsCliConfigureArgs,
    buildAwsCliCpArgs,
    buildEngineEnv,
    buildS5cmdArgs,
    computeAwsCliConcurrency,
    computeS5cmdWorkers,
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

describe("core-scaling helpers", () => {
    test("computeS5cmdWorkers scales to cores with floor and ceiling", () => {
        expect(computeS5cmdWorkers(1)).toBe(32); // floor
        expect(computeS5cmdWorkers(16)).toBe(64); // 16 * 4
        expect(computeS5cmdWorkers(64)).toBe(256); // ceiling (64*4=256)
        expect(computeS5cmdWorkers(1000)).toBe(256); // ceiling clamp
        expect(computeS5cmdWorkers(0)).toBe(32); // degenerate -> floor
    });

    test("computeAwsCliConcurrency scales to cores with floor and ceiling", () => {
        expect(computeAwsCliConcurrency(1)).toBe(16); // floor
        expect(computeAwsCliConcurrency(16)).toBe(64);
        expect(computeAwsCliConcurrency(100)).toBe(256); // ceiling
    });

    test("resolveRegion defaults to auto when unset", () => {
        expect(resolveRegion("us-west-2")).toBe("us-west-2");
        expect(resolveRegion(undefined)).toBe("auto");
        expect(resolveRegion("")).toBe("auto");
    });
});

describe("buildS5cmdArgs", () => {
    test("upload places global flags, then cp, then <local> <s3uri>", () => {
        const args = buildS5cmdArgs("upload", baseParams, 16);
        expect(args).toEqual([
            "--numworkers",
            "64",
            "--stat",
            "--log",
            "error",
            "--endpoint-url",
            "https://s3.example.com",
            "cp",
            "--concurrency",
            "64",
            "--part-size",
            "64",
            "/tmp/cache.tzst",
            "s3://cache-bucket/cache/owner/repo/abc123/my-key"
        ]);
    });

    test("download reverses operands to <s3uri> <local>", () => {
        const args = buildS5cmdArgs("download", baseParams, 16);
        const cpIdx = args.indexOf("cp");
        // last two operands, in order
        expect(args.slice(-2)).toEqual([
            "s3://cache-bucket/cache/owner/repo/abc123/my-key",
            "/tmp/cache.tzst"
        ]);
        expect(cpIdx).toBeGreaterThan(-1);
    });

    test("omits --endpoint-url when no endpoint (real AWS S3) and --part-size when unknown", () => {
        const args = buildS5cmdArgs(
            "download",
            { ...baseParams, endpoint: undefined, partSizeMb: undefined },
            16
        );
        expect(args).not.toContain("--endpoint-url");
        expect(args).not.toContain("--part-size");
        expect(args).toContain("--numworkers");
    });
});

describe("buildAwsCliConfigureArgs / buildAwsCliCpArgs", () => {
    test("configure batches set the tuned s3 knobs", () => {
        const batches = buildAwsCliConfigureArgs(baseParams, 16);
        const flat = batches.map(b => b.join(" "));
        expect(flat).toContain(
            "configure set default.s3.max_concurrent_requests 64"
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

    test("configure adds path addressing style when forcePathStyle", () => {
        const batches = buildAwsCliConfigureArgs(
            { ...baseParams, forcePathStyle: true },
            16
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
