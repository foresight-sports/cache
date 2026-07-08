import { beforeEach, describe, expect, jest, test } from "@jest/globals";

import {
    ENV_STREAM_RESTORE,
    isStreamRestoreEnabled,
    runPipeline,
    SpawnSpec,
    streamedRestore,
    StreamRestoreDeps
} from "../src/custom/streamingRestore";
import { TransferParams } from "../src/custom/transferEngine";
import { buildStreamExtractCoreArgs } from "../src/custom/utils/uncompressedTar";

type AnyMock = ReturnType<typeof jest.fn>;

const baseParams: TransferParams = {
    bucket: "cache-bucket",
    key: "cache/owner/repo/abc123/my-key",
    archivePath: "", // unused for streaming
    endpoint: "https://s3.example.com",
    region: "us-east-1",
    forcePathStyle: false
};

const tarSpec: SpawnSpec = {
    command: "/usr/bin/tar",
    args: buildStreamExtractCoreArgs("/workspace"),
    env: { PATH: "/usr/bin" }
};

// ============================================================================
// CACHE_STREAM_RESTORE off-switch.
// ============================================================================
describe("isStreamRestoreEnabled (CACHE_STREAM_RESTORE off-switch)", () => {
    test("streaming is ON by default (unset / blank)", () => {
        expect(isStreamRestoreEnabled({} as NodeJS.ProcessEnv)).toBe(true);
        expect(
            isStreamRestoreEnabled({
                [ENV_STREAM_RESTORE]: ""
            } as NodeJS.ProcessEnv)
        ).toBe(true);
    });

    test("0/false/no/off (any case) force the legacy file-based restore", () => {
        for (const off of ["0", "false", "FALSE", "No", "off", " off "]) {
            expect(
                isStreamRestoreEnabled({
                    [ENV_STREAM_RESTORE]: off
                } as NodeJS.ProcessEnv)
            ).toBe(false);
        }
    });

    test("any other value keeps streaming enabled", () => {
        for (const on of ["1", "true", "yes", "on"]) {
            expect(
                isStreamRestoreEnabled({
                    [ENV_STREAM_RESTORE]: on
                } as NodeJS.ProcessEnv)
            ).toBe(true);
        }
    });
});

// ============================================================================
// Streamed tar extract command preserves the zstd long-range window.
// ============================================================================
describe("buildStreamExtractCoreArgs (tar reads compressed stream from stdin)", () => {
    test("reads from stdin (`-xf -`) and keeps `zstd -d --long=30` unchanged", () => {
        const args = buildStreamExtractCoreArgs("/workspace");
        expect(args).toEqual([
            "--use-compress-program",
            "zstd -d --long=30",
            "-xf",
            "-", // stdin, NOT a file path
            "-P",
            "-C",
            "/workspace"
        ]);
        // The long-range window must match the create side; guard against drift.
        expect(args).toContain("zstd -d --long=30");
        // Never a scratch file path in the streamed extract.
        expect(args).not.toContain("cache.tzst");
    });
});

// ============================================================================
// Engine selection + fall-through (injected deps, no real processes / S3).
// ============================================================================
describe("streamedRestore engine selection + fallthrough", () => {
    let buildExtractCommand: AnyMock;
    let runPipelineMock: AnyMock;
    let runAwsConfigure: AnyMock;

    const makeDeps = (available: {
        s5cmd?: boolean;
        aws?: boolean;
    }): StreamRestoreDeps => ({
        findExecutable: jest.fn(async (name: string) => {
            if (name === "s5cmd" && available.s5cmd) return "/usr/bin/s5cmd";
            if (name === "aws" && available.aws) return "/usr/bin/aws";
            return undefined;
        }) as StreamRestoreDeps["findExecutable"],
        buildExtractCommand:
            buildExtractCommand as unknown as StreamRestoreDeps["buildExtractCommand"],
        runPipeline:
            runPipelineMock as unknown as StreamRestoreDeps["runPipeline"],
        runAwsConfigure:
            runAwsConfigure as unknown as StreamRestoreDeps["runAwsConfigure"]
    });

    beforeEach(() => {
        buildExtractCommand = jest.fn(async () => tarSpec);
        runPipelineMock = jest.fn(async () => undefined);
        runAwsConfigure = jest.fn(async () => undefined);
    });

    test("uses s5cmd cat | tar when present and successful; aws never runs", async () => {
        const engine = await streamedRestore(
            baseParams,
            makeDeps({ s5cmd: true, aws: true })
        );
        expect(engine).toBe("s5cmd");
        expect(runPipelineMock).toHaveBeenCalledTimes(1);
        expect(runAwsConfigure).not.toHaveBeenCalled();

        // The downloader spec fed to the pipe is `s5cmd cat …` argv (no shell
        // string), and the tar spec is the stdin extractor.
        const [downloader, tar] = runPipelineMock.mock.calls[0] as [
            SpawnSpec,
            SpawnSpec
        ];
        expect(downloader.command).toBe("/usr/bin/s5cmd");
        expect(downloader.args).toContain("cat");
        expect(downloader.args[downloader.args.length - 1]).toBe(
            "s3://cache-bucket/cache/owner/repo/abc123/my-key"
        );
        expect(tar.args).toContain("-"); // tar -xf -
    });

    test("s5cmd absent -> falls through to aws-cli cp - | tar (configure runs first)", async () => {
        const engine = await streamedRestore(
            baseParams,
            makeDeps({ s5cmd: false, aws: true })
        );
        expect(engine).toBe("aws-cli");
        expect(runAwsConfigure).toHaveBeenCalledTimes(1);
        expect(runPipelineMock).toHaveBeenCalledTimes(1);

        const [downloader] = runPipelineMock.mock.calls[0] as [SpawnSpec];
        expect(downloader.command).toBe("/usr/bin/aws");
        expect(downloader.args.slice(0, 2)).toEqual(["s3", "cp"]);
        expect(downloader.args).toContain("-"); // cp to stdout
    });

    test("s5cmd stream FAILS -> falls through to aws-cli stream", async () => {
        runPipelineMock
            .mockRejectedValueOnce(new Error("s5cmd broken pipe"))
            .mockResolvedValueOnce(undefined);
        const engine = await streamedRestore(
            baseParams,
            makeDeps({ s5cmd: true, aws: true })
        );
        expect(engine).toBe("aws-cli");
        expect(runPipelineMock).toHaveBeenCalledTimes(2);
    });

    test("both streaming engines fail -> THROWS so the caller falls back to file-based", async () => {
        runPipelineMock.mockRejectedValue(new Error("stream failed"));
        await expect(
            streamedRestore(baseParams, makeDeps({ s5cmd: true, aws: true }))
        ).rejects.toThrow(/no streaming restore engine available/);
        expect(runPipelineMock).toHaveBeenCalledTimes(2);
    });

    test("no streaming engine available -> THROWS (file-based fallback)", async () => {
        await expect(
            streamedRestore(baseParams, makeDeps({ s5cmd: false, aws: false }))
        ).rejects.toThrow(/no streaming restore engine available/);
        expect(runPipelineMock).not.toHaveBeenCalled();
        expect(buildExtractCommand).not.toHaveBeenCalled();
    });
});

// ============================================================================
// runPipeline real spawn/pipe wiring (cross-platform via node stand-ins).
// Proves: stdout->stdin piping, and BOTH children must exit 0. The actual
// download+extract OVERLAP speedup + peak RAM can only be measured on a real
// runner with s5cmd/tar/zstd and the 44 GB object.
// ============================================================================
describe("runPipeline (Node stdout->stdin wiring, both must exit 0)", () => {
    const node = process.execPath;
    const env = { ...process.env } as { [key: string]: string };
    const spec = (script: string): SpawnSpec => ({
        command: node,
        args: ["-e", script],
        env
    });

    test("resolves when producer streams to consumer and BOTH exit 0", async () => {
        const producer = spec(
            "process.stdout.write(Buffer.alloc(1<<20, 7)); process.exit(0);"
        );
        // Consumer drains stdin fully, then exits 0.
        const consumer = spec(
            "let n=0; process.stdin.on('data', d => n+=d.length); process.stdin.on('end', () => process.exit(0));"
        );
        await expect(runPipeline(producer, consumer)).resolves.toBeUndefined();
    });

    test("rejects when the DOWNLOADER exits non-zero (integrity: need clean stream)", async () => {
        const producer = spec(
            "process.stdout.write('partial'); process.exit(2);"
        );
        const consumer = spec(
            "process.stdin.resume(); process.stdin.on('end', () => process.exit(0));"
        );
        await expect(runPipeline(producer, consumer)).rejects.toThrow(
            /downloader exit 2/
        );
    });

    test("rejects when TAR exits non-zero even if the downloader would finish", async () => {
        // Consumer exits 3 immediately (simulates a tar/zstd extract error);
        // the producer is killed and the pipeline rejects.
        const producer = spec(
            "const t=setInterval(()=>process.stdout.write('x'),5);"
        );
        const consumer = spec("process.exit(3);");
        await expect(runPipeline(producer, consumer)).rejects.toThrow(
            /tar exit 3/
        );
    });
});
