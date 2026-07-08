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
// CACHE_STREAM_RESTORE opt-in gate (default OFF -> file-based).
// ============================================================================
describe("isStreamRestoreEnabled (CACHE_STREAM_RESTORE opt-in, default OFF)", () => {
    test("streaming is OFF by default (unset / blank)", () => {
        expect(isStreamRestoreEnabled({} as NodeJS.ProcessEnv)).toBe(false);
        expect(
            isStreamRestoreEnabled({
                [ENV_STREAM_RESTORE]: ""
            } as NodeJS.ProcessEnv)
        ).toBe(false);
    });

    test("only explicit 1/true/yes/on (any case, trimmed) enable streaming", () => {
        for (const on of ["1", "true", "TRUE", "Yes", "on", " on "]) {
            expect(
                isStreamRestoreEnabled({
                    [ENV_STREAM_RESTORE]: on
                } as NodeJS.ProcessEnv)
            ).toBe(true);
        }
    });

    test("0/false/no/off and any other value keep the file-based path", () => {
        for (const off of [
            "0",
            "false",
            "FALSE",
            "No",
            "off",
            " off ",
            "maybe",
            "2",
            "enabled"
        ]) {
            expect(
                isStreamRestoreEnabled({
                    [ENV_STREAM_RESTORE]: off
                } as NodeJS.ProcessEnv)
            ).toBe(false);
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

    test("uses aws-cli cp - | tar PRIMARY when present and successful; s5cmd never runs", async () => {
        const engine = await streamedRestore(
            baseParams,
            makeDeps({ s5cmd: true, aws: true })
        );
        // aws-cli is the PRIMARY streaming engine (bounded ring buffer, robust
        // with a slow tar consumer); s5cmd cat never runs when aws-cli succeeds.
        expect(engine).toBe("aws-cli");
        expect(runAwsConfigure).toHaveBeenCalledTimes(1);
        expect(runPipelineMock).toHaveBeenCalledTimes(1);

        // The downloader spec fed to the pipe is `aws s3 cp <uri> -` argv (no
        // shell string), and the tar spec is the stdin extractor.
        const [downloader, tar] = runPipelineMock.mock.calls[0] as [
            SpawnSpec,
            SpawnSpec
        ];
        expect(downloader.command).toBe("/usr/bin/aws");
        expect(downloader.args.slice(0, 2)).toEqual(["s3", "cp"]);
        expect(downloader.args).toContain("-"); // cp to stdout
        expect(tar.args).toContain("-"); // tar -xf -
    });

    test("aws absent -> falls through to SECONDARY s5cmd cat | tar (no configure)", async () => {
        const engine = await streamedRestore(
            baseParams,
            makeDeps({ s5cmd: true, aws: false })
        );
        expect(engine).toBe("s5cmd");
        // s5cmd cat needs no aws configure.
        expect(runAwsConfigure).not.toHaveBeenCalled();
        expect(runPipelineMock).toHaveBeenCalledTimes(1);

        const [downloader] = runPipelineMock.mock.calls[0] as [SpawnSpec];
        expect(downloader.command).toBe("/usr/bin/s5cmd");
        expect(downloader.args).toContain("cat");
        expect(downloader.args[downloader.args.length - 1]).toBe(
            "s3://cache-bucket/cache/owner/repo/abc123/my-key"
        );
        // The secondary s5cmd cat runs at the LOW streaming concurrency (default
        // 6), NOT the 256-way cp download concurrency that truncated the stream.
        expect(
            downloader.args[downloader.args.indexOf("--concurrency") + 1]
        ).toBe("6");
    });

    test("aws-cli stream FAILS -> falls through to s5cmd cat stream", async () => {
        runPipelineMock
            .mockRejectedValueOnce(new Error("aws-cli broken pipe"))
            .mockResolvedValueOnce(undefined);
        const engine = await streamedRestore(
            baseParams,
            makeDeps({ s5cmd: true, aws: true })
        );
        expect(engine).toBe("s5cmd");
        expect(runPipelineMock).toHaveBeenCalledTimes(2);
        // Primary attempt ran the aws configure; the s5cmd fallback did not add
        // another.
        expect(runAwsConfigure).toHaveBeenCalledTimes(1);
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
