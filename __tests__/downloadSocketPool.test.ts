import {
    afterAll,
    beforeEach,
    describe,
    expect,
    jest,
    test
} from "@jest/globals";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

// Import the REAL http-client via its file path (a different specifier than the
// mocked bare "@actions/http-client") so we can re-export its other named
// exports — @actions/cache's retryHttpClientResponse imports HttpClientError etc.
// and a partial mock that dropped them would break the module graph.
import * as realHttpClient from "../node_modules/@actions/http-client/lib/index.js";

// Capture the options the downloader passes to the @actions/http-client
// constructor so we can assert the socket-pool sizing without a real transfer.
// This is an ESM test config (extensionsToTreatAsEsm), so the http-client module
// is mocked via jest.unstable_mockModule + a dynamic import of the module under
// test (static jest.mock would not intercept an ESM import).
const httpClientCtorCalls: unknown[][] = [];

jest.unstable_mockModule("@actions/http-client", () => ({
    ...realHttpClient,
    HttpClient: jest.fn().mockImplementation((...args: unknown[]) => {
        httpClientCtorCalls.push(args);
        return {
            // Resolve a response with NO content-range so the downloader throws
            // "Range request not supported by server" immediately (no retries,
            // no network) right after the client has been constructed.
            request: jest.fn(async () => ({
                message: { statusCode: 200, headers: {} }
            })),
            get: jest.fn(),
            dispose: jest.fn()
        };
    })
}));

const { downloadCacheHttpClientConcurrent } =
    await import("../src/custom/downloadUtils");

const tmpFiles: string[] = [];
function tmpArchivePath(): string {
    const p = path.join(
        os.tmpdir(),
        `socketpool-${Math.random().toString(36).slice(2)}.bin`
    );
    tmpFiles.push(p);
    return p;
}

describe("download socket-pool sizing", () => {
    beforeEach(() => {
        httpClientCtorCalls.length = 0;
    });

    afterAll(() => {
        for (const p of tmpFiles) {
            try {
                fs.rmSync(p, { force: true });
            } catch {
                /* ignore */
            }
        }
    });

    test("sizes maxSockets to downloadConcurrency + headroom", async () => {
        await expect(
            downloadCacheHttpClientConcurrent(
                "https://example.com/x",
                tmpArchivePath(),
                {
                    timeoutInMs: 30000,
                    partSize: 32 * 1024 * 1024,
                    downloadConcurrency: 16
                }
            )
        ).rejects.toThrow("Range request not supported");

        expect(httpClientCtorCalls).toHaveLength(1);
        const requestOptions = httpClientCtorCalls[0][2] as {
            keepAlive?: boolean;
            maxSockets?: number;
        };
        expect(requestOptions.keepAlive).toBe(true);
        // 16 concurrency + 4 headroom for the initial metadata probe.
        expect(requestOptions.maxSockets).toBe(20);
    });

    test("defaults maxSockets when downloadConcurrency is unset", async () => {
        await expect(
            downloadCacheHttpClientConcurrent(
                "https://example.com/y",
                tmpArchivePath(),
                {
                    timeoutInMs: 30000,
                    partSize: 32 * 1024 * 1024
                }
            )
        ).rejects.toThrow("Range request not supported");

        const requestOptions = httpClientCtorCalls[0][2] as {
            maxSockets?: number;
        };
        // Falls back to 8 + 4 headroom.
        expect(requestOptions.maxSockets).toBe(12);
    });
});
