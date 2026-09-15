import { describe, expect, test } from "@jest/globals";

import {
    DEFAULT_ZSTD_COMPRESS_ARGS,
    ENV_ZSTD_COMPRESS_ARGS,
    getZstdCompressArgs,
    getZstdCompressProgram
} from "../src/custom/utils/uncompressedTar";

describe("getZstdCompressArgs", () => {
    test("defaults to level 3 with a 1 GiB long window", () => {
        expect(getZstdCompressArgs({})).toBe("-T0 -3 --long=30");
        expect(getZstdCompressArgs({ [ENV_ZSTD_COMPRESS_ARGS]: "   " })).toBe(
            DEFAULT_ZSTD_COMPRESS_ARGS
        );
    });

    test("accepts plain option tokens and normalizes whitespace", () => {
        expect(getZstdCompressArgs({ [ENV_ZSTD_COMPRESS_ARGS]: " -T0   -1 " })).toBe(
            "-T0 -1"
        );
        expect(
            getZstdCompressArgs({ [ENV_ZSTD_COMPRESS_ARGS]: "-T16 -3 --long=27" })
        ).toBe("-T16 -3 --long=27");
    });

    test("rejects anything that is not an option token", () => {
        for (const bad of ["-1; rm -rf /", "-T0 foo", "--long=30 /tmp/x", "-T0 -1 | cat"]) {
            expect(getZstdCompressArgs({ [ENV_ZSTD_COMPRESS_ARGS]: bad })).toBe(
                DEFAULT_ZSTD_COMPRESS_ARGS
            );
        }
    });

    test("program string prefixes zstd", () => {
        expect(getZstdCompressProgram({ [ENV_ZSTD_COMPRESS_ARGS]: "-T0 -1" })).toBe(
            "zstd -T0 -1"
        );
    });
});
