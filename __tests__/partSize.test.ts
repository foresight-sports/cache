import { describe, expect, test } from "@jest/globals";

import {
    computeEffectivePartSize,
    MAX_MULTIPART_PARTS_TARGET,
    MAX_PART_SIZE,
    S3_MIN_PART_SIZE
} from "../src/custom/utils/partSize";

const MB = 1024 * 1024;
const GB = 1024 * MB;
const TB = 1024 * GB;
// @aws-sdk/lib-storage's hard ceiling; effective part size must keep us below it.
const MAX_PARTS = 10000;

describe("computeEffectivePartSize", () => {
    const cases: Array<{
        name: string;
        cacheSize: number;
        configuredPartSize: number;
        expected: number;
    }> = [
        {
            // Small cache + tiny chunk: the sub-5 MiB configured size is lifted
            // to S3's 5 MiB per-part minimum (would otherwise be EntityTooSmall).
            name: "50 MB cache, 1 MB chunk -> floored to S3 5 MiB minimum",
            cacheSize: 50 * MB,
            configuredPartSize: 1 * MB,
            expected: S3_MIN_PART_SIZE
        },
        {
            name: "50 MB cache, 64 MB default -> keeps configured size (no upscale)",
            cacheSize: 50 * MB,
            configuredPartSize: 64 * MB,
            expected: 64 * MB
        },
        {
            // Below the upscale threshold: ceil(50GB/2000)=~26 MB < configured, so
            // the configured 64 MB still wins.
            name: "50 GB cache, 64 MB default -> keeps configured size",
            cacheSize: 50 * GB,
            configuredPartSize: 64 * MB,
            expected: 64 * MB
        },
        {
            // Large archive: upscaled toward ~2000 parts but capped at MAX_PART_SIZE
            // (fewer, bigger parts => less per-part main-loop overhead).
            name: "320 GB cache, 64 MB default -> upscaled to MAX_PART_SIZE cap",
            cacheSize: 320 * GB,
            configuredPartSize: 64 * MB,
            expected: MAX_PART_SIZE
        },
        {
            name: "400 GB cache, 32 MB chunk -> upscaled to MAX_PART_SIZE cap",
            cacheSize: 400 * GB,
            configuredPartSize: 32 * MB,
            expected: MAX_PART_SIZE
        },
        {
            name: "700 GB cache, 64 MB default -> upscaled to MAX_PART_SIZE cap",
            cacheSize: 700 * GB,
            configuredPartSize: 64 * MB,
            expected: MAX_PART_SIZE
        },
        {
            // Even a huge cache with a tiny chunk stays representable, upscaled,
            // and >= 5 MiB.
            name: "700 GB cache, 1 MB chunk -> upscaled to MAX_PART_SIZE cap",
            cacheSize: 700 * GB,
            configuredPartSize: 1 * MB,
            expected: MAX_PART_SIZE
        },
        {
            // Beyond MAX_PART_SIZE * ~9500 (~1.16 TB) the hard 9500-part ceiling
            // floor governs and grows the part size past the MAX_PART_SIZE cap so
            // the payload still fits under lib-storage's 10000-part limit.
            name: "2 TB cache, 64 MB default -> hard part-ceiling floor governs (> cap)",
            cacheSize: 2 * TB,
            configuredPartSize: 64 * MB,
            expected: Math.ceil((2 * TB) / MAX_MULTIPART_PARTS_TARGET)
        }
    ];

    test.each(cases)("$name", ({ cacheSize, configuredPartSize, expected }) => {
        const partSize = computeEffectivePartSize(
            cacheSize,
            configuredPartSize
        );

        expect(partSize).toBe(expected);

        // Invariant 1: never below S3's 5 MiB per-part minimum.
        expect(partSize).toBeGreaterThanOrEqual(S3_MIN_PART_SIZE);

        // Invariant 2: estimated parts stay strictly under the 10000-part cap.
        const estimatedParts = Math.max(1, Math.ceil(cacheSize / partSize));
        expect(estimatedParts).toBeLessThan(MAX_PARTS);

        // Invariant 3: the part size never exceeds the MAX_PART_SIZE cap unless
        // the hard part-ceiling floor forces it higher (very large payloads).
        expect(partSize).toBeLessThanOrEqual(
            Math.max(
                MAX_PART_SIZE,
                Math.ceil(cacheSize / MAX_MULTIPART_PARTS_TARGET)
            )
        );
    });

    test("never returns below the configured part size when it is already large", () => {
        // A configured part size above the upscale/ceiling terms is preserved.
        expect(computeEffectivePartSize(10 * GB, 256 * MB)).toBe(256 * MB);
    });
});
