import { describe, expect, test } from "@jest/globals";

import {
    computeEffectivePartSize,
    MAX_MULTIPART_PARTS_TARGET,
    S3_MIN_PART_SIZE
} from "../src/custom/utils/partSize";

const MB = 1024 * 1024;
const GB = 1024 * MB;
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
            name: "50 MB cache, 64 MB default -> keeps configured size",
            cacheSize: 50 * MB,
            configuredPartSize: 64 * MB,
            expected: 64 * MB
        },
        {
            name: "320 GB cache, 64 MB default -> still fits, keeps configured size",
            cacheSize: 320 * GB,
            configuredPartSize: 64 * MB,
            expected: 64 * MB
        },
        {
            // Large cache where the configured size would exceed the 10000-part
            // ceiling: part size grows to keep <= ~9500 parts.
            name: "400 GB cache, 32 MB chunk -> grown to fit under part cap",
            cacheSize: 400 * GB,
            configuredPartSize: 32 * MB,
            expected: Math.ceil((400 * GB) / MAX_MULTIPART_PARTS_TARGET)
        },
        {
            name: "700 GB cache, 64 MB default -> grown to fit under part cap",
            cacheSize: 700 * GB,
            configuredPartSize: 64 * MB,
            expected: Math.ceil((700 * GB) / MAX_MULTIPART_PARTS_TARGET)
        },
        {
            // Even a huge cache with a tiny chunk stays representable and >= 5 MiB.
            name: "700 GB cache, 1 MB chunk -> grown to fit under part cap",
            cacheSize: 700 * GB,
            configuredPartSize: 1 * MB,
            expected: Math.ceil((700 * GB) / MAX_MULTIPART_PARTS_TARGET)
        }
    ];

    test.each(cases)("$name", ({ cacheSize, configuredPartSize, expected }) => {
        const partSize = computeEffectivePartSize(cacheSize, configuredPartSize);

        expect(partSize).toBe(expected);

        // Invariant 1: never below S3's 5 MiB per-part minimum.
        expect(partSize).toBeGreaterThanOrEqual(S3_MIN_PART_SIZE);

        // Invariant 2: estimated parts stay strictly under the 10000-part cap.
        const estimatedParts = Math.max(1, Math.ceil(cacheSize / partSize));
        expect(estimatedParts).toBeLessThan(MAX_PARTS);
    });
});
