// S3 multipart-upload part-size math, centralized here so it is unit testable in
// isolation without pulling in the S3 client / AWS SDK (see __tests__/partSize.test.ts).
//
//  - @aws-sdk/lib-storage enforces a hard MAX_PARTS = 10000 ceiling. Targeting
//    <= ~9500 parts (headroom under 10000) keeps any payload representable
//    without throwing "Exceeded 10000 parts".
//  - S3 requires every part except the last to be at least 5 MiB. A small
//    upload-chunk-size on a mid-size cache could otherwise yield a sub-5 MiB part
//    that S3 rejects with EntityTooSmall.
export const MAX_MULTIPART_PARTS_TARGET = 9500;
export const S3_MIN_PART_SIZE = 5 * 1024 * 1024;

/**
 * Compute the multipart part size (in bytes) for a cache upload: the configured
 * part size, floored so the payload fits in <= ~9500 parts and never drops below
 * S3's 5 MiB per-part minimum.
 */
export function computeEffectivePartSize(
    cacheSize: number,
    configuredPartSize: number
): number {
    return Math.max(
        configuredPartSize,
        Math.ceil(cacheSize / MAX_MULTIPART_PARTS_TARGET),
        S3_MIN_PART_SIZE
    );
}
