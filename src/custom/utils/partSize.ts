// S3 multipart-upload part-size math, centralized here so it is unit testable in
// isolation without pulling in the S3 client / AWS SDK (see __tests__/partSize.test.ts).
//
//  - @aws-sdk/lib-storage enforces a hard MAX_PARTS = 10000 ceiling. Targeting
//    <= ~9500 parts (headroom under 10000) keeps any payload representable
//    without throwing "Exceeded 10000 parts".
//  - S3 requires every part except the last to be at least 5 MiB. A small
//    upload-chunk-size on a mid-size cache could otherwise yield a sub-5 MiB part
//    that S3 rejects with EntityTooSmall.
//  - Beyond just staying under the hard ceiling, a very large archive at the
//    default (e.g. 64 MB) part size produces thousands of parts; each part
//    completion is main-loop work in the single-threaded Node uploader. So we
//    also scale the part size UP for large archives — targeting ~PREFERRED parts
//    (fewer, bigger parts => less per-part overhead) — capped at MAX_PART_SIZE so
//    we keep enough parts for multipart concurrency and bound the lib-storage
//    in-flight buffer memory (queueSize * partSize).
export const MAX_MULTIPART_PARTS_TARGET = 9500;
export const PREFERRED_MULTIPART_PARTS = 2000;
export const S3_MIN_PART_SIZE = 5 * 1024 * 1024;
export const MAX_PART_SIZE = 128 * 1024 * 1024;

/**
 * Compute the multipart part size (in bytes) for a cache upload: the largest of
 *  - the configured part size (a floor: the upload-chunk-size input or default),
 *  - the hard floor that keeps the payload under the ~9500-part ceiling,
 *  - the upward "preferred" size that targets ~PREFERRED_MULTIPART_PARTS parts
 *    for large archives (capped at MAX_PART_SIZE), and
 *  - S3's 5 MiB per-part minimum.
 *
 * Taking the max means the part count is always < ~9500 (the hard floor is one
 * of the terms) and never below 5 MiB, while large archives get bigger parts to
 * cut per-part overhead.
 */
export function computeEffectivePartSize(
    cacheSize: number,
    configuredPartSize: number
): number {
    // Hard floor: guarantees ceil(cacheSize / partSize) <= ~9500 parts.
    const partCeilingFloor = Math.ceil(cacheSize / MAX_MULTIPART_PARTS_TARGET);
    // Upward scaling for large archives: aim for ~PREFERRED parts, but never let
    // a single part exceed MAX_PART_SIZE (preserves concurrency + bounds memory).
    const preferredForSize = Math.min(
        MAX_PART_SIZE,
        Math.ceil(cacheSize / PREFERRED_MULTIPART_PARTS)
    );
    return Math.max(
        configuredPartSize,
        partCeilingFloor,
        preferredForSize,
        S3_MIN_PART_SIZE
    );
}
