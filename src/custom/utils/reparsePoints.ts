// Windows directory-junction (reparse point) handling for the cache file scan.
//
// @actions/cache's resolvePaths() returns the *directory name* for a cached
// directory (e.g. "Library") and relies on tar recursing into it. That breaks
// when the directory is a Windows junction: Premier's CI routes Unity's
// Library/ onto instance-store NVMe with `cmd /c mklink /J Library Z:\...`.
// tar treats the junction as a symbolic link and stores an un-followed link
// entry instead of descending, so the archive captures ZERO files and the save
// produces an empty (~218 B) archive.
//
// The fix (see expandWindowsReparsePoints) descends *through* the junction and
// emits each real child under the SAME relative name ("Library/..."), never the
// junction's absolute target. That keeps the tar `-C <workspace>
// --files-from manifest` invocation intact while making it archive the real
// contents. It is a no-op on non-Windows and for ordinary files/directories, so
// Linux/macOS and the normal (restore-hit, real-directory) path are unchanged.
import * as fs from "fs";
import * as path from "path";

// Normalize an OS path to the forward-slash manifest form, matching the
// separator normalization @actions/cache's resolvePaths() applies to its output.
function toManifestPath(relativePath: string): string {
    return relativePath.split(path.sep).join("/");
}

/**
 * Whether `absolutePath` is a directory reparse point — a Windows junction
 * (`mklink /J`) or a directory symlink. lstat reports these as a symbolic link
 * whose stat() target is a directory; an ordinary directory is not a symlink.
 */
export function isReparsePointDirectory(absolutePath: string): boolean {
    let linkStats: fs.Stats;
    try {
        linkStats = fs.lstatSync(absolutePath);
    } catch {
        return false;
    }
    if (!linkStats.isSymbolicLink()) {
        return false;
    }
    try {
        // stat() follows the reparse point to its target.
        return fs.statSync(absolutePath).isDirectory();
    } catch {
        return false;
    }
}

// Recursively collect the contents of a directory (reached through a reparse
// point), pushing each real file — and each empty directory — as a
// workspace-relative, forward-slash manifest path into `out`.
function collectReparsePointEntries(
    absoluteDir: string,
    workspaceRoot: string,
    out: string[]
): void {
    let children: fs.Dirent[];
    try {
        children = fs.readdirSync(absoluteDir, { withFileTypes: true });
    } catch {
        // Unreadable directory: skip it, mirroring the globber's tolerance.
        return;
    }
    if (children.length === 0) {
        // Preserve an otherwise-lost empty directory as its own entry.
        out.push(toManifestPath(path.relative(workspaceRoot, absoluteDir)));
        return;
    }
    for (const child of children) {
        const absoluteChild = path.join(absoluteDir, child.name);
        let isDirectory: boolean;
        try {
            // stat() (not lstat) so nested junctions / directory symlinks are
            // descended too, rather than emitted as un-followed link entries.
            isDirectory = fs.statSync(absoluteChild).isDirectory();
        } catch {
            // Broken/dangling link or vanished entry: skip it, matching the
            // globber's omitBrokenSymbolicLinks behavior.
            continue;
        }
        if (isDirectory) {
            collectReparsePointEntries(absoluteChild, workspaceRoot, out);
        } else {
            out.push(
                toManifestPath(path.relative(workspaceRoot, absoluteChild))
            );
        }
    }
}

/**
 * Expand any Windows directory junction / reparse point in a resolved cache
 * file list into the real relative paths of its contents, preserving the
 * original relative prefix (e.g. "Library" -> "Library/a.txt",
 * "Library/sub/b.txt"). Ordinary files and directories pass through unchanged,
 * and the whole function is a no-op off Windows.
 *
 * @param cachePaths workspace-relative paths from @actions/cache resolvePaths()
 * @param workspaceRoot root the paths are relative to (tar's `-C` directory);
 *   defaults to GITHUB_WORKSPACE, matching resolvePaths() / the tar invocation
 */
export function expandWindowsReparsePoints(
    cachePaths: string[],
    workspaceRoot: string = process.env["GITHUB_WORKSPACE"] ?? process.cwd()
): string[] {
    if (process.platform !== "win32") {
        return cachePaths;
    }
    const expanded: string[] = [];
    for (const cachePath of cachePaths) {
        // "." is resolvePaths' marker for "the workspace itself" — leave as-is.
        if (cachePath === ".") {
            expanded.push(cachePath);
            continue;
        }
        const absolutePath = path.resolve(workspaceRoot, cachePath);
        if (!isReparsePointDirectory(absolutePath)) {
            expanded.push(cachePath);
            continue;
        }
        const before = expanded.length;
        collectReparsePointEntries(absolutePath, workspaceRoot, expanded);
        if (expanded.length === before) {
            // Empty/unreadable junction target: keep the original entry so
            // behavior is never worse than before the fix.
            expanded.push(cachePath);
        }
    }
    return expanded;
}
