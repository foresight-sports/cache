import { afterAll, beforeAll, describe, expect, test } from "@jest/globals";
import { execFileSync } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import {
    expandWindowsReparsePoints,
    isReparsePointDirectory
} from "../src/custom/utils/reparsePoints";

const IS_WINDOWS = process.platform === "win32";

describe("expandWindowsReparsePoints (cross-platform)", () => {
    let workspace: string;

    beforeAll(() => {
        workspace = fs.mkdtempSync(path.join(os.tmpdir(), "reparse-xplat-"));
        fs.mkdirSync(path.join(workspace, "real", "sub"), { recursive: true });
        fs.writeFileSync(path.join(workspace, "real", "a.txt"), "a");
        fs.writeFileSync(path.join(workspace, "real", "sub", "b.txt"), "b");
    });

    afterAll(() => {
        fs.rmSync(workspace, { recursive: true, force: true });
    });

    test("passes an ordinary directory entry through unchanged", () => {
        // A real directory is not a reparse point, so tar's own recursion is
        // relied on and the entry is returned as-is on every platform.
        expect(expandWindowsReparsePoints(["real"], workspace)).toEqual([
            "real"
        ]);
    });

    test("passes the '.' workspace marker through unchanged", () => {
        expect(expandWindowsReparsePoints(["."], workspace)).toEqual(["."]);
    });

    test("passes a plain file entry through unchanged", () => {
        expect(expandWindowsReparsePoints(["real/a.txt"], workspace)).toEqual([
            "real/a.txt"
        ]);
    });
});

// Junction creation via `mklink /J` is Windows-only; these assertions cover the
// actual bug (an NVMe-junctioned Library/ that produced an empty archive).
(IS_WINDOWS ? describe : describe.skip)(
    "expandWindowsReparsePoints (Windows junctions)",
    () => {
        let workspace: string;
        let target: string;

        beforeAll(() => {
            workspace = fs.mkdtempSync(path.join(os.tmpdir(), "reparse-ws-"));
            target = fs.mkdtempSync(path.join(os.tmpdir(), "reparse-tgt-"));

            // Populate the junction *target* with a nested tree.
            fs.mkdirSync(path.join(target, "sub"), { recursive: true });
            fs.writeFileSync(path.join(target, "a.txt"), "hello-a");
            fs.writeFileSync(path.join(target, "c.bin"), "hello-c");
            fs.writeFileSync(path.join(target, "sub", "b.txt"), "hello-b");

            // workspace/Library -> target  (directory junction)
            execFileSync("cmd", [
                "/c",
                "mklink",
                "/J",
                path.join(workspace, "Library"),
                target
            ]);
        });

        afterAll(() => {
            fs.rmSync(workspace, { recursive: true, force: true });
            fs.rmSync(target, { recursive: true, force: true });
        });

        test("detects the junction as a reparse-point directory", () => {
            expect(
                isReparsePointDirectory(path.join(workspace, "Library"))
            ).toBe(true);
        });

        test("does not flag the real target directory as a reparse point", () => {
            expect(isReparsePointDirectory(target)).toBe(false);
        });

        test("expands the junction into its real relative contents", () => {
            const result = expandWindowsReparsePoints(["Library"], workspace);

            // Every entry stays under the original relative "Library/" prefix —
            // never rewritten to the junction's absolute target — so the tar
            // `-C <workspace> --files-from manifest` invocation is unchanged.
            expect(result.slice().sort()).toEqual(
                ["Library/a.txt", "Library/c.bin", "Library/sub/b.txt"].sort()
            );
            for (const entry of result) {
                expect(entry.startsWith("Library/")).toBe(true);
                expect(path.isAbsolute(entry)).toBe(false);
            }
        });
    }
);
