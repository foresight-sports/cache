import { describe, expect, test } from "@jest/globals";
import { execSync } from "child_process";
import { createHash } from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import {
    createShardedArchive,
    extractShardedArchive
} from "../src/custom/shardedArchive";

// Real tar + zstd round trip on a small tree. Skipped when zstd is not on PATH
// (the unit suite covers the orchestration with mocks).
function hasZstd(): boolean {
    try {
        execSync("zstd --version", { stdio: "ignore" });
        return true;
    } catch {
        return false;
    }
}

const maybe = hasZstd() ? describe : describe.skip;

maybe("sharded archive round trip (real tar/zstd)", () => {
    test("creates N parts and restores every file and empty directory", async () => {
        const workspace = fs.mkdtempSync(path.join(os.tmpdir(), "shard-e2e-"));
        const previousWorkspace = process.env["GITHUB_WORKSPACE"];
        process.env["GITHUB_WORKSPACE"] = workspace;
        try {
            const lib = path.join(workspace, "Library");
            fs.mkdirSync(path.join(lib, "Artifacts", "aa"), {
                recursive: true
            });
            fs.mkdirSync(path.join(lib, "Artifacts", "bb"), {
                recursive: true
            });
            fs.mkdirSync(path.join(lib, "Empty"), { recursive: true });
            const expected = new Map<string, Buffer>();
            for (let i = 0; i < 12; i++) {
                const rel = path.join(
                    "Artifacts",
                    i % 2 ? "aa" : "bb",
                    `file-${i}.bin`
                );
                const body = Buffer.alloc(1024 * (i + 1) * 37, i);
                fs.writeFileSync(path.join(lib, rel), body);
                expected.set(rel.split(path.sep).join("/"), body);
            }
            fs.writeFileSync(path.join(lib, "ArtifactDB"), "db\n");
            expected.set("ArtifactDB", Buffer.from("db\n"));

            const staging = fs.mkdtempSync(
                path.join(os.tmpdir(), "shard-stage-")
            );
            const result = await createShardedArchive(
                staging,
                ["Library"],
                3,
                "e2e-key"
            );
            expect(result.parts.length).toBe(3);
            expect(result.manifest.totalFiles).toBe(13);
            expect(result.manifest.generation).toBeDefined();
            for (const part of result.parts) {
                expect(fs.statSync(part.path).size).toBeGreaterThan(0);
                expect(part.key).toBe(
                    `e2e-key.shards/${result.manifest.generation}/${part.name}`
                );
                // The recorded digest is of the real zstd part on disk.
                expect(part.sha256).toBe(
                    createHash("sha256")
                        .update(fs.readFileSync(part.path))
                        .digest("hex")
                );
            }

            // Wipe the tree, then extract every part concurrently into the workspace.
            fs.rmSync(lib, { recursive: true, force: true });
            await extractShardedArchive(result.parts.map(part => part.path));

            for (const [rel, body] of expected) {
                const restored = fs.readFileSync(path.join(lib, rel));
                expect(restored.equals(body)).toBe(true);
            }
            expect(fs.statSync(path.join(lib, "Empty")).isDirectory()).toBe(
                true
            );
            fs.rmSync(staging, { recursive: true, force: true });
        } finally {
            if (previousWorkspace === undefined) {
                delete process.env["GITHUB_WORKSPACE"];
            } else {
                process.env["GITHUB_WORKSPACE"] = previousWorkspace;
            }
            fs.rmSync(workspace, { recursive: true, force: true });
        }
    }, 120000);
});
