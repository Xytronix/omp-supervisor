import { expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { launchOptions, supportsParentSessionId, withoutParentSessionId } from "./omp-rpc-host.mjs";

test("fresh remote launch passes parent lineage as the raw parent id", () => {
	const options = launchOptions({ cwd: "/tmp", parent_session_id: "parent-123" }, "/home/test");
	const flag = options.args.indexOf("--parent-session-id");
	expect(flag).toBeGreaterThan(-1);
	expect(options.args[flag + 1]).toBe("parent-123");
});

test("resumed remote launch never rewrites the existing header with lineage", () => {
	const options = launchOptions({ cwd: "/tmp", session_id: "child-1", parent_session_id: "parent-123" }, "/home/test");
	expect(options.args).toContain("-r");
	expect(options.args).not.toContain("--parent-session-id");
});

test("continued remote launch never rewrites the existing header with lineage", () => {
	const options = launchOptions({ cwd: "/tmp", continue: true, parent_session_id: "parent-123" }, "/home/test");
	expect(options.args).toContain("-c");
	expect(options.args).not.toContain("--parent-session-id");
});

test("rejects a non-string parent_session_id", () => {
	expect(() => launchOptions({ cwd: "/tmp", parent_session_id: 7 }, "/home/test")).toThrow("Invalid parent_session_id");
});

test("probe accepts lineage only from an omp that knows the flag", () => {
	const dir = fs.mkdtempSync(path.join(os.tmpdir(), "omp-probe-"));
	const old = path.join(dir, "old");
	const current = path.join(dir, "new");
	fs.writeFileSync(old, "#!/bin/sh\necho 18.12.5\n", { mode: 0o755 });
	fs.writeFileSync(current, "#!/bin/sh\necho 'Error: --parent-session-id requires a raw nonempty OMP session id.' >&2\nexit 2\n", { mode: 0o755 });
	expect(supportsParentSessionId(old)).toBe(false);
	expect(supportsParentSessionId(current)).toBe(true);
	expect(withoutParentSessionId(["--mode=rpc", "--parent-session-id", "p", "--max-time=1m"])).toEqual(["--mode=rpc", "--max-time=1m"]);
});
