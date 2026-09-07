/** Optional integration probe for a locally authenticated OpenCode install. */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { ChildProcessSpawner } from "effect/unstable/process";
import { describe, expect } from "vite-plus/test";

import { makeOpenCodeAcpRuntime } from "./OpenCodeAcpSupport.ts";

describe.runIf(process.env.T3_OPENCODE_ACP_PROBE === "1")("OpenCode ACP CLI probe", () => {
  it.effect("creates a session through the locally authenticated OpenCode CLI", () =>
    Effect.gen(function* () {
      const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const runtime = yield* makeOpenCodeAcpRuntime({
        openCodeSettings: { binaryPath: "/home/ashutosh/.opencode/bin/opencode" },
        environment: process.env,
        childProcessSpawner,
        cwd: process.cwd(),
        clientInfo: { name: "akeru-opencode-probe", version: "0.0.0" },
      });
      const started = yield* runtime.start();
      expect(started.sessionId).toMatch(/^ses_/);
      expect(
        started.sessionSetupResult.configOptions?.some((option) => option.category === "model"),
      ).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
