import { expect, it } from "@effect/vitest";

import { buildOpenCodeAcpSpawnInput } from "./OpenCodeAcpSupport.ts";

it("builds the native OpenCode ACP command in the requested workspace", () => {
  expect(
    buildOpenCodeAcpSpawnInput({ binaryPath: "/custom/opencode" }, "/tmp/akeru-bot", {
      OPENCODE_TEST: "1",
    }),
  ).toEqual({
    command: "/custom/opencode",
    args: ["acp", "--cwd", "/tmp/akeru-bot"],
    cwd: "/tmp/akeru-bot",
    env: { OPENCODE_TEST: "1" },
  });
});
