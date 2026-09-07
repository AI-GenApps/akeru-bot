import type {
  AkeruRuntimeToolId,
  AkeruToolExecution,
  AkeruToolRuntime,
  AkeruRuntimeToolDefinition,
} from "../provider/AkeruToolRuntime.ts";
import type { ProviderApprovalDecision } from "@t3tools/contracts";

/**
 * Narrow bridge exposed by AgentController to the HTTP MCP transport.
 *
 * Keeping this contract at the MCP boundary prevents the route from knowing
 * how a provider session is implemented (Mastra, Codex app-server, or ACP).
 */
export interface AkeruMcpToolSession {
  readonly toolsForThread: () => ReadonlyArray<AkeruRuntimeToolDefinition>;
  readonly requiresApproval: (toolId: AkeruRuntimeToolId, input: unknown) => Promise<boolean>;
  readonly requestApproval: (input: {
    readonly toolCallId: string;
    readonly toolId: AkeruRuntimeToolId;
    readonly input: unknown;
  }) => Promise<ProviderApprovalDecision>;
  readonly grantApproval: (input: {
    readonly toolCallId: string;
    readonly toolId: AkeruRuntimeToolId;
    readonly input: unknown;
  }) => void;
  readonly execute: (input: Omit<AkeruToolExecution, "threadId">) => Promise<unknown>;
}

/** Build the bridge from the shared runtime without leaking its session map. */
export function makeAkeruMcpToolSession(input: {
  readonly threadId: string;
  readonly runtime: AkeruToolRuntime;
  readonly requestApproval: AkeruMcpToolSession["requestApproval"];
}): AkeruMcpToolSession {
  return {
    toolsForThread: () => input.runtime.toolsForThread(input.threadId),
    requiresApproval: (toolId, toolInput) =>
      input.runtime.requiresApproval(input.threadId, toolId, toolInput),
    requestApproval: input.requestApproval,
    grantApproval: ({ toolCallId, toolId, input: toolInput }) =>
      input.runtime.grantApproval({
        threadId: input.threadId,
        toolCallId,
        toolId,
        input: toolInput,
      }),
    execute: ({ toolId, toolCallId, input: toolInput, approvalMode }) =>
      input.runtime.execute({
        threadId: input.threadId,
        toolId,
        toolCallId,
        input: toolInput,
        approvalMode,
      }),
  };
}
