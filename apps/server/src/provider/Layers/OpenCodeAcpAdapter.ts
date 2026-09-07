/** Native OpenCode CLI adapter (`opencode acp`). */

import {
  ApprovalRequestId,
  type OpenCodeSettings,
  EventId,
  type ProviderApprovalDecision,
  type ProviderInteractionMode,
  type ProviderRuntimeEvent,
  type ProviderSession,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as EffectAcpErrors from "effect-acp/errors";
import type * as EffectAcpSchema from "effect-acp/schema";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import * as McpProviderSession from "../../mcp/McpProviderSession.ts";
import { toAcpMcpServers } from "../McpServerConfig.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import { mapAcpToAdapterError, acpPermissionOutcome } from "../acp/AcpAdapterSupport.ts";
import type * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import {
  makeAcpAssistantItemEvent,
  makeAcpContentDeltaEvent,
  makeAcpPlanUpdatedEvent,
  makeAcpRequestOpenedEvent,
  makeAcpRequestResolvedEvent,
  makeAcpToolCallEvent,
} from "../acp/AcpCoreRuntimeEvents.ts";
import {
  decideToolCallUpdateEmission,
  parsePermissionRequest,
  type AcpSessionModeState,
  type AcpToolCallState,
} from "../acp/AcpRuntimeModel.ts";
import { makeOpenCodeAcpRuntime } from "../acp/OpenCodeAcpSupport.ts";
import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";
import type { OpenCodeAdapterShape } from "../Services/OpenCodeAdapter.ts";

const PROVIDER = ProviderDriverKind.make("opencode");
const OPENCODE_RESUME_VERSION = 1 as const;

export interface OpenCodeAcpAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: EventNdjsonLogger;
  readonly instanceId?: ProviderInstanceId;
}

interface PendingApproval {
  readonly decision: Deferred.Deferred<ProviderApprovalDecision>;
}

interface OpenCodeAcpSessionContext {
  readonly threadId: ThreadId;
  session: ProviderSession;
  readonly scope: Scope.Closeable;
  readonly acp: AcpSessionRuntime.AcpSessionRuntime["Service"];
  notificationFiber: Fiber.Fiber<void, never> | undefined;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly toolCalls: Map<string, AcpToolCallState>;
  activeTurnId: TurnId | undefined;
  promptsInFlight: number;
  lastPlanFingerprint: string | undefined;
  stopped: boolean;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseOpenCodeResume(raw: unknown): { readonly sessionId: string } | undefined {
  if (!isRecord(raw) || raw.schemaVersion !== OPENCODE_RESUME_VERSION) return undefined;
  return typeof raw.sessionId === "string" && raw.sessionId.trim()
    ? { sessionId: raw.sessionId.trim() }
    : undefined;
}

function setupModelId(
  setup:
    | EffectAcpSchema.LoadSessionResponse
    | EffectAcpSchema.NewSessionResponse
    | EffectAcpSchema.ResumeSessionResponse,
): string | undefined {
  const option = setup.configOptions?.find((entry) => entry.category === "model");
  return typeof option?.currentValue === "string" ? option.currentValue : undefined;
}

function normalizeModeText(mode: { readonly id: string; readonly name: string }): string {
  return `${mode.id} ${mode.name}`.toLowerCase().replace(/[^a-z0-9]+/g, " ");
}

function requestedModeId(
  modeState: AcpSessionModeState | undefined,
  interactionMode: ProviderInteractionMode | undefined,
): string | undefined {
  if (!modeState) return undefined;
  const aliases = interactionMode === "plan" ? ["plan"] : ["build", "code", "agent"];
  for (const alias of aliases) {
    const exact = modeState.availableModes.find(
      (mode) => mode.id.toLowerCase() === alias || mode.name.toLowerCase() === alias,
    );
    if (exact) return exact.id;
  }
  return modeState.availableModes.find((mode) => normalizeModeText(mode).includes(aliases[0]!))?.id;
}

function selectPermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
  decision: Exclude<ProviderApprovalDecision, "cancel">,
): string | undefined {
  const kind =
    decision === "acceptForSession" || decision === "acceptAlways"
      ? "allow_always"
      : decision === "accept"
        ? "allow_once"
        : "reject_once";
  return request.options.find((option) => option.kind === kind)?.optionId;
}

function autoPermissionOption(
  request: EffectAcpSchema.RequestPermissionRequest,
): string | undefined {
  return (
    request.options.find((option) => option.kind === "allow_always")?.optionId ??
    request.options.find((option) => option.kind === "allow_once")?.optionId
  );
}

function encodeDiagnostic(value: unknown): string {
  try {
    return JSON.stringify(value).slice(0, 2_000);
  } catch {
    return "[unserializable ACP request]";
  }
}

export function makeOpenCodeAcpAdapter(
  openCodeSettings: OpenCodeSettings,
  options?: OpenCodeAcpAdapterLiveOptions,
) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("opencode");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* ServerConfig;
    const crypto = yield* Crypto.Crypto;
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, OpenCodeAcpSessionContext>();
    const locksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate OpenCode runtime identifier.",
            cause,
          }),
      ),
    );
    const makeEventStamp = () =>
      Effect.all({
        eventId: Effect.map(randomUUIDv4, (id) => EventId.make(id)),
        createdAt: nowIso,
      });
    const offer = (event: ProviderRuntimeEvent) => PubSub.publish(runtimeEventPubSub, event);
    const logNative = (threadId: ThreadId, method: string, payload: unknown) =>
      Effect.gen(function* () {
        if (!options?.nativeEventLogger) return;
        const observedAt = yield* nowIso;
        yield* options.nativeEventLogger.write(
          {
            observedAt,
            event: {
              id: yield* randomUUIDv4,
              kind: "notification",
              provider: PROVIDER,
              createdAt: observedAt,
              method,
              threadId,
              payload,
            },
          },
          threadId,
        );
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to write native OpenCode notification log.", {
            cause,
            threadId,
            method,
          }),
        ),
      );

    const withThreadLock = <A, E, R>(
      threadId: ThreadId,
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R> =>
      SynchronizedRef.modifyEffect(locksRef, (current) => {
        const existing = current.get(String(threadId));
        if (existing) return Effect.succeed([existing, current] as const);
        return Semaphore.make(1).pipe(
          Effect.map((semaphore) => {
            const next = new Map(current);
            next.set(String(threadId), semaphore);
            return [semaphore, next] as const;
          }),
        );
      }).pipe(Effect.flatMap((semaphore) => semaphore.withPermit(effect)));

    const requireSession = (
      threadId: ThreadId,
    ): Effect.Effect<OpenCodeAcpSessionContext, ProviderAdapterSessionNotFoundError> => {
      const context = sessions.get(threadId);
      return context && !context.stopped
        ? Effect.succeed(context)
        : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId }));
    };

    const settleApprovals = (context: OpenCodeAcpSessionContext) =>
      Effect.forEach(
        Array.from(context.pendingApprovals.values()),
        (pending) => Deferred.succeed(pending.decision, "cancel").pipe(Effect.ignore),
        { discard: true },
      );

    const stopInternal = (context: OpenCodeAcpSessionContext) =>
      Effect.gen(function* () {
        if (context.stopped) return;
        context.stopped = true;
        yield* settleApprovals(context);
        if (context.notificationFiber) yield* Fiber.interrupt(context.notificationFiber);
        yield* Scope.close(context.scope, Exit.void);
        sessions.delete(context.threadId);
        yield* offer({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: context.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const startSession: OpenCodeAdapterShape["startSession"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          if (input.provider !== undefined && input.provider !== PROVIDER) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
            });
          }
          if (!input.cwd?.trim()) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "startSession",
              issue: "cwd is required and must be non-empty.",
            });
          }
          const cwd = path.resolve(input.cwd.trim());
          const existing = sessions.get(input.threadId);
          if (existing) yield* stopInternal(existing);

          const scope = yield* Scope.make("sequential");
          let transferred = false;
          yield* Effect.addFinalizer(() =>
            transferred ? Effect.void : Scope.close(scope, Exit.void),
          );
          const pendingApprovals = new Map<ApprovalRequestId, PendingApproval>();
          const resumeSessionId = parseOpenCodeResume(input.resumeCursor)?.sessionId;
          const mcpSession = McpProviderSession.readMcpProviderSession(input.threadId);
          const mcpServers = [
            ...toAcpMcpServers(input.mcpServers ?? []),
            ...(mcpSession
              ? [
                  {
                    type: "http" as const,
                    name: "t3-code",
                    url: mcpSession.endpoint,
                    headers: [{ name: "Authorization", value: mcpSession.authorizationHeader }],
                  },
                ]
              : []),
          ];
          let context!: OpenCodeAcpSessionContext;
          const acp = yield* makeOpenCodeAcpRuntime({
            openCodeSettings,
            ...(options?.environment ? { environment: options.environment } : {}),
            childProcessSpawner,
            cwd,
            ...(resumeSessionId ? { resumeSessionId } : {}),
            clientInfo: { name: "t3-code", version: "0.0.0" },
            ...(mcpServers.length > 0 ? { mcpServers } : {}),
          }).pipe(
            Effect.provideService(Crypto.Crypto, crypto),
            Effect.provideService(Scope.Scope, scope),
            Effect.mapError(
              (cause) =>
                new ProviderAdapterProcessError({
                  provider: PROVIDER,
                  threadId: input.threadId,
                  detail: cause.message,
                  cause,
                }),
            ),
          );

          const started = yield* Effect.gen(function* () {
            yield* acp.handleRequestPermission((params) =>
              Effect.gen(function* () {
                yield* logNative(input.threadId, "session/request_permission", params);
                if (input.runtimeMode === "full-access") {
                  const optionId = autoPermissionOption(params);
                  if (optionId) {
                    return { outcome: { outcome: "selected" as const, optionId } };
                  }
                }
                const permissionRequest = parsePermissionRequest(params);
                const requestId = ApprovalRequestId.make(yield* randomUUIDv4);
                const runtimeRequestId = RuntimeRequestId.make(requestId);
                const decision = yield* Deferred.make<ProviderApprovalDecision>();
                pendingApprovals.set(requestId, { decision });
                yield* offer(
                  makeAcpRequestOpenedEvent({
                    stamp: yield* makeEventStamp(),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: context?.activeTurnId,
                    requestId: runtimeRequestId,
                    permissionRequest,
                    detail: permissionRequest.detail ?? encodeDiagnostic(params),
                    source: "acp.jsonrpc",
                    method: "session/request_permission",
                    rawPayload: params,
                  }),
                );
                const resolved = yield* Deferred.await(decision);
                pendingApprovals.delete(requestId);
                yield* offer(
                  makeAcpRequestResolvedEvent({
                    stamp: yield* makeEventStamp(),
                    provider: PROVIDER,
                    threadId: input.threadId,
                    turnId: context?.activeTurnId,
                    requestId: runtimeRequestId,
                    permissionRequest,
                    decision: resolved,
                  }),
                );
                const optionId =
                  resolved === "cancel" ? undefined : selectPermissionOption(params, resolved);
                return optionId
                  ? { outcome: { outcome: "selected" as const, optionId } }
                  : ({ outcome: { outcome: "cancelled" as const } } as const);
              }).pipe(
                Effect.mapError(
                  (cause) =>
                    new EffectAcpErrors.AcpTransportError({
                      detail: "Failed to handle OpenCode ACP permission request.",
                      cause,
                    }),
                ),
              ),
            );
            return yield* acp.start();
          }).pipe(
            Effect.mapError((cause) =>
              mapAcpToAdapterError(PROVIDER, input.threadId, "session/start", cause),
            ),
          );

          const selectedModel =
            input.modelSelection?.instanceId === boundInstanceId
              ? input.modelSelection.model
              : setupModelId(started.sessionSetupResult);
          if (selectedModel) {
            yield* acp
              .setModel(selectedModel)
              .pipe(
                Effect.mapError((cause) =>
                  mapAcpToAdapterError(
                    PROVIDER,
                    input.threadId,
                    "session/set_config_option",
                    cause,
                  ),
                ),
              );
          }
          const createdAt = yield* nowIso;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: boundInstanceId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd,
            ...(selectedModel ? { model: selectedModel } : {}),
            threadId: input.threadId,
            resumeCursor: { schemaVersion: OPENCODE_RESUME_VERSION, sessionId: started.sessionId },
            createdAt,
            updatedAt: createdAt,
          };
          context = {
            threadId: input.threadId,
            session,
            scope,
            acp,
            notificationFiber: undefined,
            pendingApprovals,
            turns: [],
            toolCalls: new Map(),
            activeTurnId: undefined,
            promptsInFlight: 0,
            lastPlanFingerprint: undefined,
            stopped: false,
          };

          const notificationFiber = yield* Stream.runDrain(
            Stream.mapEffect(acp.getEvents(), (event) =>
              Effect.gen(function* () {
                if (event._tag === "EventStreamBarrier") {
                  yield* Deferred.succeed(event.acknowledge, undefined);
                  return;
                }
                switch (event._tag) {
                  case "ModeChanged":
                    return;
                  case "AssistantItemStarted":
                    yield* offer(
                      makeAcpAssistantItemEvent({
                        stamp: yield* makeEventStamp(),
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId: context.activeTurnId,
                        itemId: event.itemId,
                        lifecycle: "item.started",
                      }),
                    );
                    return;
                  case "AssistantItemCompleted":
                    yield* offer(
                      makeAcpAssistantItemEvent({
                        stamp: yield* makeEventStamp(),
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId: context.activeTurnId,
                        itemId: event.itemId,
                        lifecycle: "item.completed",
                      }),
                    );
                    return;
                  case "PlanUpdated": {
                    yield* logNative(context.threadId, "session/update", event.rawPayload);
                    const fingerprint = event.payload.plan
                      .map((step) => `${step.status}:${step.step}`)
                      .join("\u001f");
                    if (fingerprint === context.lastPlanFingerprint) return;
                    context.lastPlanFingerprint = fingerprint;
                    yield* offer(
                      makeAcpPlanUpdatedEvent({
                        stamp: yield* makeEventStamp(),
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId: context.activeTurnId,
                        payload: event.payload,
                        source: "acp.jsonrpc",
                        method: "session/update",
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                  }
                  case "ToolCallUpdated": {
                    yield* logNative(context.threadId, "session/update", event.rawPayload);
                    const previous = context.toolCalls.get(event.toolCall.toolCallId);
                    const decision = decideToolCallUpdateEmission({
                      previous,
                      next: event.toolCall,
                      lastEmittedDetailLength: previous?.detail?.length,
                      skippedSinceEmit: 0,
                    });
                    context.toolCalls.set(event.toolCall.toolCallId, event.toolCall);
                    if (!decision.emit) return;
                    yield* offer(
                      makeAcpToolCallEvent({
                        stamp: yield* makeEventStamp(),
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId: context.activeTurnId,
                        toolCall: event.toolCall,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                  }
                  case "ContentDelta":
                    yield* logNative(context.threadId, "session/update", event.rawPayload);
                    yield* offer(
                      makeAcpContentDeltaEvent({
                        stamp: yield* makeEventStamp(),
                        provider: PROVIDER,
                        threadId: context.threadId,
                        turnId: context.activeTurnId,
                        ...(event.itemId ? { itemId: event.itemId } : {}),
                        text: event.text,
                        rawPayload: event.rawPayload,
                      }),
                    );
                    return;
                }
              }),
            ),
          ).pipe(
            Effect.catch((cause) =>
              Effect.logError("Failed to process OpenCode ACP event.", { cause }),
            ),
            Effect.forkIn(scope),
          );
          context.notificationFiber = notificationFiber;
          sessions.set(input.threadId, context);
          transferred = true;
          yield* offer({
            type: "session.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { resume: started.initializeResult },
          });
          yield* offer({
            type: "session.state.changed",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { state: "ready", reason: "OpenCode ACP session ready" },
          });
          yield* offer({
            type: "thread.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            payload: { providerThreadId: started.sessionId },
          });
          return session;
        }).pipe(Effect.scoped),
      );

    const sendTurn: OpenCodeAdapterShape["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const context = yield* requireSession(input.threadId);
        const turnId = context.activeTurnId ?? TurnId.make(yield* randomUUIDv4);
        const modelSelection =
          input.modelSelection?.instanceId === boundInstanceId ? input.modelSelection : undefined;
        if (input.modelSelection && !modelSelection) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: `OpenCode model selection is bound to another provider instance.`,
          });
        }
        const text = input.input?.trim();
        const prompt: Array<EffectAcpSchema.ContentBlock> = [];
        if (text) prompt.push({ type: "text", text });
        if (input.attachments) {
          for (const attachment of input.attachments) {
            if (attachment.type !== "image") continue;
            const attachmentPath = resolveAttachmentPath({
              attachmentsDir: serverConfig.attachmentsDir,
              attachment,
            });
            if (!attachmentPath) {
              return yield* new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "session/prompt",
                detail: `Invalid attachment id '${attachment.id}'.`,
              });
            }
            const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: PROVIDER,
                    method: "session/prompt",
                    detail: cause.message,
                    cause,
                  }),
              ),
            );
            prompt.push({
              type: "image",
              data: Buffer.from(bytes).toString("base64"),
              mimeType: attachment.mimeType,
            });
          }
        }
        if (prompt.length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Turn requires non-empty text or image attachments.",
          });
        }
        if (modelSelection?.model) {
          yield* context.acp
            .setModel(modelSelection.model)
            .pipe(
              Effect.mapError((cause) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/set_config_option", cause),
              ),
            );
        }
        const modeId = requestedModeId(yield* context.acp.getModeState, input.interactionMode);
        if (modeId) {
          yield* context.acp
            .setMode(modeId)
            .pipe(
              Effect.mapError((cause) =>
                mapAcpToAdapterError(PROVIDER, input.threadId, "session/set_config_option", cause),
              ),
            );
        }
        const steering = context.promptsInFlight > 0;
        context.promptsInFlight += 1;
        context.activeTurnId = turnId;
        context.session = {
          ...context.session,
          status: "running",
          activeTurnId: turnId,
          ...(modelSelection?.model ? { model: modelSelection.model } : {}),
          updatedAt: yield* nowIso,
        };
        if (!steering) {
          context.lastPlanFingerprint = undefined;
          yield* offer({
            type: "turn.started",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: input.threadId,
            turnId,
            payload: { model: context.session.model },
          });
        }
        return yield* context.acp.prompt({ prompt }).pipe(
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, input.threadId, "session/prompt", cause),
          ),
          Effect.tap((result) =>
            Effect.gen(function* () {
              context.turns.push({ id: turnId, items: [{ prompt, result }] });
              context.promptsInFlight = Math.max(0, context.promptsInFlight - 1);
              if (context.promptsInFlight === 0) {
                const updatedAt = yield* nowIso;
                const { activeTurnId: _, ...ready } = context.session;
                context.session = { ...ready, status: "ready", updatedAt };
                context.activeTurnId = undefined;
                yield* offer({
                  type: "turn.completed",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: input.threadId,
                  turnId,
                  payload: {
                    state: result.stopReason === "cancelled" ? "cancelled" : "completed",
                    stopReason: result.stopReason ?? null,
                  },
                });
              }
            }),
          ),
          Effect.tapError((cause) =>
            Effect.gen(function* () {
              context.promptsInFlight = Math.max(0, context.promptsInFlight - 1);
              if (context.promptsInFlight !== 0) return;
              const updatedAt = yield* nowIso;
              const { activeTurnId: _, ...ready } = context.session;
              context.session = { ...ready, status: "ready", updatedAt };
              context.activeTurnId = undefined;
              yield* offer({
                type: "turn.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: input.threadId,
                turnId,
                payload: {
                  state: "failed",
                  errorMessage: cause.message,
                },
              });
            }),
          ),
          Effect.map(() => ({
            threadId: input.threadId,
            turnId,
            resumeCursor: context.session.resumeCursor,
          })),
        );
      });

    const interruptTurn: OpenCodeAdapterShape["interruptTurn"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        yield* settleApprovals(context);
        yield* context.acp.cancel.pipe(
          Effect.mapError((cause) =>
            mapAcpToAdapterError(PROVIDER, threadId, "session/cancel", cause),
          ),
          Effect.ignore,
        );
      });

    const respondToRequest: OpenCodeAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        const pending = context.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "session/request_permission",
            detail: `Unknown pending approval request: ${requestId}`,
          });
        }
        yield* Deferred.succeed(pending.decision, decision);
      });

    const respondToUserInput: OpenCodeAdapterShape["respondToUserInput"] = (threadId, requestId) =>
      Effect.gen(function* () {
        yield* requireSession(threadId);
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "session/elicitation",
          detail: `OpenCode ACP did not issue a user-input request (${requestId}).`,
        });
      });

    const readThread: OpenCodeAdapterShape["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        return { threadId, turns: context.turns };
      });

    const rollbackThread: OpenCodeAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      Effect.gen(function* () {
        const context = yield* requireSession(threadId);
        if (!Number.isInteger(numTurns) || numTurns < 1) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          });
        }
        context.turns.splice(Math.max(0, context.turns.length - numTurns));
        return { threadId, turns: context.turns };
      });

    const stopSession: OpenCodeAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          yield* stopInternal(yield* requireSession(threadId));
        }),
      );
    const listSessions: OpenCodeAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values(), (context) => ({ ...context.session })));
    const hasSession: OpenCodeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const context = sessions.get(threadId);
        return context !== undefined && !context.stopped;
      });
    const stopAll: OpenCodeAdapterShape["stopAll"] = () =>
      Effect.forEach(sessions.values(), stopInternal, { discard: true });

    yield* Effect.addFinalizer(() =>
      stopAll().pipe(
        Effect.catch((cause) =>
          Effect.logError("Failed to stop OpenCode ACP sessions.", { cause }),
        ),
        Effect.tap(() => PubSub.shutdown(runtimeEventPubSub)),
      ),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies OpenCodeAdapterShape;
  });
}
