import * as NodeAssert from "node:assert/strict";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import type * as EffectAcpSchema from "effect-acp/schema";

import {
  OpenCodeSettings,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import * as AcpSessionRuntime from "../acp/AcpSessionRuntime.ts";
import { makeOpenCodeAcpAdapter } from "./OpenCodeAcpAdapter.ts";

const decodeOpenCodeSettings = Schema.decodeSync(OpenCodeSettings);
const testLayer = ServerConfig.layerTest(process.cwd(), {
  prefix: "akeru-opencode-acp-adapter-test-",
}).pipe(Layer.provideMerge(NodeServices.layer));

type FakeAcpRuntime = AcpSessionRuntime.AcpSessionRuntime["Service"];
type FakeAcpEvent = AcpSessionRuntime.AcpSessionRuntimeEvent;

interface FakeAcpState {
  readonly events: Queue.Queue<FakeAcpEvent>;
  readonly drainStarted: Deferred.Deferred<void>;
  readonly releaseDrain: Deferred.Deferred<void>;
  readonly contentObserved: Deferred.Deferred<void>;
}

const makeFakeAcpRuntime = (state: FakeAcpState) => () =>
  Effect.succeed({
    handleRequestPermission: () => Effect.void,
    handleElicitation: () => Effect.void,
    handleReadTextFile: () => Effect.void,
    handleWriteTextFile: () => Effect.void,
    handleCreateTerminal: () => Effect.void,
    handleTerminalOutput: () => Effect.void,
    handleTerminalWaitForExit: () => Effect.void,
    handleTerminalKill: () => Effect.void,
    handleTerminalRelease: () => Effect.void,
    handleSessionUpdate: () => Effect.void,
    handleElicitationComplete: () => Effect.void,
    handleUnknownExtRequest: () => Effect.void,
    handleUnknownExtNotification: () => Effect.void,
    handleExtRequest: () => Effect.void,
    handleExtNotification: () => Effect.void,
    start: () =>
      Effect.succeed({
        sessionId: "opencode-acp-test-session",
        initializeResult: {} as EffectAcpSchema.InitializeResponse,
        sessionSetupResult: {
          sessionId: "opencode-acp-test-session",
          configOptions: [],
        } as EffectAcpSchema.NewSessionResponse,
        modelConfigId: undefined,
      }),
    getEvents: () => Stream.fromQueue(state.events),
    drainEvents: Effect.gen(function* () {
      yield* Deferred.succeed(state.drainStarted, undefined);
      yield* Deferred.await(state.releaseDrain);
    }),
    getModeState: Effect.succeed(undefined),
    getConfigOptions: Effect.succeed([]),
    prompt: () =>
      Queue.offer(state.events, {
        _tag: "ContentDelta",
        itemId: "assistant-item",
        text: "hello from OpenCode",
        rawPayload: {},
      }).pipe(Effect.as({ stopReason: "end_turn" })),
    cancel: Effect.void,
    setMode: () => Effect.succeed({}),
    setConfigOption: () => Effect.succeed({}),
    setModel: () => Effect.void,
    setSessionModel: () => Effect.succeed({}),
    request: () => Effect.succeed(undefined),
    notify: () => Effect.void,
  } as unknown as FakeAcpRuntime);

it.layer(testLayer)("OpenCodeAcpAdapter", (it) => {
  it.effect("drains ACP updates before publishing turn.completed", () =>
    Effect.gen(function* () {
      const state: FakeAcpState = {
        events: yield* Queue.unbounded<FakeAcpEvent>(),
        drainStarted: yield* Deferred.make<void>(),
        releaseDrain: yield* Deferred.make<void>(),
        contentObserved: yield* Deferred.make<void>(),
      };
      const threadId = ThreadId.make("opencode-acp-drain-thread");
      const provider = ProviderDriverKind.make("opencode");
      const instanceId = ProviderInstanceId.make("opencode");
      const adapter = yield* makeOpenCodeAcpAdapter(
        decodeOpenCodeSettings({ binaryPath: "opencode" }),
        {
          instanceId,
          makeAcpRuntime: makeFakeAcpRuntime(state),
        },
      ).pipe(Effect.orDie);
      const events: ProviderRuntimeEvent[] = [];
      const turnCompleted = yield* Deferred.make<void>();
      const runtimeEventsFiber = yield* Stream.runForEach(adapter.streamEvents, (event) =>
        Effect.sync(() => {
          events.push(event);
        }).pipe(
          Effect.andThen(
            event.type === "content.delta"
              ? Deferred.succeed(state.contentObserved, undefined)
              : event.type === "turn.completed"
                ? Deferred.succeed(turnCompleted, undefined)
                : Effect.void,
          ),
        ),
      ).pipe(Effect.forkChild);

      yield* adapter.startSession({
        threadId,
        provider,
        providerInstanceId: instanceId,
        cwd: process.cwd(),
        runtimeMode: "full-access",
        modelSelection: { instanceId, model: "opencode-test-model" },
      });

      const sendTurnFiber = yield* adapter
        .sendTurn({ threadId, input: "hello", attachments: [] })
        .pipe(Effect.forkChild);

      yield* Deferred.await(state.drainStarted).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(state.contentObserved).pipe(Effect.timeout("2 seconds"));
      NodeAssert.equal(
        events.some(
          (event) => event.type === "turn.completed" && String(event.threadId) === String(threadId),
        ),
        false,
      );

      yield* Deferred.succeed(state.releaseDrain, undefined);
      yield* Fiber.join(sendTurnFiber).pipe(Effect.timeout("2 seconds"));
      yield* Deferred.await(turnCompleted).pipe(Effect.timeout("2 seconds"));

      const contentIndex = events.findIndex(
        (event) => event.type === "content.delta" && String(event.threadId) === String(threadId),
      );
      const completedIndex = events.findIndex(
        (event) => event.type === "turn.completed" && String(event.threadId) === String(threadId),
      );
      NodeAssert.ok(contentIndex >= 0);
      NodeAssert.ok(completedIndex > contentIndex);

      yield* Fiber.interrupt(runtimeEventsFiber);
      yield* adapter.stopSession(threadId);
    }),
  );
});
