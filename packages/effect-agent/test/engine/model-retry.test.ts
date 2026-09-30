import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { ThreadHistory } from "effect-agent/thread-history";
import { TestClock } from "effect/testing";
import { AiError, LanguageModel, Model, type Response, Toolkit } from "effect/unstable/ai";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() =>
    Schema.decodeSync(ThreadId)(`model-retry-thread-${++threadSequence}`),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("model-retry-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("model-retry-turn")),
});

const usage = { inputTokens: {}, outputTokens: {} };
const metadata: Response.StreamPartEncoded = { type: "response-metadata", id: "resp-1" };

const rateLimitedPart: Response.StreamPartEncoded = {
  type: "error",
  error: { code: 429, message: "model is temporarily rate-limited upstream" },
};

const answer: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

type Attempt = ReadonlyArray<Response.StreamPartEncoded> | AiError.AiError;

const runScripted = (attempts: ReadonlyArray<Attempt>, modelRetries?: number) =>
  Effect.gen(function* () {
    let calls = 0;

    const model = Model.make(
      "scripted",
      "model-retry",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => {
            const attempt = attempts[Math.min(calls++, attempts.length - 1)]!;

            return AiError.isAiError(attempt) ? Stream.fail(attempt) : Stream.fromIterable(attempt);
          },
        }),
      ),
    );

    const agent = Agent.withModel(
      Agent.make("model-retry", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer.",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "10 minutes",
          toolConcurrency: 1,
          ...(modelRetries === undefined ? {} : { modelRetries }),
        }),
      }),
      model,
    );

    const fiber = yield* Effect.forkChild(Stream.runDrain(AgentRuntime.stream(agent, "begin")));

    // Backoff sleeps start at different points in the run; advance until it settles.
    yield* TestClock.adjust("1 second").pipe(
      Effect.repeat({ until: () => fiber.pollUnsafe() !== undefined }),
    );
    const exit = yield* Fiber.await(fiber);

    return { exit, calls };
  });

const failureTag = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;

layer(Layer.mergeAll(identifiers, ThreadHistory.layer))("model call retries", (it) => {
  it.effect("retries a rate-limited error part that arrives before any content", () =>
    Effect.gen(function* () {
      const { exit, calls } = yield* runScripted([[metadata, rateLimitedPart], answer], 2);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(calls).toBe(2);
    }),
  );

  it.effect("retries a retryable provider failure", () =>
    Effect.gen(function* () {
      const rateLimited = AiError.make({
        module: "scripted",
        method: "streamText",
        reason: new AiError.RateLimitError({}),
      });

      const { exit, calls } = yield* runScripted([rateLimited, rateLimited, answer], 2);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(calls).toBe(3);
    }),
  );

  it.effect("fails unchanged when retries are off or exhausted", () =>
    Effect.gen(function* () {
      const off = yield* runScripted([[metadata, rateLimitedPart], answer]);
      const exhausted = yield* runScripted([[metadata, rateLimitedPart]], 1);

      expect(off.calls).toBe(1);
      expect(failureTag(off.exit)).toMatchObject({ _tag: "ModelProtocolError" });
      expect(exhausted.calls).toBe(2);
      expect(failureTag(exhausted.exit)).toMatchObject({ _tag: "ModelProtocolError" });
    }),
  );

  it.effect("does not retry once content has streamed", () =>
    Effect.gen(function* () {
      const { exit, calls } = yield* runScripted(
        [[metadata, { type: "text-start", id: "answer" }, rateLimitedPart], answer],
        2,
      );

      expect(calls).toBe(1);
      expect(failureTag(exit)).toMatchObject({ _tag: "ModelProtocolError" });
    }),
  );
});
