import { layer, describe, expect } from "@effect/vitest";

import { Context, Deferred, Effect, Fiber, Layer, Ref, Stream } from "effect";

import { LogicalReplication, PgoutputV1, PgoutputPlugin, Client } from "../../src";
import { StreamError } from "../../src/errors";

import { makeTestClient } from "../support/test-client.js";

const slotName = "slot_acknowledge";
const decoderName = "pgoutput";
const publicationName = "acknowledge_test_pub";

class AcknowledgeReplication extends Context.Service<
  AcknowledgeReplication,
  LogicalReplication.LogicalReplicationService<PgoutputV1.LogicalReplicationMessageV1>
>()("test/AcknowledgeReplication") {}

const TestClient = makeTestClient({ slotName, decoderName, publicationName });

const makeStageLayer = () => {
  const pgoutputPlugin = PgoutputPlugin.make({
    protoVersion: 1,
    publicationNames: [publicationName],
  });

  return LogicalReplication.layer(AcknowledgeReplication, pgoutputPlugin, {
    acknowledge: { auto: false },
  }).pipe(Layer.provide(Client.layerFromConfig()));
};

describe("acknowledge", () => {
  layer(TestClient.layer, { excludeTestServices: true })((it) => {
    it.effect(
      "resumes streaming from the lsn returned by a stopped subscription, and replays from the beginning when re-subscribing at lsn zero",
      () =>
        Effect.gen(function* () {
          const sql = yield* TestClient;
          const inserted = yield* Ref.make(0);

          const insertFive = () => sql`
          INSERT INTO users(firstname, lastname, email, phone)
          SELECT md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
          FROM generate_series(1, 5) RETURNING *
        `;

          const countInserts = (
            stream: Stream.Stream<PgoutputV1.LogicalReplicationMessageV1, StreamError>,
            onCount: { readonly target: number; readonly deferred: Deferred.Deferred<void> },
          ) =>
            Stream.runForEach(stream, (msg) => {
              if (!PgoutputV1.LogicalReplicationMessageV1.$is("Insert")(msg)) return Effect.void;

              return Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(inserted, (count) => count + 1);

                if (count === onCount.target) {
                  yield* Deferred.complete(onCount.deferred, Effect.void);
                }
              });
            });

          const lastLsn = yield* Effect.gen(function* () {
            const logicalReplication = yield* AcknowledgeReplication;
            const { stream } = yield* logicalReplication.subscribe(slotName);
            const fiveInserted = yield* Deferred.make<void>();
            const fiber = yield* Effect.forkScoped(
              countInserts(stream, { target: 5, deferred: fiveInserted }),
            );

            yield* insertFive();
            yield* Deferred.await(fiveInserted);
            expect(yield* Ref.get(inserted)).toBe(5);

            const lsn = yield* logicalReplication.lastLsn();
            yield* Fiber.interrupt(fiber);
            return lsn;
          }).pipe(Effect.provide(makeStageLayer()));

          yield* Effect.gen(function* () {
            const logicalReplication = yield* AcknowledgeReplication;
            const { stream } = yield* logicalReplication.subscribe(slotName, lastLsn);
            const tenInserted = yield* Deferred.make<void>();
            const fiber = yield* Effect.forkScoped(
              countInserts(stream, { target: 10, deferred: tenInserted }),
            );

            expect(yield* Ref.get(inserted)).toBe(5);

            yield* insertFive();
            yield* Deferred.await(tenInserted);
            expect(yield* Ref.get(inserted)).toBe(10);

            yield* Fiber.interrupt(fiber);
          }).pipe(Effect.provide(makeStageLayer()));

          yield* Effect.gen(function* () {
            const logicalReplication = yield* AcknowledgeReplication;
            const { stream } = yield* logicalReplication.subscribe(slotName);
            const twentyInserted = yield* Deferred.make<void>();
            const fiber = yield* Effect.forkScoped(
              countInserts(stream, { target: 20, deferred: twentyInserted }),
            );

            yield* Deferred.await(twentyInserted);
            expect(yield* Ref.get(inserted)).toBe(20);

            yield* Fiber.interrupt(fiber);
          }).pipe(Effect.provide(makeStageLayer()));
        }),
    );
  });
});
