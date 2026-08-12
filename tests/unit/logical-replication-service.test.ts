import EventEmitter from "node:events";

import { vi } from "vitest";
import { describe, it, expect } from "@effect/vitest";

import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  pipe,
  Result,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";
import { TestClock } from "effect/testing";
import { faker } from "@faker-js/faker";

import { Client, LogicalReplication, OutputPlugin } from "../../src/index.js";
import { Query } from "../../src/client.js";
import { UnknownError } from "../../src/errors/pg/index.js";
import * as StandbyStatusUpdate from "../../src/standby-status-update.js";
import * as Lsn from "../../src/lsn.js";

import { WireMother } from "../support/wire-mother.js";

class TestReplication extends Context.Service<
  TestReplication,
  LogicalReplication.LogicalReplicationService<Buffer, never>
>()("test/Replication") {}

const makeFixture = Effect.gen(function* () {
  const started = yield* Deferred.make<void>();
  const parsed = yield* Deferred.make<void>();

  const eventEmitter = new EventEmitter();
  const on = vi.fn(eventEmitter.on.bind(eventEmitter));
  const off = vi.fn(eventEmitter.off.bind(eventEmitter));
  const sendCopyFromChunk = vi.fn(() => {});

  const mockConnection = Object.assign(eventEmitter, { on, off, sendCopyFromChunk });

  const clientEventEmitter = new EventEmitter();
  const clientOn = vi.fn(clientEventEmitter.on.bind(clientEventEmitter));
  const clientOff = vi.fn(clientEventEmitter.off.bind(clientEventEmitter));

  const mockConnectedClient = Object.assign(clientEventEmitter, {
    on: clientOn,
    off: clientOff,
    connection: mockConnection,
  });

  const MockClient = Layer.succeed(Client.Client, {
    query: (() =>
      Effect.succeed({ command: "", rowCount: 0, oid: 0, rows: [], fields: [] })) as Query,
    connect: () => Effect.succeed(mockConnectedClient as never),
  });

  const start = vi.fn((_c: unknown, _slot: string, _lsn: Lsn.Lsn) =>
    Deferred.complete(started, Effect.void),
  );

  const mockPlugin = OutputPlugin.make<Uint8Array>({
    name: "mock_replication_plugin",
    start,
    parse: Effect.fnUntraced(function* (buf) {
      yield* Deferred.complete(parsed, Effect.void);
      return buf;
    }),
  });

  const TestLayer = LogicalReplication.layer(TestReplication, mockPlugin).pipe(
    Layer.provide(MockClient),
  );

  return { TestLayer, on, off, mockConnection, started, parsed, start };
});

describe("Logival replication service", () => {
  it.effect("delivers a decoded message to the consumer for every copyData frame", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, started, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const done = yield* Deferred.make<void>();
          const onMessage = vi.fn();

          yield* stream.pipe(
            Stream.runForEach((msg) =>
              Effect.gen(function* () {
                onMessage(msg);
                if (onMessage.mock.calls.length === 2) yield* Deferred.done(done, Exit.void);
              }),
            ),
            Effect.forkScoped,
          );
          yield* Deferred.await(started);
          const payload1 = Buffer.from([0x01]);
          const payload2 = Buffer.from([0x01]);

          mockConnection.emit("copyData", { chunk: WireMother.walData({ payload: payload1 }) });
          mockConnection.emit("copyData", { chunk: WireMother.walData({ payload: payload2 }) });

          yield* Deferred.await(done);

          expect(onMessage).nthCalledWith(1, payload1);
          expect(onMessage).nthCalledWith(2, payload2);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("registers the connection listeners when the stream is consumed", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, on, started }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          yield* stream.pipe(Stream.runDrain, Effect.forkScoped);

          yield* Deferred.await(started);

          expect(on).nthCalledWith(1, "replicationStart", expect.any(Function));
          expect(on).nthCalledWith(2, "copyData", expect.any(Function));
          expect(on).nthCalledWith(3, "error", expect.any(Function));
          expect(on).nthCalledWith(4, "close", expect.any(Function));
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("removes the connection listeners on scope close", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, off, started }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const scope = yield* Scope.make();
          yield* stream.pipe(Stream.runCollect, Effect.forkIn(scope));
          yield* Deferred.await(started);

          yield* Scope.close(scope, Exit.void);

          expect(off).nthCalledWith(1, "replicationStart", expect.any(Function));
          expect(off).nthCalledWith(2, "copyData", expect.any(Function));
          expect(off).nthCalledWith(3, "error", expect.any(Function));
          expect(off).nthCalledWith(4, "close", expect.any(Function));
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("fails the stream when the connection emits an error", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, started, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const fiber = yield* stream.pipe(Stream.runCollect, Effect.forkScoped);

          yield* Deferred.await(started);

          const err = new Error("socket closed");
          mockConnection.emit("error", err);

          const exit = yield* Fiber.await(fiber);

          expect(exit).toSatisfy(Exit.isFailure);
          expect(exit).toSatisfy(Exit.hasFails);
          if (Exit.isFailure(exit)) {
            const failure = pipe(exit.cause, Cause.findFail, Result.getOrThrow);
            expect(failure.error).toStrictEqual(
              new UnknownError({ cause: err, message: undefined, operation: undefined }),
            );
          }
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("ends the stream successfully when the connection closes", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, started, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const fiber = yield* stream.pipe(Stream.runCollect, Effect.forkScoped);

          yield* Deferred.await(started);
          const payload = Buffer.from([0x01]);

          mockConnection.emit("copyData", { chunk: WireMother.walData({ payload }) });
          mockConnection.emit("close", {});

          const exit = yield* Fiber.await(fiber);

          expect(exit).toSatisfy(Exit.isSuccess);
          if (Exit.isSuccess(exit)) {
            expect(exit.value).toStrictEqual([payload]);
          }
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("ends the stream successfully on a graceful 'Connection terminated' error", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, started, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const fiber = yield* stream.pipe(Stream.runCollect, Effect.forkScoped);

          yield* Deferred.await(started);
          const payload = Buffer.from([0x01]);

          mockConnection.emit("copyData", { chunk: WireMother.walData({ payload }) });
          mockConnection.emit("error", new Error("Connection terminated"));

          const exit = yield* Fiber.await(fiber);

          expect(exit).toSatisfy(Exit.isSuccess);
          if (Exit.isSuccess(exit)) {
            expect(exit.value).toStrictEqual([payload]);
          }
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("acknowledge sends a standby status update for the given lsn and ping flag", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { acknowledge } = yield* logicalReplication.subscribe("slot_pgoutput");

          const WAL_END = Lsn.make(faker.number.bigInt());
          yield* acknowledge(WAL_END, true);
          const latestStatus = yield* StandbyStatusUpdate.make({ endWal: WAL_END, ping: true });

          expect(mockConnection.sendCopyFromChunk).toHaveBeenCalledTimes(1);
          expect(mockConnection.sendCopyFromChunk).lastCalledWith(latestStatus);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("acknowledge without ping leaves the reply flag clear", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { acknowledge } = yield* logicalReplication.subscribe("slot_pgoutput");
          const endWal = Lsn.zero();

          yield* acknowledge(endWal);
          const latestStatus = yield* StandbyStatusUpdate.make({ endWal, ping: false });

          expect(mockConnection.sendCopyFromChunk).toHaveBeenCalledTimes(1);
          expect(mockConnection.sendCopyFromChunk).lastCalledWith(latestStatus);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("acknowledge without ping leaves the reply flag clear", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { acknowledge } = yield* logicalReplication.subscribe("slot_pgoutput");
          const endWal = Lsn.zero();

          yield* acknowledge(endWal);
          const latestStatus = yield* StandbyStatusUpdate.make({ endWal, ping: false });

          expect(mockConnection.sendCopyFromChunk).toHaveBeenCalledTimes(1);
          expect(mockConnection.sendCopyFromChunk).lastCalledWith(latestStatus);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("starts replication from the provided uptoLsn", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, started, start }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const uptoLsn = Lsn.make(faker.number.bigInt());
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput", uptoLsn);

          yield* stream.pipe(Stream.runDrain, Effect.forkScoped);

          yield* Deferred.await(started);

          expect(start).toHaveBeenCalledTimes(1);
          expect(start).toHaveBeenCalledWith(expect.anything(), "slot_pgoutput", uptoLsn);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("heartbeat", () =>
    makeFixture.pipe(
      Effect.flatMap(({ TestLayer, started, mockConnection }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream, heartbeat } = yield* logicalReplication.subscribe("slot_pgoutput");
          yield* stream.pipe(Stream.runDrain, Effect.forkScoped);
          yield* Deferred.await(started);
          const initial = yield* SubscriptionRef.get(heartbeat);

          expect(initial).toSatisfy(Option.isNone);

          const WAL_END = Lsn.make(faker.number.bigInt());
          mockConnection.emit("copyData", {
            chunk: WireMother.keepalive({ endWal: WAL_END }),
          });

          const [value] = yield* SubscriptionRef.changes(heartbeat).pipe(
            Stream.drop(1),
            Stream.take(1),
            Stream.runCollect,
          );

          expect(value).toSatisfy(Option.isSome);
          if (Option.isSome(value)) {
            expect(value.value.lsn).toStrictEqual(WAL_END);
            expect(value.value.shouldReply).toBe(false);
          }
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );
});
