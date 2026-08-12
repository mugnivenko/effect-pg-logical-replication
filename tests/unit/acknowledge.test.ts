import EventEmitter from "node:events";

import { vi } from "vitest";
import { describe, it, expect } from "@effect/vitest";

import { faker } from "@faker-js/faker";

import {
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";

import { Client, LogicalReplication, OutputPlugin } from "../../src";
import { Query } from "../../src/client";
import * as StandbyStatusUpdate from "../../src/standby-status-update";
import * as Lsn from "../../src/lsn";
import * as PgTimestamp from "../../src/pg-timestamp";

import { WireMother } from "../support/wire-mother";

class TestReplication extends Context.Service<
  TestReplication,
  LogicalReplication.LogicalReplicationService<void, never>
>()("test/Replication") {}

const makeFixture = Effect.gen(function* () {
  const acked = yield* Deferred.make<void>();
  const parsed = yield* Deferred.make<void>();
  const started = yield* Deferred.make<void>();

  const sendCopyFromChunk = vi.fn(() => {
    Effect.runFork(Deferred.succeed(acked, undefined));
  });
  const mockConnection = Object.assign(new EventEmitter(), { sendCopyFromChunk });

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

  const mockPlugin = OutputPlugin.make<void>({
    name: "mock_replication_plugin",
    start: () => Deferred.complete(started, Effect.void),
    parse: () => Deferred.complete(parsed, Effect.void),
  });

  const TestLayer = LogicalReplication.layer(TestReplication, mockPlugin).pipe(
    Layer.provide(MockClient),
  );

  return { acked, parsed, started, sendCopyFromChunk, mockConnection, TestLayer };
});

describe("acknowledge", () => {
  it.effect("acknowledges once the latch opens, then on each fixed interval", () =>
    makeFixture.pipe(
      Effect.flatMap(({ acked, started, mockConnection, sendCopyFromChunk, TestLayer }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");
          const drain = yield* stream.pipe(Stream.runDrain, Effect.forkScoped);

          yield* Deferred.await(started);
          mockConnection.emit("replicationStart", {});

          yield* Effect.raceFirst(Deferred.await(acked), Fiber.join(drain));

          const status = yield* StandbyStatusUpdate.make({ endWal: Lsn.zero(), ping: false });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(1);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(status);

          yield* TestClock.adjust(Duration.seconds(10));
          const newStatus = yield* StandbyStatusUpdate.make({ endWal: Lsn.zero(), ping: false });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(2);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(newStatus);

          yield* TestClock.adjust(Duration.seconds(20));
          const latestStatus = yield* StandbyStatusUpdate.make({ endWal: Lsn.zero(), ping: false });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(4);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(latestStatus);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("confirms the LSN advanced by an WALData frame", () =>
    makeFixture.pipe(
      Effect.flatMap(({ acked, sendCopyFromChunk, mockConnection, TestLayer, started, parsed }) =>
        Effect.gen(function* () {
          const WAL_END = Lsn.make(faker.number.bigInt());
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const drain = yield* Effect.forkScoped(Stream.runDrain(stream));

          yield* Deferred.await(started);
          mockConnection.emit("replicationStart", {});

          yield* Effect.raceFirst(Deferred.await(acked), Fiber.join(drain));

          const status = yield* StandbyStatusUpdate.make({ endWal: Lsn.zero(), ping: false });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(1);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(status);

          mockConnection.emit("replicationStart", {});
          mockConnection.emit("copyData", {
            chunk: WireMother.walData({
              walStart: Lsn.zero(),
              walEnd: WAL_END,
              payload: Buffer.from([0x01]),
            }),
          });
          yield* Deferred.await(parsed);
          yield* TestClock.adjust(Duration.seconds(10));
          const latestStatus = yield* StandbyStatusUpdate.make({ endWal: WAL_END, ping: false });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(2);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(latestStatus);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("advances lastLsn and sends a ping-flagged ack on a shouldReply keepalive", () =>
    makeFixture.pipe(
      Effect.flatMap(({ acked, sendCopyFromChunk, mockConnection, TestLayer, started }) =>
        Effect.gen(function* () {
          const WAL_END = Lsn.make(faker.number.bigInt());
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");
          const drain = yield* stream.pipe(Stream.runDrain, Effect.forkScoped);

          yield* Deferred.await(started);
          mockConnection.emit("replicationStart", {});

          yield* Effect.raceFirst(Deferred.await(acked), Fiber.join(drain));

          const status = yield* StandbyStatusUpdate.make({ endWal: Lsn.zero(), ping: false });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(1);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(status);

          mockConnection.emit("copyData", {
            chunk: WireMother.keepalive({ endWal: WAL_END, shouldReply: true }),
          });
          yield* TestClock.adjust(Duration.zero);
          const latestStatus = yield* StandbyStatusUpdate.make({ endWal: WAL_END, ping: true });

          expect(sendCopyFromChunk).toHaveBeenCalledTimes(2);
          expect(sendCopyFromChunk).to.have.been.lastCalledWith(latestStatus);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("does not acknowledge until the stream is consumed", () =>
    makeFixture.pipe(
      Effect.flatMap(({ sendCopyFromChunk, TestLayer }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          yield* logicalReplication.subscribe("slot_pgoutput");

          yield* TestClock.adjust(Duration.seconds(60));

          expect(sendCopyFromChunk).not.toHaveBeenCalled();
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("interrupts the ack loop on scope close (no sends after teardown)", () =>
    makeFixture.pipe(
      Effect.flatMap(({ mockConnection, started, acked, sendCopyFromChunk, TestLayer }) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(Date.now());
          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const scope = yield* Scope.make();
          yield* stream.pipe(Stream.runDrain, Effect.forkIn(scope));

          yield* Deferred.await(started);
          mockConnection.emit("replicationStart", {});

          yield* Deferred.await(acked);
          const before = sendCopyFromChunk.mock.calls.length;

          yield* Scope.close(scope, Exit.void);

          yield* TestClock.adjust(Duration.seconds(60));
          expect(sendCopyFromChunk).toHaveBeenCalledTimes(before);
        }).pipe(Effect.provide(TestLayer)),
      ),
    ),
  );

  it.effect("fails the subscribe stream when acknowledge fails", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();

      const sendCopyFromChunk = vi.fn(() => {
        throw new Error("socket closed");
      });
      const mockConnection = Object.assign(new EventEmitter(), { sendCopyFromChunk });

      const mockClient = Object.assign(new EventEmitter(), { connection: mockConnection });

      const MockClient = Layer.succeed(Client.Client, {
        query: (() =>
          Effect.succeed({ command: "", rowCount: 0, oid: 0, rows: [], fields: [] })) as Query,
        connect: () => Effect.succeed(mockClient as never),
      });

      const mockPlugin = OutputPlugin.make<void>({
        name: "mock_replication_plugin",
        start: () => Deferred.complete(started, Effect.void),
        parse: () => Effect.void,
      });

      const TestLayer = LogicalReplication.layer(TestReplication, mockPlugin).pipe(
        Layer.provide(MockClient),
      );

      return yield* Effect.gen(function* () {
        yield* TestClock.setTime(Date.now());
        const logicalReplication = yield* TestReplication;
        const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

        const fiber = yield* stream.pipe(Stream.runDrain, Effect.forkScoped);

        yield* Deferred.await(started);
        mockConnection.emit("replicationStart", {});

        const exit = yield* Fiber.await(fiber);

        expect(exit).toSatisfy(Exit.isFailure);
        expect(exit).toSatisfy(Exit.hasDies);
      }).pipe(Effect.provide(TestLayer));
    }),
  );

  it.effect("acks with the correct wire bytes", () => {
    const timestamp = PgTimestamp.fromWire(faker.number.bigInt());

    return makeFixture.pipe(
      Effect.flatMap(({ acked, started, mockConnection, sendCopyFromChunk, TestLayer }) =>
        Effect.gen(function* () {
          // yield* TestClock.adjust(PgTimestamp.toNanos(timestamp));

          const expected = WireMother.standbyStatusUpdate({ walEnd: Lsn.make(1n), timestamp });

          const logicalReplication = yield* TestReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");
          yield* Effect.forkScoped(Stream.runDrain(stream));

          yield* Deferred.await(started);
          mockConnection.emit("replicationStart", {});
          yield* Deferred.await(acked);

          expect(sendCopyFromChunk).toHaveBeenLastCalledWith(expected);
        }).pipe(
          Effect.provide(TestLayer),
          // TestClock.adjust converts nanos via Number(nanos) / 1_000_000 (Duration.toMillis),
          // which is already lossy for real epoch nanos (~1e18, past Number.MAX_SAFE_INTEGER).
          // Clock.monotonicTimeNanos is rebuilt from that rounded millis value, so it drifts by
          // more than the wire format's microsecond precision allows. An exact Clock override
          // sidesteps TestClock's float representation so the test reads back the exact timestamp.
          Effect.provideService(Clock.Clock, {
            monotonicTimeNanosUnsafe: () => timestamp.micros * 1000n,
            monotonicTimeNanos: Effect.succeed(timestamp.micros * 1000n),
            currentTimeMillisUnsafe: () => Number(timestamp.micros / 1000n),
            currentTimeMillis: Effect.succeed(Number(timestamp.micros / 1000n)),
            currentTimeNanosUnsafe: () => timestamp.micros * 1000n,
            currentTimeNanos: Effect.succeed(timestamp.micros * 1000n),
            sleep: () => Effect.never,
          }),
        ),
      ),
    );
  });
});
