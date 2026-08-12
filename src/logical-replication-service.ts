import type { types } from "pg";
import type { CopyDataMessage as PgCopyDataMessage } from "pg-protocol/dist/messages";

import {
  Cause,
  Context,
  Duration,
  Effect,
  FiberSet,
  Latch,
  Layer,
  Match,
  Option,
  Predicate,
  Queue,
  Ref,
  Result,
  Schedule,
  Scope,
  Stream,
  SubscriptionRef,
} from "effect";

import { OutputPlugin } from "./output-plugin.js";

import * as CopyDataMessage from "./copy-data-message.js";
import * as StandbyStatusUpdate from "./standby-status-update.js";
import * as Lsn from "./lsn.js";
import * as PgTimestamp from "./pg-timestamp.js";

import { PgError, StreamError } from "./errors/index.js";

import { Client } from "./client.js";

export interface Config {
  getTypeParser?: typeof types.getTypeParser;
  acknowledge?: {
    /**
     * If false, the periodic standby status update is not sent automatically;
     * the consumer is responsible for calling the returned `acknowledge` function.
     * Default: true
     */
    auto?: boolean;

    window?: Duration.Input;
  };
}

const make = <A, E>(
  plugin: OutputPlugin<A, E>,
  config?: Config,
): Effect.Effect<LogicalReplicationService<A, E>, never, Client> =>
  Effect.gen(function* () {
    yield* Effect.log("initializing");

    const client = yield* Client;

    const copyDataMessage = yield* CopyDataMessage.make();

    const lastLsn = yield* Ref.make(Lsn.zero());

    return {
      lastLsn: Effect.fn("LogicalReplication.lastLsn")(function* () {
        const lsn = yield* Ref.get(lastLsn);
        yield* Effect.logTrace("lastLsn read").pipe(
          Effect.annotateLogs({ lsn: Lsn.toString(lsn) }),
        );
        return lsn;
      }),
      subscribe: Effect.fn("LogicalReplication.subscribe")(function* (
        slotName: string,
        uptoLsn?: Lsn.Lsn,
      ) {
        const runFork = yield* FiberSet.makeRuntime<never>();

        yield* Effect.log("starting").pipe((eff) =>
          Predicate.isNotUndefined(uptoLsn)
            ? Effect.annotateLogs(eff, { uptoLsn: Lsn.toString(uptoLsn) })
            : eff,
        );

        if (Predicate.isNotUndefined(uptoLsn)) {
          yield* Ref.set(lastLsn, uptoLsn);
        }

        const latch = yield* Latch.make();

        const heartbeat = yield* SubscriptionRef.make(
          Option.none<{ lsn: Lsn.Lsn; timestamp: PgTimestamp.PgTimestamp; shouldReply: boolean }>(),
        );

        const connectedClient = yield* client.connect();
        const { connection } = connectedClient;

        yield* Effect.logDebug("connected");

        yield* Effect.sync(() =>
          connectedClient.on("error", (err: unknown) => {
            Effect.logError("client error", err).pipe(runFork);
          }),
        );

        const acknowledge = Effect.fn("acknowledge")(function* (
          lsn: Lsn.Lsn,
          ping: boolean = false,
        ) {
          yield* Effect.logDebug("sending standby status update").pipe(
            Effect.annotateLogs({ lsn: Lsn.toString(lsn), ping }),
          );

          const chunk = yield* StandbyStatusUpdate.make({ endWal: lsn, ping });
          // @ts-expect-error sendCopyFromChunk exists at runtime on pg's Connection but is missing from its types
          yield* Effect.sync(() => connection.sendCopyFromChunk(chunk));
        });

        const stream = Stream.callback<Buffer<ArrayBufferLike>, PgError.PgError>(
          Effect.fnUntraced(function* (queue) {
            const acquire = Effect.sync(() => {
              const onReplicationStart = () => {
                Effect.logInfo("replication started").pipe(runFork);
                latch.openUnsafe();
              };

              const onData = ({ chunk }: PgCopyDataMessage) => {
                Effect.logTrace("copy data received").pipe(
                  Effect.annotateLogs({ bytes: chunk.length }),
                  runFork,
                );
                Queue.offerUnsafe(queue, Buffer.from(chunk));
              };

              const onError = (err: unknown) => {
                const pgErr = PgError.classify(err);
                Match.value(pgErr).pipe(
                  Match.tag("ConnectionTerminated", () => {
                    Effect.logInfo("connection terminated").pipe(runFork);
                    Queue.endUnsafe(queue);
                  }),
                  Match.orElse(() => {
                    Effect.logError("stream error", pgErr).pipe(runFork);
                    Queue.failCauseUnsafe(queue, Cause.fail(pgErr));
                  }),
                );
              };

              const onEnd = () => {
                Effect.logInfo("stream ended").pipe(runFork);
                Queue.endUnsafe(queue);
              };

              connection.on("replicationStart", onReplicationStart);
              connection.on("copyData", onData);
              connection.on("error", onError);
              connection.on("close", onEnd);
              connection.on("end", onEnd);
              connectedClient.on("error", onError);

              return { onData, onError, onEnd, onReplicationStart };
            });

            yield* Effect.acquireRelease(
              acquire,
              ({ onData, onError, onEnd, onReplicationStart }) =>
                Effect.gen(function* () {
                  connection.off("replicationStart", onReplicationStart);
                  connection.off("copyData", onData);
                  connection.off("error", onError);
                  connection.off("close", onEnd);
                  connection.off("end", onEnd);
                  connectedClient.off("error", onError);

                  yield* Effect.logDebug("listeners detached");
                }),
            );

            const lastLsnVal = yield* Ref.get(lastLsn);

            yield* Effect.logDebug("starting plugin").pipe(
              Effect.annotateLogs({
                plugin: plugin.name,
                lastLsn: Lsn.toString(lastLsnVal),
              }),
            );

            yield* plugin.start(client, slotName, lastLsnVal);
          }),
        );

        const policy = Match.value(config).pipe(
          Match.whenAnd(
            Match.defined,
            { acknowledge: { window: Match.defined } },
            ({ acknowledge }) => {
              const { window } = acknowledge as NonNullable<typeof acknowledge>;
              return Schedule.fixed(window as NonNullable<typeof window>);
            },
          ),
          Match.orElse(() => Schedule.fixed(Duration.seconds(10))),
        );

        const acknowledgeLoop = Effect.gen(function* () {
          const lsn = yield* Ref.get(lastLsn);

          yield* acknowledge(lsn);

          yield* Effect.logDebug("acknowledge").pipe(
            Effect.annotateLogs({ lsn: Lsn.toString(lsn) }),
          );
        }).pipe(Effect.repeat(policy));

        const awaitedAcknowledgeLoop = latch.await.pipe(Effect.andThen(acknowledgeLoop));

        const parsedStream = stream.pipe(
          Stream.filterMapEffect<Buffer<ArrayBufferLike>, A, void, E | StreamError, never>(
            Effect.fnUntraced(function* (payload) {
              const copyDataMessageType = yield* copyDataMessage.parse(payload);

              return yield* CopyDataMessage.$match({
                WalData: ({ walEnd, payload }) =>
                  Effect.gen(function* () {
                    yield* Effect.logTrace("WAL data").pipe(
                      Effect.annotateLogs({ walEnd: Lsn.toString(walEnd) }),
                    );

                    yield* Ref.set(lastLsn, walEnd);

                    const data = yield* plugin.parse(payload, config) as Effect.Effect<A, E>;

                    return Result.succeed(data);
                  }),
                PrimaryKeepaliveMessage: ({ endWal, timestamp, shouldReply }) =>
                  Effect.gen(function* () {
                    yield* Effect.logTrace("primary keepalive").pipe(
                      Effect.annotateLogs({ endWal: Lsn.toString(endWal), shouldReply }),
                    );

                    yield* Ref.set(lastLsn, endWal);
                    if (shouldReply) {
                      yield* acknowledge(endWal, true);
                    }
                    yield* SubscriptionRef.set(
                      heartbeat,
                      Option.some({ lsn: endWal, timestamp, shouldReply }),
                    );
                    return Result.failVoid;
                  }),
              })(copyDataMessageType);
            }),
          ),
        );

        const filteredStream = Match.value(config).pipe(
          Match.whenAnd(Match.defined, { acknowledge: { auto: false } }, () => parsedStream),
          Match.orElse(() => parsedStream.pipe(Stream.mergeEffect(awaitedAcknowledgeLoop))),
        );

        return { stream: filteredStream, acknowledge, heartbeat };
      }),
    };
  });

export interface LogicalReplicationService<A, E = never> {
  lastLsn: () => Effect.Effect<Lsn.Lsn>;
  subscribe: (
    slotName: string,
    uptoLsn?: Lsn.Lsn,
  ) => Effect.Effect<
    {
      stream: Stream.Stream<A, E | StreamError>;
      acknowledge: (lsn: Lsn.Lsn, ping?: boolean) => Effect.Effect<void>;
      heartbeat: SubscriptionRef.SubscriptionRef<
        Option.Option<{ lsn: Lsn.Lsn; timestamp: PgTimestamp.PgTimestamp; shouldReply: boolean }>
      >;
    },
    PgError.PgError,
    Scope.Scope
  >;
}

export const layer = <Self, A, E, LE = never, LR = never>(
  tag: Context.Key<Self, LogicalReplicationService<A, E>>,
  plugin: OutputPlugin<A, E> | Effect.Effect<OutputPlugin<A, E>, LE, LR>,
  config?: Config,
) =>
  Layer.effect(
    tag,
    Effect.isEffect(plugin)
      ? Effect.flatMap(plugin, (plugin) => make(plugin, config))
      : make(plugin, config),
  );
