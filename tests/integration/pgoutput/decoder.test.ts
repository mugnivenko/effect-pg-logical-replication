import { layer, describe, expect, describeWrapped } from "@effect/vitest";

import { faker } from "@faker-js/faker";

import {
  Array,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  pipe,
  Stream,
  Context,
  Record,
  Ref,
  Option,
  Result,
  SubscriptionRef,
} from "effect";

import {
  LogicalReplication,
  PgoutputV1,
  PgoutputPlugin,
  Client,
  Lsn,
  Pgoutput,
} from "../../../src";

import { makeTestClient } from "../../support/test-client.js";

const slotName = "slot_pgoutput";
const decoderName = "pgoutput";
const publicationName = "pgoutput_test_pub";

class PgoutputReplication extends Context.Service<
  PgoutputReplication,
  LogicalReplication.LogicalReplicationService<PgoutputV1.LogicalReplicationMessageV1>
>()("test/Replication") {}

const TestClient = makeTestClient({ slotName, decoderName, publicationName });

const pgoutputPlugin = PgoutputPlugin.make({
  protoVersion: 1,
  publicationNames: [publicationName],
});

const TestLogicalReplication = LogicalReplication.layer(PgoutputReplication, pgoutputPlugin).pipe(
  Layer.provide(Client.layerFromConfig()),
);

describe("decoder", () => {
  layer(TestClient.layer, { excludeTestServices: true })((it) => {
    it.effect("should collect update logs", () => {
      return Effect.gen(function* () {
        const sql = yield* TestClient;

        const EXPECTED_INSERTS = 10;

        const EXPECTED_TRANSACTIONS = 2;

        const fiber = yield* Effect.gen(function* () {
          const logicalReplication = yield* PgoutputReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          const commits = yield* Ref.make(0);

          return yield* stream.pipe(
            Stream.tap((msg) =>
              PgoutputV1.LogicalReplicationMessageV1.$is("Commit")(msg)
                ? Ref.update(commits, (num) => num + 1)
                : Effect.void,
            ),
            Stream.takeUntilEffect((msg) =>
              Effect.map(
                Ref.get(commits),
                (num) =>
                  PgoutputV1.LogicalReplicationMessageV1.$is("Commit")(msg) &&
                  num >= EXPECTED_TRANSACTIONS,
              ),
            ),
            Stream.runCollect,
          );
        }).pipe(Effect.forkScoped);

        const result = yield* sql`
         INSERT INTO users(firstname, lastname, email, phone)
         SELECT md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
         FROM generate_series(1, 5) RETURNING *
      `;

        const insert = yield* sql`
         INSERT INTO user_contents(user_id, title, body)
         SELECT id, md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
         FROM users
         WHERE id >= ${result[0].id} RETURNING *
      `;

        const all = yield* Fiber.join(fiber);
        const messages = pipe(
          all,
          Array.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Insert")),
        );

        const relation = pipe(
          all,
          Array.findFirst(PgoutputV1.LogicalReplicationMessageV1.$is("Relation")),
          Option.getOrThrow,
        );

        const relationLast = pipe(
          all,
          Array.findLast(PgoutputV1.LogicalReplicationMessageV1.$is("Relation")),
          Option.getOrThrow,
        );

        expect(result.length).toBe(5);
        expect(insert.length).toBe(5);
        expect(
          pipe(all, Array.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Begin"))),
        ).toHaveLength(EXPECTED_TRANSACTIONS);
        expect(
          pipe(all, Array.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Commit"))),
        ).toHaveLength(EXPECTED_TRANSACTIONS);
        expect(messages).toHaveLength(EXPECTED_INSERTS);
        expect(
          pipe(
            relation.columns,
            Array.map((column) => column.name),
          ),
        ).toEqual(["id", "firstname", "lastname", "email", "phone", "deleted", "created"]);
        expect(
          pipe(
            relationLast.columns,
            Array.map((column) => column.name),
          ),
        ).toEqual(["id", "user_id", "title", "body", "deleted", "created"]);
        pipe(
          messages,
          Array.take(5),
          Array.forEach((val, idx) => {
            expect(val.relation.name).toBe("users");
            const inserted = pipe(
              result[idx],
              Record.map((value) => Pgoutput.TupleColumnValue.Text({ value })),
            );
            expect(val.new).toStrictEqual(inserted);
          }),
        );
        pipe(
          messages,
          Array.takeRight(5),
          Array.forEach((val, idx) => {
            expect(val.relation.name).toBe("user_contents");
            const inserted = pipe(
              insert[idx],
              Record.map((value) => Pgoutput.TupleColumnValue.Text({ value })),
            );
            expect(val.new).toStrictEqual(inserted);
          }),
        );
      }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
    });

    it.effect("should decode update", () => {
      return Effect.gen(function* () {
        const sql = yield* TestClient;

        const [inserted] = yield* sql`
          INSERT INTO users(firstname, lastname, email, phone)
          VALUES (md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT))
          RETURNING *
        `;

        const logicalReplication = yield* PgoutputReplication;
        const [{ lsn }] = yield* sql<{ lsn: string }>`SELECT pg_current_wal_lsn() AS lsn`;
        const startLsn = yield* Lsn.fromString(lsn);
        const { stream, acknowledge } = yield* logicalReplication.subscribe(
          "slot_pgoutput",
          startLsn,
        );

        const fiber = yield* stream
          .pipe(
            Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Update")),
            Stream.take(1),
            Stream.runCollect,
          )
          .pipe(Effect.forkScoped);

        const [updated] = yield* sql`
          UPDATE users SET firstname = md5(RANDOM()::TEXT) WHERE id = ${inserted.id} RETURNING *
        `;

        const [message] = yield* Fiber.join(fiber);

        yield* acknowledge(yield* logicalReplication.lastLsn());

        expect(message).toBeDefined();
        expect(message).toSatisfy(PgoutputV1.LogicalReplicationMessageV1.$is("Update"));
        if (!PgoutputV1.LogicalReplicationMessageV1.$is("Update")(message)) return;
        expect(message.relation.name).toBe("users");
        expect(message.tupleData).toSatisfy(Pgoutput.UpdateTupleData.$is("None"));
        expect(message.new).toStrictEqual(
          pipe(
            updated,
            Record.map((value) => Pgoutput.TupleColumnValue.Text({ value })),
          ),
        );
      }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
    });

    it.effect("should not decode inserts from a rolled back transaction", () => {
      return Effect.gen(function* () {
        const sql = yield* TestClient;
        const logicalReplication = yield* PgoutputReplication;
        const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

        const inserted = yield* Ref.make(0);
        const fiveInserted = yield* Deferred.make<void>();
        const tenInserted = yield* Deferred.make<void>();

        const fiber = yield* stream
          .pipe(
            Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Insert")),
            Stream.runForEach(() =>
              Ref.updateAndGet(inserted, (num) => num + 1).pipe(
                Effect.flatMap((num) => {
                  if (num === 5) return Deferred.complete(fiveInserted, Effect.void);
                  if (num === 10) return Deferred.complete(tenInserted, Effect.void);
                  return Effect.void;
                }),
              ),
            ),
          )
          .pipe(Effect.forkScoped);

        const insertFive = () => sql`
          INSERT INTO users(firstname, lastname, email, phone)
          SELECT md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
          FROM generate_series(1, 5) RETURNING *
        `;

        yield* insertFive();
        yield* Deferred.await(fiveInserted);
        expect(yield* Ref.get(inserted)).toBe(5);

        yield* insertFive().pipe(
          Effect.andThen(Effect.fail("rollback")),
          sql.withTransaction,
          Effect.ignore,
        );

        yield* Effect.sleep(Duration.millis(300));
        expect(yield* Ref.get(inserted)).toBe(5);

        yield* insertFive().pipe(sql.withTransaction);

        yield* Deferred.await(tenInserted);
        expect(yield* Ref.get(inserted)).toBe(10);

        yield* Fiber.interrupt(fiber);
      }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
    });

    it.effect("should decode pg_logical_emit_message", () => {
      const pgoutputPlugin = PgoutputPlugin.make({
        protoVersion: 1,
        publicationNames: [publicationName],
        messages: true,
      });

      const TestLogicalReplication = LogicalReplication.layer(
        PgoutputReplication,
        pgoutputPlugin,
      ).pipe(Layer.provide(Client.layerFromConfig()));

      return Effect.gen(function* () {
        const sql = yield* TestClient;

        const fiber = yield* Effect.gen(function* () {
          const logicalReplication = yield* PgoutputReplication;
          const { stream } = yield* logicalReplication.subscribe("slot_pgoutput");

          return yield* stream.pipe(
            Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Message")),
            Stream.take(1),
            Stream.runCollect,
          );
        }).pipe(Effect.forkScoped);

        const prefix = faker.string.sample();
        const content = faker.string.sample();
        yield* sql`SELECT pg_logical_emit_message(true, ${prefix}, ${content})`;

        const [message] = yield* Fiber.join(fiber);

        expect(message).toBeDefined();
        expect(message).toSatisfy(PgoutputV1.LogicalReplicationMessageV1.$is("Message"));
        if (!PgoutputV1.LogicalReplicationMessageV1.$is("Message")(message)) return;
        expect(message.transactional).toBe(true);
        expect(message.prefix).toBe(prefix);
        expect(message.content).toStrictEqual(Buffer.from(content));
      }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
    });

    describeWrapped("delete", () => {
      it.effect("should decode delete", () => {
        return Effect.gen(function* () {
          const sql = yield* TestClient;

          const [inserted] = yield* sql`
              INSERT INTO users(firstname, lastname, email, phone)
              VALUES (md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT))
              RETURNING *
            `;

          const logicalReplication = yield* PgoutputReplication;
          const [{ lsn }] = yield* sql<{ lsn: string }>`SELECT pg_current_wal_lsn() AS lsn`;
          const startLsn = yield* Lsn.fromString(lsn);
          const { stream, acknowledge } = yield* logicalReplication.subscribe(
            "slot_pgoutput",
            startLsn,
          );

          const fiber = yield* stream
            .pipe(
              Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Delete")),
              Stream.take(1),
              Stream.runCollect,
            )
            .pipe(Effect.forkScoped);

          yield* sql`DELETE FROM users WHERE id = ${inserted.id}`;

          const [message] = yield* Fiber.join(fiber);

          yield* acknowledge(yield* logicalReplication.lastLsn());

          expect(message).toBeDefined();
          expect(message).toSatisfy(PgoutputV1.LogicalReplicationMessageV1.$is("Delete"));
          if (!PgoutputV1.LogicalReplicationMessageV1.$is("Delete")(message)) return;
          expect(message.relation.name).toBe("users");
          expect(message.tupleData).toSatisfy(Pgoutput.DeleteTupleData.$is("Key"));
          expect(message.tupleData.value["id"]).toStrictEqual(
            Pgoutput.TupleColumnValue.Text({ value: inserted.id }),
          );
        }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
      });

      it.effect("should decode delete with REPLICA IDENTITY FULL", () => {
        return Effect.gen(function* () {
          const sql = yield* TestClient;

          yield* sql`ALTER TABLE users REPLICA IDENTITY FULL`;

          const [inserted] = yield* sql`
              INSERT INTO users(firstname, lastname, email, phone)
              VALUES (md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT))
              RETURNING *
            `;

          const logicalReplication = yield* PgoutputReplication;
          const [{ lsn }] = yield* sql<{ lsn: string }>`SELECT pg_current_wal_lsn() AS lsn`;
          const startLsn = yield* Lsn.fromString(lsn);
          const { stream, acknowledge } = yield* logicalReplication.subscribe(
            "slot_pgoutput",
            startLsn,
          );

          const fiber = yield* stream
            .pipe(
              Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Delete")),
              Stream.take(1),
              Stream.runCollect,
            )
            .pipe(Effect.forkScoped);

          yield* sql`DELETE FROM users WHERE id = ${inserted.id}`;

          const [message] = yield* Fiber.join(fiber);

          yield* acknowledge(yield* logicalReplication.lastLsn());

          yield* sql`ALTER TABLE users REPLICA IDENTITY DEFAULT`;

          expect(message).toBeDefined();
          if (!PgoutputV1.LogicalReplicationMessageV1.$is("Delete")(message)) return;
          expect(message.tupleData).toSatisfy(Pgoutput.DeleteTupleData.$is("Old"));
          expect(message.tupleData.value).toStrictEqual(
            pipe(
              inserted,
              Record.map((value) => Pgoutput.TupleColumnValue.Text({ value })),
            ),
          );
        }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
      });

      it.effect("should decode multiple deletes in a single statement", () => {
        return Effect.gen(function* () {
          const sql = yield* TestClient;

          const rows = yield* sql`
              INSERT INTO users(firstname, lastname, email, phone)
              SELECT md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
              FROM generate_series(1, 3) RETURNING *
            `;
          const insertedIds = rows.map((row) => row.id);

          const logicalReplication = yield* PgoutputReplication;
          const [{ lsn }] = yield* sql<{ lsn: string }>`SELECT pg_current_wal_lsn() AS lsn`;
          const startLsn = yield* Lsn.fromString(lsn);
          const { stream, acknowledge } = yield* logicalReplication.subscribe(
            "slot_pgoutput",
            startLsn,
          );

          const fiber = yield* stream
            .pipe(
              Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Delete")),
              Stream.take(3),
              Stream.runCollect,
            )
            .pipe(Effect.forkScoped);

          yield* sql`DELETE FROM users WHERE ${sql.in("id", insertedIds)}`;

          const messages = yield* Fiber.join(fiber);

          yield* acknowledge(yield* logicalReplication.lastLsn());

          expect(messages).toHaveLength(3);

          const actualIds = new Set();

          pipe(
            messages,
            Array.forEach((message) => {
              if (!PgoutputV1.LogicalReplicationMessageV1.$is("Delete")(message)) return;

              Pgoutput.TupleColumnValue.$match({
                Text: ({ value }) => {
                  actualIds.add(value);
                },
                Null: () => {},
                Unchanged: () => {},
                Binary: () => {},
              })(message.tupleData.value["id"]);
            }),
          );

          expect(actualIds).toEqual(new Set(insertedIds));
        }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
      });

      it.effect(
        "should decode cascade deletes across related tables (FK ON DELETE CASCADE)",
        () => {
          return Effect.gen(function* () {
            const sql = yield* TestClient;

            const users = yield* sql`
              INSERT INTO users(firstname, lastname, email, phone)
              SELECT md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
              FROM generate_series(1, 5) RETURNING *
            `;
            const userIds = users.map((user) => user.id);

            yield* sql`
              INSERT INTO user_contents(user_id, title, body)
              SELECT id, md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
              FROM users
              WHERE ${sql.in("id", userIds)} RETURNING *
            `;

            const logicalReplication = yield* PgoutputReplication;
            const [{ lsn }] = yield* sql<{ lsn: string }>`SELECT pg_current_wal_lsn() AS lsn`;
            const startLsn = yield* Lsn.fromString(lsn);
            const { stream, acknowledge } = yield* logicalReplication.subscribe(
              "slot_pgoutput",
              startLsn,
            );

            const fiber = yield* stream
              .pipe(
                Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Delete")),
                Stream.take(10),
                Stream.runCollect,
              )
              .pipe(Effect.forkScoped);

            yield* sql`DELETE FROM users WHERE ${sql.in("id", userIds)}`;

            const messages = yield* Fiber.join(fiber);

            yield* acknowledge(yield* logicalReplication.lastLsn());

            expect(messages).toHaveLength(10);

            const tableNames = pipe(
              messages,
              Array.filterMap((message) =>
                PgoutputV1.LogicalReplicationMessageV1.$is("Delete")(message)
                  ? Result.succeed(message.relation.name)
                  : Result.failVoid,
              ),
            );

            expect(tableNames.filter((name) => name === "users")).toHaveLength(5);
            expect(tableNames.filter((name) => name === "user_contents")).toHaveLength(5);
          }).pipe(Effect.provide(TestLogicalReplication), Effect.timeout("5 seconds"));
        },
      );
    });

    it.effect("should decode a huge transaction, with heartbeats along the way", () => {
      const ROW_COUNT = 20 * 1000;

      return Effect.gen(function* () {
        const sql = yield* TestClient;

        const logicalReplication = yield* PgoutputReplication;
        const [{ lsn }] = yield* sql<{ lsn: string }>`SELECT pg_current_wal_lsn() AS lsn`;
        const startLsn = yield* Lsn.fromString(lsn);
        const { stream, acknowledge, heartbeat } = yield* logicalReplication.subscribe(
          "slot_pgoutput",
          startLsn,
        );

        const updateCount = yield* Ref.make(0);
        const allUpdated = yield* Deferred.make<void>();

        const fiber = yield* stream
          .pipe(
            Stream.filter(PgoutputV1.LogicalReplicationMessageV1.$is("Update")),
            Stream.filter((msg) => msg.relation.name === "huge_transaction"),
            Stream.runForEach((_msg) => {
              return Ref.updateAndGet(updateCount, (num) => num + 1).pipe(
                Effect.flatMap((num) =>
                  num === ROW_COUNT ? Deferred.succeed(allUpdated, void 0) : Effect.void,
                ),
              );
            }),
          )
          .pipe(Effect.forkScoped);

        const heartbeatSeen = yield* SubscriptionRef.changes(heartbeat)
          .pipe(Stream.filter(Option.isSome), Stream.take(1), Stream.runDrain)
          .pipe(Effect.forkScoped);

        yield* sql`UPDATE huge_transaction SET column1 = md5(RANDOM()::TEXT), column2 = md5(RANDOM()::TEXT)`;

        yield* Effect.raceFirst(Deferred.await(allUpdated), Fiber.join(fiber));
        yield* acknowledge(yield* logicalReplication.lastLsn());

        expect(yield* Ref.get(updateCount)).toBe(ROW_COUNT);

        yield* Fiber.join(heartbeatSeen);

        yield* Fiber.interrupt(fiber);
      }).pipe(Effect.provide(TestLogicalReplication));
    });
  });
});
