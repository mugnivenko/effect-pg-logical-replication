import { Context, Effect, Layer, ManagedRuntime, Redacted } from "effect";
import { PostgreSqlContainer, StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { PgClient } from "@effect/sql-pg";
import { Reactivity } from "effect/unstable/reactivity";

class PgContainer extends Context.Service<PgContainer, StartedPostgreSqlContainer>()(
  "PgContainer",
) {}

const database = "playground";
const username = "postgres";
const password = "postgrespw";

const PgContainerLive = Layer.effect(
  PgContainer,
  Effect.acquireRelease(
    Effect.promise(() =>
      new PostgreSqlContainer("postgres:latest")
        .withDatabase(database)
        .withUsername(username)
        .withPassword(password)
        .withCommand([
          "postgres",
          "-c",
          "wal_level=logical",
          "-c",
          "max_wal_senders=10",
          "-c",
          "max_replication_slots=10",
          "-c",
          "log_replication_commands=on",
          "-c",
          "log_statement=mod",
        ])
        .start(),
    ),
    (container) => Effect.promise(() => container.stop()),
  ),
);

const createSchema = Effect.fn(function* (container: StartedPostgreSqlContainer) {
  const sql = yield* PgClient.makeClient({ url: Redacted.make(container.getConnectionUri()) });

  yield* sql`
    CREATE TABLE users (
      id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      firstname TEXT NOT NULL,
      lastname TEXT NOT NULL,
      email VARCHAR(1000),
      phone VARCHAR(1000),
      deleted boolean NOT NULL DEFAULT false,
      created timestamp with time zone NOT NULL DEFAULT NOW()
    );
  `;

  yield* sql`
    CREATE TABLE user_contents (
      id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      user_id BIGINT NOT NULL REFERENCES users(id) ON UPDATE CASCADE ON DELETE CASCADE,
      title TEXT NOT NULL,
      body TEXT NOT NULL,
      deleted boolean NOT NULL DEFAULT false,
      created timestamp with time zone NOT NULL DEFAULT NOW()
    );
  `;

  yield* sql`
    CREATE TABLE huge_transaction (
      id BIGINT GENERATED ALWAYS AS IDENTITY,
      PRIMARY KEY(id),
      column1 TEXT NOT NULL,
      column2 TEXT NOT NULL,
      column3 TEXT NOT NULL,
      column4 TEXT NOT NULL,
      column5 TEXT NOT NULL,
      column6 TEXT NOT NULL,
      column7 TEXT NOT NULL,
      column8 TEXT NOT NULL,
      column9 TEXT NOT NULL,
      column10 TEXT NOT NULL,
      column11 TEXT NOT NULL,
      column12 TEXT NOT NULL,
      column13 TEXT NOT NULL,
      column14 TEXT NOT NULL,
      column15 TEXT NOT NULL,
      column16 TEXT NOT NULL,
      column17 TEXT NOT NULL,
      column18 TEXT NOT NULL,
      column19 TEXT NOT NULL,
      column20 TEXT NOT NULL
    );
  `;

  yield* sql`
    INSERT INTO huge_transaction (
      column1, column2, column3, column4, column5,
      column6, column7, column8, column9, column10,
      column11, column12, column13, column14, column15,
      column16, column17, column18, column19, column20
    )
    SELECT
      md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT),
      md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT),
      md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT),
      md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT), md5(RANDOM()::TEXT)
    FROM generate_series(1, 20 * 1000);
  `;
});

const runtime = ManagedRuntime.make(PgContainerLive);

export function setup() {
  return runtime.runPromise(
    Effect.gen(function* () {
      const container = yield* PgContainer;

      yield* createSchema(container).pipe(Effect.scoped, Effect.provide(Reactivity.layer));

      const host = container.getHost();
      const port = String(container.getPort());

      process.env.PGHOST = host;
      process.env.PGPORT = port;
      process.env.POSTGRES_HOST = host;
      process.env.POSTGRES_PORT = port;
      process.env.PGUSER = username;
      process.env.PGDATABASE = database;
      process.env.PGPASSWORD = password;
    }),
  );
}

export function teardown() {
  return runtime.dispose();
}
