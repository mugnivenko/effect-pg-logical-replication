import { Config, Context, Effect, Layer, Predicate, Redacted } from "effect";
import { PgClientConfig } from "@effect/sql-pg/PgClient";

import {
  ClientConfig,
  Client as PgClient,
  QueryArrayConfig,
  QueryArrayResult,
  QueryConfig,
  QueryConfigValues,
  QueryResult,
  QueryResultRow,
} from "pg";

import { PgError } from "./errors/index.js";

export interface ReplicationClientConfig extends ClientConfig {
  replication: "database";
}

export interface Query {
  <R extends unknown[] = unknown[], I = unknown[]>(
    queryConfig: QueryArrayConfig<I>,
    values?: QueryConfigValues<I>,
  ): Effect.Effect<QueryArrayResult<R>, PgError.PgError>;
  <R extends QueryResultRow = Record<string, unknown>, I = unknown[]>(
    queryConfig: QueryConfig<I>,
  ): Effect.Effect<QueryResult<R>, PgError.PgError>;
  <R extends QueryResultRow = any, I = any[]>(
    queryTextOrConfig: string | QueryConfig<I>,
    values?: QueryConfigValues<I>,
  ): Effect.Effect<QueryResult<R>, PgError.PgError>;
}

export const make = Effect.fnUntraced(function* (options: PgClientConfig) {
  const client = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new PgClient({
          ...(Predicate.isNotUndefined(options.url) && {
            connectionString: Redacted.value(options.url),
          }),
          user: options.username,
          host: options.host,
          database: options.database,
          ...(Predicate.isNotUndefined(options.password) && {
            password: Redacted.value(options.password),
          }),
          ssl: options.ssl,
          port: options.port,
          application_name: options.applicationName ?? "effect/pg/logical-replication",
          types: options.types,
          replication: "database",
        } as ReplicationClientConfig),
      catch: (cause) => PgError.classify(cause, "PgClient: Failed to create client", "create"),
    }),
    (client) => Effect.promise(() => client.end()).pipe(Effect.timeoutOption(1000)),
    { interruptible: true },
  );

  const query = Effect.fn("Client.query")(function* (
    textOrConfig: string | QueryConfig<unknown[]>,
    values?: QueryConfigValues<unknown[]>,
  ) {
    return yield* Effect.tryPromise({
      try: () => client.query(textOrConfig, values),
      catch: (cause) => PgError.classify(cause, "PgClient: query failed", "query"),
    });
  }) as Query;

  return {
    connect: Effect.fn("Client.connect")(function* () {
      return yield* Effect.tryPromise({
        try: () => client.connect(),
        catch: (cause) => PgError.classify(cause, "PgClient: Failed to connect", "connect"),
      });
    }),
    query,
  };
});

export class Client extends Context.Service<
  Client,
  {
    readonly connect: () => Effect.Effect<PgClient, PgError.PgError>;
    readonly query: Query;
  }
>()("Client") {}

export const layer = (options: PgClientConfig) => Layer.effect(Client, make(options));

export const layerFromConfig = (overrides?: Partial<PgClientConfig>): Layer.Layer<Client, any> =>
  Effect.gen(function* () {
    const config = yield* Config.all({
      host: Config.string("PGHOST"),
      port: Config.int("PGPORT"),
      username: Config.string("PGUSER"),
      password: Config.redacted("PGPASSWORD"),
      database: Config.string("PGDATABASE"),
    });

    return layer({
      host: config.host,
      port: config.port,
      username: config.username,
      password: config.password,
      database: config.database,
      ...overrides,
    } as PgClientConfig);
  }).pipe(Effect.orDie, Layer.unwrap);
