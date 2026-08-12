import { Context, Effect, Layer } from "effect";
import { PgClient } from "@effect/sql-pg";
import { Reactivity } from "effect/unstable/reactivity";

export interface TestClientOptions {
  readonly slotName: string;
  readonly decoderName: string;
  readonly publicationName: string;
}

export const makeTestClient = ({ slotName, decoderName, publicationName }: TestClientOptions) => {
  class TestClient extends Context.Service<TestClient>()("TestClient", {
    make: Effect.gen(function* () {
      const client = yield* PgClient.makeClient({});

      yield* Effect.addFinalizer(() =>
        Effect.gen(function* () {
          yield* client`DROP PUBLICATION IF EXISTS ${client(publicationName)}`.pipe(Effect.exit);
          yield* client`SELECT pg_drop_replication_slot(${slotName})`.pipe(Effect.exit);
        }),
      );

      yield* client`SELECT * FROM pg_create_logical_replication_slot(${slotName}, ${decoderName})`;
      yield* client`CREATE PUBLICATION ${client(publicationName)} FOR ALL TABLES`;

      return client;
    }),
  }) {
    static layer = Layer.effect(this, this.make).pipe(Layer.provide(Reactivity.layer));
  }

  return TestClient;
};
