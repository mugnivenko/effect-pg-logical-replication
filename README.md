# pg-logical-replication

**Work in progress**

A complete rewrite of the pg-logical-replication library in Effect

## 2. Usage

- This is an example using `pgoutput`. A replication slot(`slot_pgoutput`) must be created on the PostgreSQL
  server.
  - `SELECT * FROM pg_create_logical_replication_slot('slot_pgoutput', 'pgoutput')`

```typescript
class Replication extends Context.Service<
  Replication,
  LogicalReplication.LogicalReplicationService<PgoutputV1.LogicalReplicationMessageV1>
>()("app/Replication") {}

const plugin = PgoutputPlugin.make({
  protoVersion: 1,
  publicationNames: ["pgoutput_pub"],
});

const ReplicationLive = LogicalReplication.layer(Replication, plugin).pipe(
  Layer.provide(Client.layerFromConfig()),
);

const program = Effect.gen(function* () {
  const replication = yield* Replication;

  const { stream } = yield* replication.subscribe("slot_pgoutput");

  yield* stream.pipe(
    Stream.runForEach((message) =>
      Effect.sync(() => {
        // Do something what you want.

        if (pipe(message, PgoutputV1.LogicalReplicationMessageV1.$is("Update"))) {
        }
      }),
    ),
  );
}).pipe(Effect.provide(ReplicationLive));
```

## Contributors

<a href="https://github.com/mugnivenko/effect-pg-logical-replication/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mugnivenko/effect-pg-logical-replication" />
</a>
