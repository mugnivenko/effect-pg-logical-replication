import { types } from "pg";

import {
  Array,
  Effect,
  HashSet,
  Match,
  MutableHashMap,
  MutableHashSet,
  Option,
  pipe,
  Record,
  Result,
} from "effect";

import type * as BinaryReader from "../../../binary-reader.js";
import {
  MissingRelation,
  UnknownReplicaIdentity,
  UnknownTupleDataMessage,
  UnknownTupleDataValueIdentifier,
} from "../../../errors/index.js";

import {
  ColumnFlag,
  DataTypeId,
  DeleteTupleData,
  Relation,
  RelationColumn,
  RelationId,
  ReplicaIdentity,
  TruncateOption,
  TupleColumnValue,
  TupleDataMessageIdentifier,
  TupleDataValueIdentifier,
  UpdateTupleData,
  type State,
  type TransactionId,
} from "./types.js";
import { Config } from "../../../logical-replication-service.js";

const readRelationColumn = Effect.fn("readRelationColumn")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
  config?: Config,
) {
  const flags = yield* reader.readUint8();
  const name = yield* reader.readString();
  const dataTypeId = (yield* reader.readInt32()) as DataTypeId;
  const typeModifier = yield* reader.readInt32();

  const { dataTypeName, dataTypeNamespace } = pipe(
    state.types,
    MutableHashMap.get(dataTypeId),
    Option.map(({ name, namespace }) => ({
      dataTypeName: Option.some(name),
      dataTypeNamespace: Option.some(namespace),
    })),
    Option.getOrElse(() => ({
      dataTypeName: Option.none<string>(),
      dataTypeNamespace: Option.none<string>(),
    })),
  );

  const parser = config?.getTypeParser?.(dataTypeId) ?? types.getTypeParser(dataTypeId);

  return {
    flags,
    name,
    dataTypeId,
    typeModifier,
    dataTypeName,
    dataTypeNamespace,
    parser,
  } satisfies RelationColumn;
});

const readTupleData = Effect.fn("readTuple")(function* (
  reader: BinaryReader.BinaryReader,
  { columns }: Relation,
) {
  const tupleData = Record.empty<string, TupleColumnValue>();
  const columnsNum = yield* reader.readInt16();

  for (let i = 0; i < columnsNum; i++) {
    const { name, parser } = columns[i];
    const kind = yield* reader.readUint8();

    yield* Match.value(kind).pipe(
      Match.when(
        TupleDataValueIdentifier.Null,
        Effect.fnUntraced(function* () {
          tupleData[name] = TupleColumnValue.Null();
        }),
      ),
      Match.when(
        TupleDataValueIdentifier.Unchanged,
        Effect.fnUntraced(function* () {
          tupleData[name] = TupleColumnValue.Unchanged();
        }),
      ),
      Match.when(
        TupleDataValueIdentifier.Binary,
        Effect.fnUntraced(function* () {
          const valsize = yield* reader.readInt32();
          const value = yield* reader.read(valsize);
          tupleData[name] = TupleColumnValue.Binary({ value });
        }),
      ),
      Match.when(
        TupleDataValueIdentifier.Text,
        Effect.fnUntraced(function* () {
          const valsize = yield* reader.readInt32();
          const valbuf = yield* reader.read(valsize);
          const value = reader.decodeText(valbuf);
          tupleData[name] = TupleColumnValue.Text({ value: parser(value) });
        }),
      ),
      Match.orElse(() =>
        Effect.fail(new UnknownTupleDataValueIdentifier({ identifier: String.fromCharCode(kind) })),
      ),
    );
  }

  return tupleData;
});

export const decodeBegin = Effect.fn("decodeBegin")(function* (reader: BinaryReader.BinaryReader) {
  const finalLsn = yield* reader.readLsn();
  const timestamp = yield* reader.readTime();
  const xid = yield* reader.readInt32();
  return { finalLsn, timestamp, xid: xid as TransactionId };
});

export const decodeMessage = Effect.fn("decodeMessage")(function* (
  reader: BinaryReader.BinaryReader,
) {
  const flags = yield* reader.readUint8();
  const lsn = yield* reader.readLsn();
  const prefix = yield* reader.readString();
  const contentLength = yield* reader.readInt32();
  return {
    flags,
    transactional: Boolean(flags),
    lsn,
    prefix,
    contentLength,
    content: yield* reader.read(contentLength),
  };
});

export const decodeCommit = Effect.fn("decodeCommit")(function* (
  reader: BinaryReader.BinaryReader,
) {
  return {
    flags: yield* reader.readUint8(),
    lsn: yield* reader.readLsn(),
    endLsn: yield* reader.readLsn(),
    timestamp: yield* reader.readTime(),
  };
});

export const decodeOrigin = Effect.fn("decodeOrigin")(function* (
  reader: BinaryReader.BinaryReader,
) {
  const lsn = yield* reader.readLsn();
  const name = yield* reader.readString();
  return { lsn, name };
});

export enum ReplicaIdentityIdByte {
  Default = /* d */ 0x64,
  Nothing = /* n */ 0x6e,
  AllColumns = /* f */ 0x66,
  Index = /* i */ 0x69,
}

const readReplicaIdentity = Effect.fn("readRelationReplicaIdentity")(function* (
  reader: BinaryReader.BinaryReader,
) {
  const ident = yield* reader.readUint8();
  return yield* Match.value(ident).pipe(
    Match.when(ReplicaIdentityIdByte.Default, () => ReplicaIdentity.Default),
    Match.when(ReplicaIdentityIdByte.Nothing, () => ReplicaIdentity.Nothing),
    Match.when(ReplicaIdentityIdByte.AllColumns, () => ReplicaIdentity.AllColumns),
    Match.when(ReplicaIdentityIdByte.Index, () => ReplicaIdentity.Index),
    Match.option,
    Effect.fromOption,
    Effect.mapError(() => new UnknownReplicaIdentity({ identifier: String.fromCharCode(ident) })),
  );
});

export const decodeRelation = Effect.fn("decodeRelation")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
  config?: Config,
) {
  const oid = (yield* reader.readInt32()) as RelationId;
  const namespace = yield* reader.readString();
  const name = yield* reader.readString();
  const replicaIdentity = yield* readReplicaIdentity(reader);

  const columnsNum = yield* reader.readInt16();
  const columns = yield* pipe(
    columnsNum,
    Array.makeBy((idx) => idx),
    Effect.forEach(() => readRelationColumn<R>(reader, state, config)),
  );

  const keyColumns = Array.filterMap(columns, (it) =>
    it.flags === ColumnFlag.Key ? Result.succeed(it.name) : Result.failVoid,
  );

  const msg = { oid, namespace, name, replicaIdentity, columns, keyColumns } satisfies Relation;

  pipe(state.relations, MutableHashMap.set(oid, msg));

  return msg;
});

export const decodeType = Effect.fn("decodeType")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
) {
  const dataTypeId = (yield* reader.readInt32()) as DataTypeId;
  const namespace = yield* reader.readString();
  const name = yield* reader.readString();
  pipe(state.types, MutableHashMap.set(dataTypeId, { namespace, name }));
  return { dataTypeId, namespace, name };
});

export const decodeInsert = Effect.fn("decodeInsert")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
) {
  const oid = (yield* reader.readInt32()) as RelationId;

  const relation = yield* pipe(
    state.relations,
    MutableHashMap.get(oid),
    Option.match({
      onSome: (relation) => Effect.succeed(relation),
      onNone: () => Effect.fail(new MissingRelation({ oid })),
    }),
  );

  /* discard 'N' tuple identifier */ yield* reader.readUint8();

  return { relation, new: yield* readTupleData(reader, relation) };
});

export const decodeUpdate = Effect.fn("decodeUpdate")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
) {
  const oid = (yield* reader.readInt32()) as RelationId;

  const relation = yield* pipe(
    state.relations,
    MutableHashMap.get(oid),
    Option.match({
      onSome: Effect.succeed,
      onNone: () => Effect.fail(new MissingRelation({ oid })),
    }),
  );

  const submessage = yield* reader.readUint8();

  const { tupleData, new: newTuple } = yield* Match.value(submessage).pipe(
    Match.when(
      TupleDataMessageIdentifier.PrimaryKey,
      Effect.fn(function* () {
        const key = yield* readTupleData(reader, relation);
        /* discard 'N' tuple identifier */ yield* reader.readUint8();
        const newTuple = yield* readTupleData(reader, relation);
        return { tupleData: UpdateTupleData.Key({ value: key }), new: newTuple };
      }),
    ),
    Match.when(
      TupleDataMessageIdentifier.OldTuple,
      Effect.fn(function* () {
        const old = yield* readTupleData(reader, relation);
        /* discard 'N' tuple identifier */ yield* reader.readUint8();
        const newTuple = yield* readTupleData(reader, relation);
        return { tupleData: UpdateTupleData.Old({ value: old }), new: newTuple };
      }),
    ),
    Match.when(
      TupleDataMessageIdentifier.NewTuple,
      Effect.fn(function* () {
        const newTuple = yield* readTupleData(reader, relation);
        return { tupleData: UpdateTupleData.None(), new: newTuple };
      }),
    ),
    Match.orElse(() =>
      Effect.fail(new UnknownTupleDataMessage({ identifier: String.fromCharCode(submessage) })),
    ),
  );

  return { relation, tupleData, new: newTuple };
});

export const decodeDelete = Effect.fn("decodeDelete")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
) {
  const oid = yield* reader.readInt32();

  const relation = yield* Option.match(MutableHashMap.get(state.relations, oid), {
    onSome: (relation) => Effect.succeed(relation),
    onNone: () => Effect.fail(new MissingRelation({ oid })),
  });

  const msgId = yield* reader.readUint8();

  const { tupleData } = yield* Match.value(msgId).pipe(
    Match.when(
      TupleDataMessageIdentifier.PrimaryKey,
      Effect.fn(function* () {
        const key = yield* readTupleData(reader, relation);
        return { tupleData: DeleteTupleData.Key({ value: key }) };
      }),
    ),
    Match.when(
      TupleDataMessageIdentifier.OldTuple,
      Effect.fn(function* () {
        const old = yield* readTupleData(reader, relation);
        return { tupleData: DeleteTupleData.Old({ value: old }) };
      }),
    ),
    Match.orElse(() =>
      Effect.fail(new UnknownTupleDataMessage({ identifier: String.fromCharCode(msgId) })),
    ),
  );

  return { relation, tupleData };
});

export const decodeTruncate = Effect.fn("decodeTruncate")(function* <R extends Relation>(
  reader: BinaryReader.BinaryReader,
  state: State<R>,
) {
  const relsNum = yield* reader.readInt32();
  const flags = yield* reader.readUint8();

  const options = MutableHashSet.empty<TruncateOption>();

  if (flags & TruncateOption.Cascade) {
    MutableHashSet.add(options, TruncateOption.Cascade);
  }

  if (flags & TruncateOption.RestartIdentity) {
    MutableHashSet.add(options, TruncateOption.RestartIdentity);
  }

  const relations = yield* pipe(
    relsNum,
    Array.makeBy((idx) => idx),
    Effect.forEach(
      Effect.fnUntraced(function* () {
        const oid = yield* reader.readInt32();
        return yield* pipe(
          state.relations,
          MutableHashMap.get(oid),
          Option.match({
            onSome: (relation) => Effect.succeed(relation),
            onNone: () => Effect.fail(new MissingRelation({ oid })),
          }),
        );
      }),
    ),
  );

  return {
    options: pipe(options, HashSet.fromIterable),
    relations,
  };
});
