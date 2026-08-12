import { Boolean, Effect, Match, MutableHashMap, Option, Ref } from "effect";

import * as BinaryReader from "../../../binary-reader.js";

import {
  decodeBegin,
  decodeCommit,
  decodeDelete,
  decodeInsert,
  decodeMessage,
  decodeOrigin,
  decodeRelation,
  decodeTruncate,
  decodeType,
  decodeUpdate,
} from "../common/parser.js";
import { UnknownMessage } from "../../../errors/index.js";
import {
  MessageIdentifierByte,
  TransactionId,
  type DataTypeId,
  type RelationId,
  type State,
} from "../common/types.js";
import { LogicalReplicationMessageV2, MessageIdentifierByteV2, RelationV2 } from "./types.js";

export const make = Effect.fnUntraced(function* () {
  const state = {
    types: MutableHashMap.empty<DataTypeId, { namespace: string; name: string }>(),
    relations: MutableHashMap.empty<RelationId, RelationV2>(),
  } satisfies State<RelationV2>;

  const isInStreamRef = yield* Ref.make(false);

  const decodeMessageV2 = Effect.fn("decodeMessageV2")(function* (
    reader: BinaryReader.BinaryReader,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { flags, transactional, lsn, prefix, contentLength, content } =
      yield* decodeMessage(reader);

    return LogicalReplicationMessageV2.Message({
      xid,
      flags,
      transactional,
      lsn,
      prefix,
      contentLength,
      content,
    });
  });

  const decodeRelationV2 = Effect.fn("decodeRelationV2")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV2>,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { oid, namespace, name, replicaIdentity, columns, keyColumns } =
      yield* decodeRelation<RelationV2>(reader, state);

    return LogicalReplicationMessageV2.Relation({
      xid,
      oid,
      namespace,
      name,
      replicaIdentity,
      columns,
      keyColumns,
    });
  });

  const decodeTypeV2 = Effect.fn("decodeTypeV2")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV2>,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { dataTypeId, namespace, name } = yield* decodeType<RelationV2>(reader, state);

    return LogicalReplicationMessageV2.Type({ xid, dataTypeId, namespace, name });
  });

  const decodeInsertV2 = Effect.fn("decodeInsertV2")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV2>,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { relation, new: newTuple } = yield* decodeInsert<RelationV2>(reader, state);

    return LogicalReplicationMessageV2.Insert({ xid, relation, new: newTuple });
  });

  const decodeUpdateV2 = Effect.fn("decodeUpdateV2")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV2>,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { relation, tupleData, new: newTuple } = yield* decodeUpdate<RelationV2>(reader, state);

    return LogicalReplicationMessageV2.Update({ xid, relation, tupleData, new: newTuple });
  });

  const decodeDeleteV2 = Effect.fn("decodeDeleteV2")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV2>,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { relation, tupleData } = yield* decodeDelete<RelationV2>(reader, state);

    return LogicalReplicationMessageV2.Delete({ xid, relation, tupleData });
  });

  const decodeTruncateV2 = Effect.fn("decodeTruncateV2")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV2>,
  ) {
    const isInStream = yield* Ref.get(isInStreamRef);

    const xid = yield* Boolean.match(isInStream, {
      onTrue: () =>
        Effect.gen(function* () {
          const xid = (yield* reader.readInt32()) as TransactionId;
          return Option.some(xid);
        }),
      onFalse: () => Effect.succeed(Option.none<TransactionId>()),
    });

    const { options, relations } = yield* decodeTruncate<RelationV2>(reader, state);

    return LogicalReplicationMessageV2.Truncate({ xid, options, relations });
  });

  const decodeStreamStart = Effect.fn("decodeStreamStart")(function* (
    reader: BinaryReader.BinaryReader,
  ) {
    const xid = (yield* reader.readInt32()) as TransactionId;

    const firstSegment = (yield* reader.readInt8()) === 1;

    yield* Ref.set(isInStreamRef, true);

    return LogicalReplicationMessageV2.StreamStart({ xid, firstSegment });
  });

  const decodeStreamStop = Effect.fn("decodeStreamStop")(function* () {
    yield* Ref.set(isInStreamRef, false);

    return LogicalReplicationMessageV2.StreamStop();
  });

  const decodeStreamCommit = Effect.fn("decodeStreamCommit")(function* (
    reader: BinaryReader.BinaryReader,
  ) {
    const xid = (yield* reader.readInt32()) as TransactionId;
    const flags = yield* reader.readInt8();
    const lsn = yield* reader.readLsn();
    const endLsn = yield* reader.readLsn();
    const timestamp = yield* reader.readTime();

    return LogicalReplicationMessageV2.StreamCommit({ xid, flags, lsn, endLsn, timestamp });
  });

  const decodeStreamAbort = Effect.fn("decodeStreamAbort")(function* (
    reader: BinaryReader.BinaryReader,
  ) {
    const xid = (yield* reader.readInt32()) as TransactionId;
    const subXid = (yield* reader.readInt32()) as TransactionId;

    return LogicalReplicationMessageV2.StreamAbort({ xid, subXid });
  });

  const parse = Effect.fn("parse")(function* (buf: Uint8Array<ArrayBufferLike>) {
    const reader = yield* BinaryReader.make(buf);
    const tag = yield* reader.readUint8();

    return yield* Match.value(tag).pipe(
      Match.when(MessageIdentifierByte.Begin, () => decodeBegin(reader)),
      Match.when(MessageIdentifierByte.Message, () => decodeMessageV2(reader)),
      Match.when(MessageIdentifierByte.Commit, () => decodeCommit(reader)),
      Match.when(MessageIdentifierByte.Origin, () => decodeOrigin(reader)),
      Match.when(MessageIdentifierByte.Relation, () => decodeRelationV2(reader, state)),
      Match.when(MessageIdentifierByte.Type, () => decodeTypeV2(reader, state)),
      Match.when(MessageIdentifierByte.Insert, () => decodeInsertV2(reader, state)),
      Match.when(MessageIdentifierByte.Update, () => decodeUpdateV2(reader, state)),
      Match.when(MessageIdentifierByte.Delete, () => decodeDeleteV2(reader, state)),
      Match.when(MessageIdentifierByte.Truncate, () => decodeTruncateV2(reader, state)),
      Match.when(MessageIdentifierByteV2.StreamStart, () => decodeStreamStart(reader)),
      Match.when(MessageIdentifierByteV2.StreamStop, () => decodeStreamStop()),
      Match.when(MessageIdentifierByteV2.StreamCommit, () => decodeStreamCommit(reader)),
      Match.when(MessageIdentifierByteV2.StreamAbort, () => decodeStreamAbort(reader)),
      Match.orElse(() => Effect.fail(new UnknownMessage({ identifier: String.fromCharCode(tag) }))),
    );
  });

  return { parse } as const;
});

export type PgoutputParserV2 = Effect.Success<ReturnType<typeof make>>;
