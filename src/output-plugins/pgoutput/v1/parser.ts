import { Effect, Match, MutableHashMap } from "effect";

import * as BinaryReader from "../../../binary-reader.js";
import { UnknownMessage } from "../../../errors/index.js";
import type { Config } from "../../../logical-replication-service.js";

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
import {
  MessageIdentifierByte,
  type DataTypeId,
  type RelationId,
  type State,
} from "../common/types.js";

import { LogicalReplicationMessageV1, RelationV1 } from "../v1/types.js";

export const make = Effect.fnUntraced(function* () {
  const state = {
    types: MutableHashMap.empty<DataTypeId, { namespace: string; name: string }>(),
    relations: MutableHashMap.empty<RelationId, RelationV1>(),
  } satisfies State<RelationV1>;

  const decodeBeginV1 = Effect.fn("decodeBeginV1")(function* (reader: BinaryReader.BinaryReader) {
    const data = yield* decodeBegin(reader);
    return LogicalReplicationMessageV1.Begin(data);
  });

  const decodeMessageV1 = Effect.fn("decodeMessageV1")(function* (
    reader: BinaryReader.BinaryReader,
  ) {
    const data = yield* decodeMessage(reader);
    return LogicalReplicationMessageV1.Message(data);
  });

  const decodeCommitV1 = Effect.fn("decodeCommitV1")(function* (reader: BinaryReader.BinaryReader) {
    const data = yield* decodeCommit(reader);
    return LogicalReplicationMessageV1.Commit(data);
  });

  const decodeOriginV1 = Effect.fn("decodeOriginV1")(function* (reader: BinaryReader.BinaryReader) {
    const data = yield* decodeOrigin(reader);
    return LogicalReplicationMessageV1.Origin(data);
  });

  const decodeRelationV1 = Effect.fn("decodeRelationV1")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV1>,
    config?: Config,
  ) {
    const data = yield* decodeRelation(reader, state, config);
    return LogicalReplicationMessageV1.Relation(data);
  });

  const decodeTypeV1 = Effect.fn("decodeTypeV1")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV1>,
  ) {
    const data = yield* decodeType(reader, state);
    return LogicalReplicationMessageV1.Type(data);
  });

  const decodeInsertV1 = Effect.fn("decodeInsertV1")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV1>,
  ) {
    const data = yield* decodeInsert(reader, state);
    return LogicalReplicationMessageV1.Insert(data);
  });

  const decodeUpdateV1 = Effect.fn("decodeUpdateV1")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV1>,
  ) {
    const data = yield* decodeUpdate(reader, state);
    return LogicalReplicationMessageV1.Update(data);
  });

  const decodeDeleteV1 = Effect.fn("decodeDeleteV1")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV1>,
  ) {
    const data = yield* decodeDelete(reader, state);
    return LogicalReplicationMessageV1.Delete(data);
  });

  const decodeTruncateV1 = Effect.fn("decodeTruncateV1")(function* (
    reader: BinaryReader.BinaryReader,
    state: State<RelationV1>,
  ) {
    const data = yield* decodeTruncate(reader, state);
    return LogicalReplicationMessageV1.Truncate(data);
  });

  const parse = Effect.fn("parse")(function* (buf: Uint8Array<ArrayBufferLike>, config?: Config) {
    const reader = yield* BinaryReader.make(buf);
    const tag = yield* reader.readUint8();

    return yield* Match.value(tag).pipe(
      Match.when(MessageIdentifierByte.Begin, () => decodeBeginV1(reader)),
      Match.when(MessageIdentifierByte.Message, () => decodeMessageV1(reader)),
      Match.when(MessageIdentifierByte.Commit, () => decodeCommitV1(reader)),
      Match.when(MessageIdentifierByte.Origin, () => decodeOriginV1(reader)),
      Match.when(MessageIdentifierByte.Relation, () => decodeRelationV1(reader, state, config)),
      Match.when(MessageIdentifierByte.Type, () => decodeTypeV1(reader, state)),
      Match.when(MessageIdentifierByte.Insert, () => decodeInsertV1(reader, state)),
      Match.when(MessageIdentifierByte.Update, () => decodeUpdateV1(reader, state)),
      Match.when(MessageIdentifierByte.Delete, () => decodeDeleteV1(reader, state)),
      Match.when(MessageIdentifierByte.Truncate, () => decodeTruncateV1(reader, state)),
      Match.orElse(() => Effect.fail(new UnknownMessage({ identifier: String.fromCharCode(tag) }))),
    );
  });

  return { parse } as const;
});

export type PgoutputParserV1 = Effect.Success<ReturnType<typeof make>>;
