import { type Brand, Data, type Option, type MutableHashMap, type Array, HashSet } from "effect";

import * as Lsn from "../../../lsn.js";
import * as PgTimestamp from "../../../pg-timestamp.js";

export type DataTypeId = Brand.Branded<number, "DataTypeId">;

export type RelationId = Brand.Branded<number, "RelationId">;

export interface RelationColumn {
  flags: ColumnFlag;
  name: string;
  dataTypeId: DataTypeId;
  dataTypeNamespace: Option.Option<string>;
  dataTypeName: Option.Option<string>;
  typeModifier: number;
  parser: (raw: string) => unknown;
}

export interface Relation {
  oid: RelationId;
  namespace: string;
  name: string;
  replicaIdentity: ReplicaIdentity;
  columns: Array.NonEmptyArray<RelationColumn>;
  keyColumns: string[];
}

export interface State<R extends Relation> {
  readonly types: MutableHashMap.MutableHashMap<DataTypeId, { namespace: string; name: string }>;
  readonly relations: MutableHashMap.MutableHashMap<RelationId, R>;
}

export type TupleColumnValue = Data.TaggedEnum<{
  Null: {};
  Unchanged: {};
  Text: {
    readonly value: unknown;
  };
  Binary: {
    readonly value: Uint8Array;
  };
}>;

export type TupleData = Record<string, TupleColumnValue>;

export type DeleteTupleData = Data.TaggedEnum<{
  Key: {
    readonly value: TupleData;
  };
  Old: {
    readonly value: TupleData;
  };
}>;

export type UpdateTupleData = Data.TaggedEnum<{
  Key: {
    readonly value: TupleData;
  };
  Old: {
    readonly value: TupleData;
  };
  None: {};
}>;

export const TupleColumnValue = Data.taggedEnum<TupleColumnValue>();

export const DeleteTupleData = Data.taggedEnum<DeleteTupleData>();

export const UpdateTupleData = Data.taggedEnum<UpdateTupleData>();

export enum TruncateOption {
  Cascade = 0b1,
  RestartIdentity = 0b10,
}

export enum ReplicaIdentity {
  Default = "default",
  Nothing = "nothing",
  AllColumns = "all_columns",
  Index = "index",
}

export enum ColumnFlag {
  None = 0,
  Key = 1,
}

export type TransactionId = Brand.Branded<number, "TransactionId">;

export enum TupleDataValueIdentifier {
  Null = /* n */ 0x6e,
  Unchanged = /* u */ 0x75,
  Text = /* t */ 0x74,
  Binary = /* b */ 0x62,
}

export enum TupleDataMessageIdentifier {
  PrimaryKey = /* K */ 0x4b,
  OldTuple = /* O */ 0x4f,
  NewTuple = /* N */ 0x4e,
}

export enum MessageIdentifierByte {
  Begin = /* B */ 0x42,
  Message = /* M */ 0x4d,
  Commit = /* C */ 0x43,
  Origin = /* O */ 0x4f,
  Relation = /* R */ 0x52,
  Type = /* Y */ 0x59,
  Insert = /* I */ 0x49,
  Update = /* U */ 0x55,
  Delete = /* D */ 0x44,
  Truncate = /* T */ 0x54,
}

export interface Begin {
  finalLsn: Option.Option<Lsn.Lsn>;
  timestamp: PgTimestamp.PgTimestamp;
  xid: TransactionId;
}

export interface Message {
  flags: number;
  transactional: boolean;
  lsn: Option.Option<Lsn.Lsn>;
  prefix: string;
  contentLength: number;
  content: Uint8Array;
}

export interface Commit {
  flags: number;
  lsn: Option.Option<Lsn.Lsn>;
  endLsn: Option.Option<Lsn.Lsn>;
  timestamp: PgTimestamp.PgTimestamp;
}

export interface Origin {
  lsn: Option.Option<Lsn.Lsn>;
  name: string;
}

export interface Type {
  dataTypeId: DataTypeId;
  namespace: string;
  name: string;
}

export interface Insert {
  new: TupleData;
}

export interface Update {
  tupleData: UpdateTupleData;
  new: TupleData;
}

export interface Delete {
  tupleData: DeleteTupleData;
}

export interface Truncate {
  options: HashSet.HashSet<TruncateOption>;
}
