import { Data, Option } from "effect";

import * as Lsn from "../../../lsn.js";
import * as PgTimestamp from "../../../pg-timestamp.js";

import type {
  Begin,
  Message,
  Commit,
  Origin,
  Type,
  Insert,
  Update,
  Delete,
  Truncate,
  Relation,
  TransactionId,
} from "../common/types.js";

export type LogicalReplicationMessageV2 = Data.TaggedEnum<{
  Begin: Begin;
  Message: Message & {
    xid: Option.Option<TransactionId>;
  };
  Commit: Commit;
  Origin: Origin;
  Relation: Relation & {
    xid: Option.Option<TransactionId>;
  };
  Type: Type & {
    xid: Option.Option<TransactionId>;
  };
  Insert: Insert & {
    xid: Option.Option<TransactionId>;
    relation: RelationV2;
  };
  Update: Update & {
    xid: Option.Option<TransactionId>;
    relation: RelationV2;
  };
  Delete: Delete & {
    xid: Option.Option<TransactionId>;
    relation: RelationV2;
  };
  Truncate: Truncate & {
    xid: Option.Option<TransactionId>;
    relations: RelationV2[];
  };
  StreamStart: {
    xid: TransactionId;
    firstSegment: boolean;
  };
  StreamStop: {};
  StreamCommit: {
    xid: TransactionId;
    flags: number;
    lsn: Option.Option<Lsn.Lsn>;
    endLsn: Option.Option<Lsn.Lsn>;
    timestamp: PgTimestamp.PgTimestamp;
  };
  StreamAbort: {
    xid: TransactionId;
    subXid: TransactionId;
  };
}>;

export type BeginV2 = Extract<LogicalReplicationMessageV2, { _tag: "Begin" }>;
export type CommitV2 = Extract<LogicalReplicationMessageV2, { _tag: "Commit" }>;
export type DeleteV2 = Extract<LogicalReplicationMessageV2, { _tag: "Delete" }>;
export type InsertV2 = Extract<LogicalReplicationMessageV2, { _tag: "Insert" }>;
export type MessageV2 = Extract<LogicalReplicationMessageV2, { _tag: "Message" }>;
export type OriginV2 = Extract<LogicalReplicationMessageV2, { _tag: "Origin" }>;
export type RelationV2 = Extract<LogicalReplicationMessageV2, { _tag: "Relation" }>;
export type TruncateV2 = Extract<LogicalReplicationMessageV2, { _tag: "Truncate" }>;
export type TypeV2 = Extract<LogicalReplicationMessageV2, { _tag: "Type" }>;
export type UpdateV2 = Extract<LogicalReplicationMessageV2, { _tag: "Update" }>;
export type StreamStart = Extract<LogicalReplicationMessageV2, { _tag: "StreamStart" }>;
export type StreamStop = Extract<LogicalReplicationMessageV2, { _tag: "StreamStop" }>;
export type StreamCommit = Extract<LogicalReplicationMessageV2, { _tag: "StreamCommit" }>;
export type StreamAbort = Extract<LogicalReplicationMessageV2, { _tag: "StreamAbort" }>;

export const LogicalReplicationMessageV2 = Data.taggedEnum<LogicalReplicationMessageV2>();

export enum MessageIdentifierByteV2 {
  StreamStart = /* S */ 0x53,
  StreamStop = /* E */ 0x45,
  StreamCommit = /* c */ 0x63,
  StreamAbort = /* A */ 0x41,
}
