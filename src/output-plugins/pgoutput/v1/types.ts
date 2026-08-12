import { Data } from "effect";

import type {
  Relation,
  Begin,
  Message,
  Commit,
  Origin,
  Type,
  Insert,
  Update,
  Delete,
  Truncate,
} from "../common/types.js";

export type LogicalReplicationMessageV1 = Data.TaggedEnum<{
  Begin: Begin;
  Message: Message;
  Commit: Commit;
  Origin: Origin;
  Relation: Relation;
  Type: Type;
  Insert: Insert & {
    relation: RelationV1;
  };
  Update: Update & {
    relation: RelationV1;
  };
  Delete: Delete & {
    relation: RelationV1;
  };
  Truncate: Truncate & {
    relations: RelationV1[];
  };
}>;

export type BeginV1 = Extract<LogicalReplicationMessageV1, { _tag: "Begin" }>;
export type CommitV1 = Extract<LogicalReplicationMessageV1, { _tag: "Commit" }>;
export type DeleteV1 = Extract<LogicalReplicationMessageV1, { _tag: "Delete" }>;
export type InsertV1 = Extract<LogicalReplicationMessageV1, { _tag: "Insert" }>;
export type MessageV1 = Extract<LogicalReplicationMessageV1, { _tag: "Message" }>;
export type OriginV1 = Extract<LogicalReplicationMessageV1, { _tag: "Origin" }>;
export type RelationV1 = Extract<LogicalReplicationMessageV1, { _tag: "Relation" }>;
export type TruncateV1 = Extract<LogicalReplicationMessageV1, { _tag: "Truncate" }>;
export type TypeV1 = Extract<LogicalReplicationMessageV1, { _tag: "Type" }>;
export type UpdateV1 = Extract<LogicalReplicationMessageV1, { _tag: "Update" }>;

export const LogicalReplicationMessageV1 = Data.taggedEnum<LogicalReplicationMessageV1>();
