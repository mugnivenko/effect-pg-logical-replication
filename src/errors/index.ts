export * as PgError from "./pg/index.js";

export {
  UnexpectedEndOfMessage,
  RelationNotFound,
  UnknownTupleDataMessage,
  UnknownReplicaIdentity,
  MissingRelation,
  UnknownTupleDataValueIdentifier,
  UnknownMessage,
} from "./parser.js";

export { UnexpectedCopyDataMessage, ConnectionNotFound } from "./replication.js";

export { ParseLsnError } from "./lsn.js";

import * as PgError from "./pg/index.js";

import type { UnexpectedCopyDataMessage } from "./replication.js";

import type {
  MissingRelation,
  UnexpectedEndOfMessage,
  UnknownTupleDataValueIdentifier,
  UnknownReplicaIdentity,
  UnknownTupleDataMessage,
  UnknownMessage,
} from "./parser.js";

export type StreamError = PgError.PgError | UnexpectedCopyDataMessage | UnexpectedEndOfMessage;

export type ParseError =
  | UnexpectedEndOfMessage
  | MissingRelation
  | UnknownReplicaIdentity
  | UnknownTupleDataMessage
  | UnknownTupleDataValueIdentifier
  | UnknownMessage;
