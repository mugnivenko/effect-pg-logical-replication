import { Schema } from "effect";
import { ReasonsFiels } from "./reasons-fields.js";

export class ConnectionException extends Schema.TaggedError<ConnectionException>(
  "effect/pg/logical-replication/ConnectionException",
)("ConnectionException", ReasonsFiels) {
  static code = "08000";
}

export class ConnectionDoesNotExist extends Schema.TaggedError<ConnectionDoesNotExist>(
  "effect/pg/logical-replication/ConnectionDoesNotExist",
)("ConnectionDoesNotExist", ReasonsFiels) {
  static code = "08003";
}

export class ConnectionFailure extends Schema.TaggedError<ConnectionFailure>(
  "effect/pg/logical-replication/ConnectionFailure",
)("ConnectionFailure", ReasonsFiels) {
  static code = "08006";
}

export class ConnectionTerminated extends Schema.TaggedError<ConnectionTerminated>(
  "effect/pg/logical-replication/ConnectionTerminated",
)("ConnectionTerminated", ReasonsFiels) {}

export class ConnectionTerminatedUnexpectedly extends Schema.TaggedError<ConnectionTerminatedUnexpectedly>(
  "effect/pg/logical-replication/ConnectionTerminatedUnexpectedly",
)("ConnectionTerminatedUnexpectedly", ReasonsFiels) {}

export class ProtocolViolation extends Schema.TaggedError<ProtocolViolation>(
  "effect/pg/logical-replication/ProtocolViolation",
)("ProtocolViolation", ReasonsFiels) {
  static code = "08P01";
}

export type ClassConnectionException =
  | ConnectionException
  | ProtocolViolation
  | ConnectionFailure
  | ConnectionTerminated
  | ConnectionTerminatedUnexpectedly
  | ConnectionDoesNotExist;
