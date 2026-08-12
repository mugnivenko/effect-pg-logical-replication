import { Match, Option, Predicate, Schema } from "effect";

import {
  ClassConnectionException,
  ConnectionDoesNotExist,
  ConnectionException,
  ConnectionFailure,
  ConnectionTerminated,
  ConnectionTerminatedUnexpectedly,
  ProtocolViolation,
} from "./connection.js";
import { DatabaseError } from "pg";
import { ReasonsFiels } from "./reasons-fields.js";

export class UnknownError extends Schema.TaggedError<UnknownError>(
  "effect/pg/logical-replication/UnknownError",
)("UnknownError", ReasonsFiels) {}

export type PgError = UnknownError | ClassConnectionException;

function isDatabaseError(err: unknown): err is DatabaseError {
  return Predicate.hasProperty(err, "message") && Predicate.hasProperty(err, "code");
}

export function classify(cause: unknown, message?: string, operation?: string): PgError {
  const props = { cause, message, operation };

  if (isDatabaseError(cause)) {
    const err = Match.value(cause.code).pipe(
      Match.when(ConnectionException.code, () => new ConnectionException(props)),
      Match.when(ConnectionDoesNotExist.code, () => new ConnectionDoesNotExist(props)),
      Match.when(ConnectionFailure.code, () => new ConnectionFailure(props)),
      Match.when(ProtocolViolation.code, () => new ProtocolViolation(props)),
      Match.option,
    );

    if (Option.isSome(err)) {
      return err.value;
    }
  }

  if (Error.isError(cause)) {
    const err = Match.value(cause.message).pipe(
      Match.when("Connection terminated", () => new ConnectionTerminated(props)),
      Match.when(
        "Connection terminated unexpectedly",
        () => new ConnectionTerminatedUnexpectedly(props),
      ),
      Match.option,
    );

    if (Option.isSome(err)) {
      return err.value;
    }
  }

  return new UnknownError(props);
}
