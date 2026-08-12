import { Data } from "effect";

export class UnexpectedCopyDataMessage extends Data.TaggedError("UnexpectedCopyDataMessage")<{
  readonly identifier: string;
}> {}

export class ConnectionNotFound extends Data.TaggedError("ConnectionNotFound") {}
