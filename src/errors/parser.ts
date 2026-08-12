import { Data } from "effect";

export class UnexpectedEndOfMessage extends Data.TaggedError("UnexpectedEndOfMessage")<{
  offset: number;
  need: number;
  length: number;
}> {}

export class RelationNotFound extends Data.TaggedError("RelationNotFound")<{}> {}

export class UnknownTupleDataMessage extends Data.TaggedError("UnknownTupleDataSubmessage")<{
  identifier: string;
}> {}

export class UnknownReplicaIdentity extends Data.TaggedError("UnknownReplicaIdentity")<{
  identifier: string;
}> {}

export class MissingRelation extends Data.TaggedError("MissingRelation")<{
  oid: number;
}> {}

export class UnknownTupleDataValueIdentifier extends Data.TaggedError(
  "UnknownTupleDataValueIdentifier",
)<{
  identifier: string;
}> {}

export class UnknownMessage extends Data.TaggedError("UnknownMessage")<{
  identifier: string;
}> {}
