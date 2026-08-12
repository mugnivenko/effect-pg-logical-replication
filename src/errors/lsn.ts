import { Data } from "effect";

export class ParseLsnError extends Data.TaggedError("ParseLsnError")<{
  message: string;
}> {}
