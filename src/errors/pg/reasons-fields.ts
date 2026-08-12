import { Schema } from "effect";

export const ReasonsFiels = {
  cause: Schema.Defect(),
  message: Schema.optional(Schema.String),
  operation: Schema.optional(Schema.String),
};
