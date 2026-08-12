// https://www.postgresql.org/docs/current/protocol-logical-replication.html#PROTOCOL-LOGICAL-REPLICATION-PARAMS
export interface Options {
  protoVersion: 1 | 2;
  publicationNames: string[];
  messages?: boolean;
}

export * from "./common/types.js";
