import { Effect, Scope } from "effect";

import { Client } from "./client.js";
import { PgError } from "./errors/index.js";
import * as Lsn from "./lsn.js";
import type { Config } from "./logical-replication-service.js";

export interface OutputPlugin<A = unknown, E = unknown> {
  readonly name: string;
  readonly parse: (buffer: Uint8Array, config?: Config) => Effect.Effect<A, E>;
  readonly start: (
    client: Client["Service"],
    slotName: string,
    lastLsn: Lsn.Lsn,
  ) => Effect.Effect<unknown, PgError.PgError, Scope.Scope>;
}

export const make = <A, E = never>(plugin: OutputPlugin<A, E>) => plugin;
