import { Effect, Scope } from "effect";
import { Client } from "../client.js";

export abstract class AbstractPlugin<O = unknown, A = unknown, E = never> {
  public constructor(public readonly options: O) {}

  public abstract get name(): string;
  public abstract start(
    client: Client["Service"],
    slotName: string,
    lastLsn: bigint,
  ): Effect.Effect<A, E, Scope.Scope>;
  public abstract parse(buffer: Buffer): Effect.Effect<A, E>;
}
