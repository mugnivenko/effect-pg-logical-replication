import { Array, Effect, Equal, Function, Hash, Predicate } from "effect";

import { ParseLsnError } from "./errors/index.js";

const TypeId = "~effect/pg/logical-replication/Lsn";

export interface Lsn extends Equal.Equal {
  readonly [TypeId]: typeof TypeId;
  readonly value: bigint;
}

export const isLsn = (arg: unknown): arg is Lsn => Predicate.hasProperty(arg, TypeId);

const LsnProto = {
  [TypeId]: TypeId,
  [Hash.symbol](this: Lsn) {
    return Hash.hash(this.value);
  },
  [Equal.symbol](this: Lsn, that: unknown): boolean {
    return isLsn(that) && Equal.equals(this.value, that.value);
  },
};

export function make(val: bigint): Lsn {
  const lsn = Object.create(LsnProto);
  lsn.value = val;
  return lsn;
}

export function zero(): Lsn {
  return make(0n);
}

export function toString(lsn: Lsn): string {
  const high = lsn.value >> 32n;
  const low = lsn.value & 0xffff_ffffn;
  return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`;
}

export function toBigint(val: Lsn): bigint {
  return val.value;
}

export const add = Function.dual<(that: Lsn) => (self: Lsn) => Lsn, (self: Lsn, that: Lsn) => Lsn>(
  2,
  (self, that) => {
    const newVal = self.value + that.value;

    return make(newVal);
  },
);

export function fromString(val: string): Effect.Effect<Lsn, ParseLsnError> {
  return Effect.gen(function* () {
    const splitted = val.split("/");

    if (Array.length(splitted) === 1) {
      yield* new ParseLsnError({ message: `Missing '/' separator: ${val}` });
    }

    const [highStr, lowStr] = splitted;

    const high = yield* Effect.try({
      try: () => BigInt(`0x${highStr}`),
      catch: (err) => new ParseLsnError({ message: `Invalid high part: ${err}` }),
    });

    const low = yield* Effect.try({
      try: () => BigInt(`0x${lowStr}`),
      catch: (err) => new ParseLsnError({ message: `Invalid low part: ${err}` }),
    });

    return make((high << 32n) | low);
  });
}
