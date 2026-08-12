import { Equal, Hash, Predicate } from "effect";

const TypeId = "~effect/pg/logical-replication/PgTimestamp";

// PostgreSQL epoch (2000-01-01) in microseconds since Unix epoch.
const PG_EPOCH_OFFSET_MICROS = 946_684_800_000_000n;

export interface PgTimestamp extends Equal.Equal {
  readonly [TypeId]: typeof TypeId;
  readonly micros: bigint;
}

export const isPgTimestamp = (arg: unknown): arg is PgTimestamp =>
  Predicate.hasProperty(arg, TypeId);

const Proto = {
  [TypeId]: TypeId,
  [Hash.symbol](this: PgTimestamp) {
    return Hash.hash(this.micros);
  },
  [Equal.symbol](this: PgTimestamp, that: unknown): boolean {
    return isPgTimestamp(that) && Equal.equals(this.micros, that.micros);
  },
};

function fromMicros(micros: bigint): PgTimestamp {
  const ts = Object.create(Proto);
  ts.micros = micros;
  return ts;
}

export const fromWire = (microsSincePgEpoch: bigint): PgTimestamp =>
  fromMicros(microsSincePgEpoch + PG_EPOCH_OFFSET_MICROS);

export const toWire = (ts: PgTimestamp): bigint => ts.micros - PG_EPOCH_OFFSET_MICROS;

export const fromMillis = (millisSinceUnixEpoch: bigint): PgTimestamp =>
  fromMicros(millisSinceUnixEpoch * 1000n);

export const fromNanos = (nanosSinceUnixEpoch: bigint): PgTimestamp =>
  fromMicros(nanosSinceUnixEpoch / 1000n);

export const toMicros = (ts: PgTimestamp): bigint => ts.micros;

export const toNanos = (ts: PgTimestamp): bigint => ts.micros * 1000n;
