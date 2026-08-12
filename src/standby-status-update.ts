import { Clock, Effect, pipe } from "effect";
import * as Lsn from "./lsn.js";
import * as PgTimestamp from "./pg-timestamp.js";

export const make = Effect.fnUntraced(function* ({
  endWal,
  ping,
}: {
  endWal: Lsn.Lsn;
  ping: boolean;
}) {
  const currentTimeNanos = yield* Clock.currentTimeNanos;

  const tsMicros = pipe(currentTimeNanos, PgTimestamp.fromNanos, PgTimestamp.toWire);

  const acked = pipe(endWal, Lsn.add(Lsn.make(1n)), Lsn.toBigint);

  const buf = Buffer.alloc(34);
  buf.fill(0x72); // 'r'
  buf.writeBigUInt64BE(acked, 1); // received
  buf.writeBigUInt64BE(acked, 9); // flushed
  buf.writeBigUInt64BE(acked, 17); // applied
  buf.writeBigUint64BE(tsMicros, 25);
  buf.writeInt8(ping ? 1 : 0, 33);

  return buf;
});
