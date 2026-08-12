import * as Lsn from "../../src/lsn.js";
import * as PgTimestamp from "../../src/pg-timestamp.js";

const XLOG_DATA = /* w */ 0x77;
const KEEPALIVE = /* k */ 0x6b;
const STANDBY_STATUS = /* r */ 0x72;

export class WireMother {
  static walData({
    walStart = Lsn.zero(),
    walEnd = Lsn.zero(),
    rawTs = PgTimestamp.fromWire(0n),
    payload = Buffer.from([0x01, 0x02, 0x03, 0x04]),
  }: {
    walStart?: Lsn.Lsn;
    walEnd?: Lsn.Lsn;
    rawTs?: PgTimestamp.PgTimestamp;
    payload?: Buffer;
  }): Buffer {
    const buf = Buffer.alloc(25 + payload.length);
    buf.writeUInt8(XLOG_DATA, 0);
    buf.writeBigUInt64BE(Lsn.toBigint(walStart), 1);
    buf.writeBigUInt64BE(Lsn.toBigint(walEnd), 9);
    buf.writeBigUInt64BE(PgTimestamp.toWire(rawTs), 17);
    payload.copy(buf, 25);
    return buf;
  }

  static keepalive({
    endWal = Lsn.zero(),
    rawTs = PgTimestamp.fromWire(0n),
    shouldReply = false,
  }: {
    endWal?: Lsn.Lsn;
    rawTs?: PgTimestamp.PgTimestamp;
    shouldReply?: boolean;
  }): Buffer {
    const buf = Buffer.alloc(18);
    buf.writeUInt8(KEEPALIVE, 0);
    buf.writeBigUInt64BE(Lsn.toBigint(endWal), 1);
    buf.writeBigUInt64BE(PgTimestamp.toWire(rawTs), 9);
    buf.writeUInt8(shouldReply ? 1 : 0, 17);
    return buf;
  }

  static standbyStatusUpdate({
    walEnd,
    timestamp,
  }: {
    walEnd: Lsn.Lsn;
    timestamp: PgTimestamp.PgTimestamp;
  }) {
    const walVal = Lsn.toBigint(walEnd);
    const timestampVal = PgTimestamp.toWire(timestamp);

    const buf = Buffer.alloc(34);
    buf.fill(STANDBY_STATUS);
    buf.writeBigUInt64BE(walVal, 1);
    buf.writeBigUInt64BE(walVal, 9);
    buf.writeBigUInt64BE(walVal, 17);
    buf.writeBigUInt64BE(timestampVal, 25);
    buf.writeInt8(0, 33);
    return buf;
  }
}
