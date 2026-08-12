import { Effect, Option, Ref } from "effect";

import { TextDecoder } from "node:util";

import { UnexpectedEndOfMessage } from "./errors/index.js";
import * as PgTimestamp from "./pg-timestamp.js";
import * as Lsn from "./lsn.js";

const textDecoder = new TextDecoder();

export const make = Effect.fnUntraced(function* (buffer: Uint8Array) {
  const offset = yield* Ref.make(0);

  const view = new DataView(buffer.buffer, buffer.byteOffset, buffer.byteLength);

  const checkSize = Effect.fn("checkSize")(function* (need: number) {
    const length = buffer.length;
    const offsetVal = yield* Ref.get(offset);

    if (length >= offsetVal + need) {
      return yield* Effect.void;
    }

    yield* Effect.logError("UnexpectedEndOfMessage", { length, offset: offsetVal, need });

    yield* new UnexpectedEndOfMessage({ length, offset: offsetVal, need });
  });

  const readUint32 = Effect.fn("BinaryReader.readUint32")(function* () {
    yield* checkSize(4);
    const offsetVal = yield* Ref.getAndUpdate(offset, (val) => (val += 4));
    return view.getUint32(offsetVal);
  });

  const readUint64 = Effect.fn("BinaryReader.readUint64")(function* () {
    yield* checkSize(8);
    const offsetVal = yield* Ref.getAndUpdate(offset, (val) => (val += 8));
    return view.getBigUint64(offsetVal);
  });

  return {
    readUint8: Effect.fn("BinaryReader.readUint8")(function* () {
      yield* checkSize(1);
      const offsetVal = yield* Ref.getAndUpdate(offset, (val) => (val += 1));
      return view.getUint8(offsetVal);
    }),
    readUint32,
    readUint64,
    readInt8: Effect.fn("BinaryReader.readInt8")(function* () {
      yield* checkSize(1);
      const offsetVal = yield* Ref.getAndUpdate(offset, (val) => (val += 1));
      return view.getInt8(offsetVal);
    }),
    readInt16: Effect.fn("BinaryReader.readInt16")(function* () {
      yield* checkSize(2);
      const offsetVal = yield* Ref.getAndUpdate(offset, (val) => (val += 2));
      return view.getInt16(offsetVal);
    }),
    readInt32: Effect.fn("BinaryReader.readInt32")(function* () {
      yield* checkSize(4);
      const offsetVal = yield* Ref.getAndUpdate(offset, (val) => (val += 4));
      return view.getInt32(offsetVal);
    }),
    readString: Effect.fn("BinaryReader.readString")(function* () {
      const offsetVal = yield* Ref.get(offset);
      const endIdx = buffer.indexOf(0x00, offsetVal);

      if (endIdx < 0) {
        yield* Effect.logError("UnexpectedEndOfMessage", {
          length: buffer.length,
          offset: offsetVal,
          need: endIdx,
        });

        yield* new UnexpectedEndOfMessage({
          length: buffer.length,
          need: endIdx,
          offset: offsetVal,
        });
      }

      const strBuf = buffer.subarray(offsetVal, endIdx);
      yield* Ref.update(offset, () => endIdx + 1);
      return textDecoder.decode(strBuf);
    }),
    read: Effect.fn("BinaryReader.read")(function* (need: number) {
      yield* checkSize(need);
      const start = yield* Ref.getAndUpdate(offset, (val) => val + need);
      return buffer.subarray(start, start + need);
    }),
    rest: Effect.fn("BinaryReader.rest")(function* () {
      const start = yield* Ref.getAndUpdate(offset, () => buffer.length);
      return buffer.subarray(start);
    }),
    readLsn: Effect.fn("BinaryReader.readLsn")(function* () {
      const val = yield* readUint64();
      if (val === 0n) return Option.none<Lsn.Lsn>();
      return Option.some(Lsn.make(val));
    }),
    readTime: Effect.fn("BinaryReader.readTime")(function* () {
      return PgTimestamp.fromWire(yield* readUint64());
    }),
    decodeText(strBuf: Uint8Array<ArrayBufferLike>) {
      return textDecoder.decode(strBuf);
    },
  };
});

export type BinaryReader = Effect.Success<ReturnType<typeof make>>;
