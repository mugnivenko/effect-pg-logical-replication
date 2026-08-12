import { Data, Effect, Match } from "effect";

import * as BinaryReader from "./binary-reader.js";

import { UnexpectedCopyDataMessage } from "./errors/index.js";
import * as Lsn from "./lsn.js";
import * as PgTimestamp from "./pg-timestamp.js";

enum CopyDataMessageIdentifier {
  WALData = /* w */ 0x77,
  PrimaryKeepaliveMessage = /* k */ 0x6b,
}

export type CopyDataMessage = Data.TaggedEnum<{
  WalData: {
    walStart: Lsn.Lsn;
    walEnd: Lsn.Lsn;
    timestamp: PgTimestamp.PgTimestamp;
    payload: Uint8Array<ArrayBufferLike>;
  };
  PrimaryKeepaliveMessage: {
    endWal: Lsn.Lsn;
    timestamp: PgTimestamp.PgTimestamp;
    shouldReply: boolean;
  };
}>;

export const { WalData, PrimaryKeepaliveMessage, $match } = Data.taggedEnum<CopyDataMessage>();

export const make = Effect.fnUntraced(function* () {
  return {
    parse: Effect.fn("CopyDataMessageParser.parse")(function* (buffer: Buffer<ArrayBufferLike>) {
      const reader = yield* BinaryReader.make(buffer);

      const identifier = yield* reader.readUint8();

      return yield* Match.value(identifier).pipe(
        Match.when(
          CopyDataMessageIdentifier.WALData,
          Effect.fn("CopyDataMessageParser.parseWalData")(function* () {
            const walStart = yield* reader.readUint64();
            const walEnd = yield* reader.readUint64();

            const timestamp = yield* reader.readTime();

            const payload = yield* reader.rest();

            return WalData({
              walStart: Lsn.make(walStart),
              walEnd: Lsn.make(walEnd),
              timestamp,
              payload,
            });
          }),
        ),
        Match.when(
          CopyDataMessageIdentifier.PrimaryKeepaliveMessage,
          Effect.fn("CopyDataMessageParser.parsePrimaryKeepaliveMessage")(function* () {
            const endWal = yield* reader.readUint64();

            const timestamp = yield* reader.readTime();

            const shouldReply = yield* reader.readUint8();

            return PrimaryKeepaliveMessage({
              endWal: Lsn.make(endWal),
              timestamp,
              shouldReply: Boolean(shouldReply),
            });
          }),
        ),
        Match.orElse(() =>
          Effect.fail(
            new UnexpectedCopyDataMessage({ identifier: String.fromCharCode(identifier) }),
          ),
        ),
      );
    }),
  };
});
