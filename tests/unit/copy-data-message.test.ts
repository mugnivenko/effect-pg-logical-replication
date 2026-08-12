import { faker } from "@faker-js/faker";

import { describe, it, expect } from "@effect/vitest";
import { Effect, Exit, pipe, Predicate } from "effect";

import * as CopyDataMessage from "../../src/copy-data-message";
import * as StandbyStatusUpdate from "../../src/standby-status-update";
import * as Lsn from "../../src/lsn";
import * as PgTimestamp from "../../src/pg-timestamp";

import { WireMother } from "../support/wire-mother";

describe("copy-data-message", () => {
  it.effect("parses an WALData frame and reads the payload as the rest", () =>
    Effect.gen(function* () {
      const { parse } = yield* CopyDataMessage.make();

      const walStart = Lsn.make(faker.number.bigInt());
      const walEnd = Lsn.make(faker.number.bigInt());
      const rawTs = PgTimestamp.fromWire(faker.number.bigInt());
      const payload = Buffer.from([0x01, 0x02, 0x03, 0x04]);

      const msg = yield* parse(WireMother.walData({ walStart, walEnd, rawTs, payload }));

      expect(msg).toSatisfy(Predicate.isTagged("WalData"));
      if (!Predicate.isTagged("WalData")(msg)) return;
      expect(msg.walStart).toStrictEqual(walStart);
      expect(msg.walEnd).toStrictEqual(walEnd);
      expect(msg.timestamp).toStrictEqual(rawTs);
      expect(Buffer.from(msg.payload)).toEqual(payload);
    }),
  );

  it("parses a PrimaryKeepalive frame with a single walEnd and the reply flag", async () =>
    Effect.gen(function* () {
      const { parse } = yield* CopyDataMessage.make();

      const endWal = Lsn.make(faker.number.bigInt());
      const rawTs = PgTimestamp.fromWire(faker.number.bigInt());
      const shouldReply = true;

      const msg = yield* parse(WireMother.keepalive({ endWal, rawTs, shouldReply }));

      expect(msg).toBe(Predicate.isTagged("PrimaryKeepaliveMessage"));
      if (!Predicate.isTagged("PrimaryKeepaliveMessage")(msg)) return;
      expect(msg.endWal).toStrictEqual(endWal);
      expect(msg.timestamp).toStrictEqual(rawTs);
      expect(msg.shouldReply).toBe(shouldReply);
    }));

  it("fails with UnexpectedCopyDataMessage on an unknown identifier", async () =>
    Effect.gen(function* () {
      const bad = Buffer.from([0x5a, 0, 0, 0, 0]);
      const { parse } = yield* CopyDataMessage.make();
      const exit = yield* parse(bad).pipe(Effect.exit);

      expect(exit).toSatisfy(Exit.isFailure);
    }));
});

describe("standby-status-update", () => {
  it("builds a 34-byte 'r' frame confirming endWal in all three slots", () =>
    Effect.gen(function* () {
      const endWal = Lsn.make(faker.number.bigInt());
      const buf = yield* StandbyStatusUpdate.make({ endWal, ping: false });

      expect(buf.length).toBe(34);
      expect(buf.readUInt8(0)).toBe(0x72);

      const acked = pipe(endWal, Lsn.add(Lsn.make(1n)));
      expect(buf.readBigUInt64BE(1)).toBe(acked);
      expect(buf.readBigUInt64BE(9)).toBe(acked);
      expect(buf.readBigUInt64BE(17)).toBe(acked);
      expect(buf.readInt8(33)).toBe(0);
    }));

  it("sets the ping byte when requested", () =>
    Effect.gen(function* () {
      const buf = yield* StandbyStatusUpdate.make({ endWal: Lsn.zero(), ping: true });
      expect(buf.readInt8(33)).toBe(1);
    }));
});
