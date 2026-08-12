import { describe, expect, it } from "@effect/vitest";

import { faker } from "@faker-js/faker";

import { Cause, Effect, Equal, Exit, Hash, pipe, Predicate, Result } from "effect";

import * as Lsn from "../../src/lsn";
import { LsnMother } from "../support/lsn-mother";

const randomBigint = () => faker.number.bigInt({ max: 0xffff_ffff_ffff_ffffn });

describe("Lsn", () => {
  describe("make / toBigint", () => {
    it("round-trips the underlying bigint value", () => {
      const value = randomBigint();
      expect(Lsn.toBigint(Lsn.make(value))).toBe(value);
    });
  });

  describe("zero", () => {
    it("produces an Lsn with value 0n", () => {
      expect(Lsn.toBigint(Lsn.zero())).toBe(0n);
    });
  });

  describe("isLsn", () => {
    it("returns true for values created via make/zero", () => {
      expect(Lsn.isLsn(Lsn.make(1n))).toBe(true);
      expect(Lsn.isLsn(Lsn.zero())).toBe(true);
    });

    it("returns false for non-Lsn values", () => {
      expect(Lsn.isLsn(1n)).toBe(false);
      expect(Lsn.isLsn("0/0")).toBe(false);
      expect(Lsn.isLsn(null)).toBe(false);
      expect(Lsn.isLsn({})).toBe(false);
    });
  });

  describe("toString", () => {
    it("formats zero as 0/0", () => {
      expect(Lsn.toString(Lsn.zero())).toBe("0/0");
    });

    it("splits the value into high/low 32-bit halves and uppercases hex", () => {
      const { lsn, text } = LsnMother.text();
      expect(Lsn.toString(lsn)).toBe(text);
    });

    it("omits leading zeros on either half", () => {
      const value = 0x1n << 32n;
      expect(Lsn.toString(Lsn.make(value))).toBe("1/0");
    });
  });

  describe("add", () => {
    it("supports data-first application", () => {
      const firstVal = randomBigint();
      const lastVal = randomBigint();
      const first = Lsn.make(firstVal);
      const last = Lsn.make(lastVal);
      const result = Lsn.add(first, last);

      expect(Lsn.toBigint(result)).toBe(firstVal + lastVal);
    });

    it("supports data-last (curried) application", () => {
      const firstVal = randomBigint();
      const lastVal = randomBigint();
      const first = Lsn.make(firstVal);
      const last = Lsn.make(lastVal);
      const result = Lsn.add(first)(last);
      expect(Lsn.toBigint(result)).toBe(firstVal + lastVal);
    });
  });

  describe("fromString", () => {
    it.effect("parses a well-formed LSN string", () =>
      Effect.gen(function* () {
        const { text, lsn: expected } = LsnMother.text();
        const lsn = yield* Lsn.fromString(text);
        expect(Equal.equals(lsn, expected)).toBe(true);
      }),
    );

    it.effect("parses lowercase hex", () =>
      Effect.gen(function* () {
        const { text, lsn: expected } = LsnMother.text({ lowercase: true });
        const lsn = yield* Lsn.fromString(text);
        expect(Equal.equals(lsn, expected)).toBe(true);
      }),
    );

    it.effect("round-trips through toString", () =>
      Effect.gen(function* () {
        const { text } = LsnMother.text();
        const lsn = yield* Lsn.fromString(text);
        expect(Lsn.toString(lsn)).toBe(text);
      }),
    );

    it.effect.each([
      { message: "missing '/' separator", input: "16339B270" },
      { message: "invalid high part", input: "zz/0" },
      { message: "invalid low part", input: "0/zz" },
    ])("fails with ParseLsnError: $message", ({ input }) =>
      Effect.gen(function* () {
        const exit = yield* Lsn.fromString(input).pipe(Effect.exit);

        expect(exit).toSatisfy(Exit.isFailure);
        if (!Exit.isFailure(exit)) return;
        const failure = pipe(exit.cause, Cause.findFail, Result.getOrThrow);
        expect(failure.error).toSatisfy(Predicate.isTagged("ParseLsnError"));
      }),
    );
  });

  describe("Equal / Hash", () => {
    it("considers Lsn values with the same underlying bigint equal", () => {
      const lsn = LsnMother.random();
      expect(Equal.equals(lsn, lsn)).toBe(true);
    });

    it("considers Lsn values with different bigints unequal", () => {
      const first = LsnMother.random();
      const second = LsnMother.random();
      expect(Equal.equals(first, second)).toBe(false);
    });

    it("produces the same hash for equal values", () => {
      const lsn = LsnMother.random();
      expect(Hash.hash(lsn)).toBe(Hash.hash(lsn));
    });
  });
});
