import { faker } from "@faker-js/faker";

import * as Lsn from "../../src/lsn.js";

export class LsnMother {
  static randomHalf(): bigint {
    return faker.number.bigInt({ min: 0n, max: 0xffff_ffffn });
  }

  static random(): Lsn.Lsn {
    return Lsn.make(faker.number.bigInt({ min: 0n, max: 0xffff_ffff_ffff_ffffn }));
  }

  static text({
    high = LsnMother.randomHalf(),
    low = LsnMother.randomHalf(),
    lowercase = false,
  }: {
    high?: bigint;
    low?: bigint;
    lowercase?: boolean;
  } = {}): { text: string; high: bigint; low: bigint; lsn: Lsn.Lsn } {
    const format = (n: bigint) => (lowercase ? n.toString(16) : n.toString(16).toUpperCase());

    return {
      text: `${format(high)}/${format(low)}`,
      high,
      low,
      lsn: Lsn.make((high << 32n) | low),
    };
  }
}
