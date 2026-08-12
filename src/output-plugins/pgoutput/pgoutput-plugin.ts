import { Effect, Match } from "effect";

import * as OuptutPlugin from "../../output-plugin.js";
import { Config } from "../../logical-replication-service.js";
import { Client } from "../../client.js";
import { PgError } from "../../errors/index.js";
import * as Lsn from "../../lsn.js";

import { Options } from "./types.js";
import { PgoutputParserV1 } from "./v1/index.js";
import { PgoutputParserV2 } from "./v2/index.js";
import { ParseError } from "./errors.js";
import { LogicalReplicationMessageV1 } from "./v1/types.js";

export const make = Effect.fnUntraced(function* <M = LogicalReplicationMessageV1, E = ParseError>(
  options: Options,
) {
  const parser = yield* Match.value(options.protoVersion).pipe(
    Match.when(1, () => PgoutputParserV1.make()),
    Match.when(2, () => PgoutputParserV2.make()),
    Match.exhaustive,
  );

  return OuptutPlugin.make<M, E>({
    name: "pgoutput",
    parse(buffer: Uint8Array<ArrayBufferLike>, config?: Config): Effect.Effect<M, E> {
      return parser.parse(buffer, config) as Effect.Effect<M, E>;
    },
    start(
      client: Client["Service"],
      slotName: string,
      lsn: Lsn.Lsn,
    ): Effect.Effect<unknown, PgError.PgError> {
      const strOptions = [
        `proto_version '${options.protoVersion}'`,
        `publication_names '${options.publicationNames.join(",")}'`,
        `messages '${options.messages ?? false}'`,
      ];

      const lastLsn = Lsn.toString(lsn);

      const sql = `START_REPLICATION SLOT "${slotName}" LOGICAL ${lastLsn} (${strOptions.join(
        ", ",
      )})`;

      return client.query(sql);
    },
  });
});
