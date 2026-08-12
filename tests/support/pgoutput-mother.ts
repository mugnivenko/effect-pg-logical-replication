import { faker } from "@faker-js/faker";

import * as Lsn from "../../src/lsn.js";
import * as PgTimestamp from "../../src/pg-timestamp.js";
import {
  ColumnFlag,
  MessageIdentifierByte,
  ReplicaIdentity,
  TruncateOption,
  TupleDataMessageIdentifier,
  DeleteTupleData,
  UpdateTupleData,
  TupleDataValueIdentifier,
  type TupleData,
  TupleColumnValue,
} from "../../src/output-plugins/pgoutput/common/types.js";
import { ReplicaIdentityIdByte } from "../../src/output-plugins/pgoutput/common/parser.js";
import { Function, HashSet, Match, Predicate } from "effect";

const REPLICA_IDENTITY_BYTE = Match.type<ReplicaIdentity>().pipe(
  Match.when(ReplicaIdentity.Default, () => ReplicaIdentityIdByte.Default),
  Match.when(ReplicaIdentity.Nothing, () => ReplicaIdentityIdByte.Nothing),
  Match.when(ReplicaIdentity.AllColumns, () => ReplicaIdentityIdByte.AllColumns),
  Match.when(ReplicaIdentity.Index, () => ReplicaIdentityIdByte.Index),
  Match.exhaustive,
);

const encoder = new TextEncoder();

class Writer {
  private readonly chunks: Buffer[] = [];

  uint8(value: number): this {
    const buf = Buffer.alloc(1);
    buf.writeUInt8(value);
    this.chunks.push(buf);
    return this;
  }

  int16(value: number): this {
    const buf = Buffer.alloc(2);
    buf.writeInt16BE(value);
    this.chunks.push(buf);
    return this;
  }

  int32(value: number): this {
    const buf = Buffer.alloc(4);
    buf.writeInt32BE(value);
    this.chunks.push(buf);
    return this;
  }

  uint64(value: bigint): this {
    const buf = Buffer.alloc(8);
    buf.writeBigUInt64BE(value);
    this.chunks.push(buf);
    return this;
  }

  string(value: string): this {
    this.chunks.push(Buffer.from(value, "utf8"));
    return this.uint8(0);
  }

  bytes(value: Buffer | Uint8Array): this {
    this.chunks.push(Buffer.from(value));
    return this;
  }

  lsn(value: Lsn.Lsn): this {
    return this.uint64(Lsn.toBigint(value));
  }

  time(value: PgTimestamp.PgTimestamp): this {
    return this.uint64(PgTimestamp.toWire(value));
  }

  build(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

function writeTupleData(writer: Writer, values: TupleData): Writer {
  const columns = Object.values(values);
  writer.int16(columns.length);
  for (const column of columns) {
    TupleColumnValue.$match(column, {
      Null: () => {
        writer.uint8(TupleDataValueIdentifier.Null);
      },
      Unchanged: () => {
        writer.uint8(TupleDataValueIdentifier.Unchanged);
      },
      Text: ({ value }) => {
        const encoded = encoder.encode(String(value));
        writer.uint8(TupleDataValueIdentifier.Text).int32(encoded.length).bytes(encoded);
      },
      Binary: ({ value }) => {
        writer.uint8(TupleDataValueIdentifier.Binary).int32(value.length).bytes(value);
      },
    });
  }
  return writer;
}

const DEFAULT_LSN = Lsn.make(faker.number.bigInt());
const DEFAULT_TIMESTAMP = PgTimestamp.fromWire(faker.number.bigInt());

export interface ColumnOptions {
  flags?: ColumnFlag;
  name: string;
  dataTypeId?: number;
  typeModifier?: number;
}

export class PgoutputMother {
  static begin({
    finalLsn = DEFAULT_LSN,
    timestamp = DEFAULT_TIMESTAMP,
    xid = faker.number.int({ min: 10, max: 20_000 }),
  }: {
    finalLsn?: Lsn.Lsn;
    timestamp?: PgTimestamp.PgTimestamp;
    xid?: number;
  }): Buffer {
    return new Writer()
      .uint8(MessageIdentifierByte.Begin)
      .lsn(finalLsn)
      .time(timestamp)
      .int32(xid)
      .build();
  }

  static message({
    flags = 0,
    lsn = DEFAULT_LSN,
    prefix = faker.string.alphanumeric(),
    content = faker.string.alphanumeric(),
  }: {
    flags?: number;
    lsn?: Lsn.Lsn;
    prefix?: string;
    content?: Buffer | Uint8Array | string;
  }): Buffer {
    const encoded = Predicate.isString(content) ? encoder.encode(content) : content;
    return new Writer()
      .uint8(MessageIdentifierByte.Message)
      .uint8(flags)
      .lsn(lsn)
      .string(prefix)
      .int32(encoded.length)
      .bytes(encoded)
      .build();
  }

  static commit({
    flags = 0,
    lsn = DEFAULT_LSN,
    endLsn = DEFAULT_LSN,
    timestamp = DEFAULT_TIMESTAMP,
  }: {
    flags?: number;
    lsn?: Lsn.Lsn;
    endLsn?: Lsn.Lsn;
    timestamp?: PgTimestamp.PgTimestamp;
  }): Buffer {
    return new Writer()
      .uint8(MessageIdentifierByte.Commit)
      .uint8(flags)
      .lsn(lsn)
      .lsn(endLsn)
      .time(timestamp)
      .build();
  }

  static origin({
    lsn = DEFAULT_LSN,
    name = faker.string.alphanumeric(),
  }: {
    lsn?: Lsn.Lsn;
    name?: string;
  }): Buffer {
    return new Writer().uint8(MessageIdentifierByte.Origin).lsn(lsn).string(name).build();
  }

  static relation({
    oid = faker.number.int({ min: 10, max: 20_000 }),
    namespace = "public",
    name = "users",
    replicaIdentity = ReplicaIdentity.Default,
    columns = [
      {
        name: "id",
        dataTypeId: faker.number.int({ min: 10_000, max: 20_000 }),
        flags: ColumnFlag.Key,
      },
    ],
  }: {
    oid?: number;
    namespace?: string;
    name?: string;
    replicaIdentity?: ReplicaIdentity;
    columns?: ColumnOptions[];
  }): Buffer {
    const writer = new Writer()
      .uint8(MessageIdentifierByte.Relation)
      .int32(oid)
      .string(namespace)
      .string(name)
      .uint8(REPLICA_IDENTITY_BYTE(replicaIdentity))
      .int16(columns.length);

    for (const column of columns) {
      writer
        .uint8(column.flags ?? ColumnFlag.None)
        .string(column.name)
        .int32(column.dataTypeId ?? faker.number.int({ min: 10_000, max: 20_000 }))
        .int32(column.typeModifier ?? -1);
    }

    return writer.build();
  }

  static type({
    dataTypeId = faker.number.int({ min: 10_000, max: 20_000 }),
    namespace = faker.string.alphanumeric(),
    name = faker.string.alphanumeric(),
  }: {
    dataTypeId?: number;
    namespace?: string;
    name?: string;
  }): Buffer {
    return new Writer()
      .uint8(MessageIdentifierByte.Type)
      .int32(dataTypeId)
      .string(namespace)
      .string(name)
      .build();
  }

  static insert({
    oid = faker.number.int({ min: 10, max: 20_000 }),
    new: newTuple,
  }: {
    oid?: number;
    new: TupleData;
  }): Buffer {
    const writer = new Writer()
      .uint8(MessageIdentifierByte.Insert)
      .int32(oid)
      .uint8(TupleDataMessageIdentifier.NewTuple);
    return writeTupleData(writer, newTuple).build();
  }

  static update({
    oid = faker.number.int({ min: 10, max: 20_000 }),
    tupleData,
    new: newTuple,
  }: {
    oid?: number;
    tupleData: UpdateTupleData;
    new: TupleData;
  }): Buffer {
    const writer = new Writer().uint8(MessageIdentifierByte.Update).int32(oid);

    UpdateTupleData.$match({
      Key: ({ value }) => {
        writer.uint8(TupleDataMessageIdentifier.PrimaryKey);
        writeTupleData(writer, value);
      },
      Old: ({ value }) => {
        writer.uint8(TupleDataMessageIdentifier.OldTuple);
        writeTupleData(writer, value);
      },
      None: Function.constVoid,
    })(tupleData);

    writer.uint8(TupleDataMessageIdentifier.NewTuple);

    return writeTupleData(writer, newTuple).build();
  }

  static delete({
    oid = faker.number.int({ min: 10, max: 20_000 }),
    tupleData,
  }: {
    oid?: number;
    tupleData: DeleteTupleData;
  }): Buffer {
    const writer = new Writer().uint8(MessageIdentifierByte.Delete).int32(oid);

    DeleteTupleData.$match({
      Key: ({ value }) => {
        writer.uint8(TupleDataMessageIdentifier.PrimaryKey);
        writeTupleData(writer, value);
      },
      Old: ({ value }) => {
        writer.uint8(TupleDataMessageIdentifier.OldTuple);
        writeTupleData(writer, value);
      },
    })(tupleData);

    return writer.build();
  }

  static truncate({
    relationOids = [faker.number.int({ min: 10, max: 20_000 })],
    options,
  }: {
    relationOids?: number[];
    options?: HashSet.HashSet<TruncateOption>;
  }): Buffer {
    const flags = Match.value(options).pipe(
      Match.when(Match.undefined, () => 0),
      Match.whenAnd(
        (options) => HashSet.has(options, TruncateOption.Cascade),
        (options) => HashSet.has(options, TruncateOption.RestartIdentity),
        () => TruncateOption.Cascade | TruncateOption.RestartIdentity,
      ),
      Match.when(
        (options) => HashSet.has(options, TruncateOption.Cascade),
        () => TruncateOption.Cascade,
      ),
      Match.when(
        (options) => HashSet.has(options, TruncateOption.RestartIdentity),
        () => TruncateOption.RestartIdentity,
      ),
      Match.orElseAbsurd,
    );

    const writer = new Writer()
      .uint8(MessageIdentifierByte.Truncate)
      .int32(relationOids.length)
      .uint8(flags);

    for (const oid of relationOids) writer.int32(oid);

    return writer.build();
  }
}
