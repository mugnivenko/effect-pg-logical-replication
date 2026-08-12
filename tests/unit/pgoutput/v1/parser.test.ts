import { describe, expect, it } from "@effect/vitest";

import { faker } from "@faker-js/faker";

import { Cause, Effect, Exit, HashSet, Option, pipe, Predicate, Result, String } from "effect";

import { PgoutputParserV1 } from "../../../../src/output-plugins/pgoutput/v1";
import { PgoutputMother } from "../../../support/pgoutput-mother";
import * as Lsn from "../../../../src/lsn";
import * as PgTimestamp from "../../../../src/pg-timestamp";
import {
  ColumnFlag,
  DeleteTupleData,
  ReplicaIdentity,
  TruncateOption,
  TupleColumnValue,
  UpdateTupleData,
} from "../../../../src/output-plugins/pgoutput/common/types";

const int32 = () => faker.number.int({ min: -2147483648, max: 2147483647 });

const generateOid = () => faker.number.int({ max: 5000 });

const randomLsn = () => Lsn.make(faker.number.bigInt());

const randomTimestamp = () => PgTimestamp.fromWire(faker.number.bigInt());

const randomDataTypeId = () => faker.number.int({ min: 10_000, max: 20_000 });

const randomIdentifier = () => faker.string.alphanumeric({ length: { min: 2, max: 256 } });

const uniqueIdentifiers = (count: number) => faker.helpers.uniqueArray(randomIdentifier, count);

describe("PgoutputParserV1", () => {
  describe("begin", () => {
    it.effect("parses a Begin message and preserves xid, finalLsn, and timestamp", () =>
      Effect.gen(function* () {
        const xid = int32();
        const lsn = randomLsn();
        const timestamp = randomTimestamp();
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.begin({ xid, finalLsn: lsn, timestamp });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Begin"));
        if (!Predicate.isTagged("Begin")(res)) return;
        expect(res.xid).toStrictEqual(xid);
        expect(res.finalLsn).toStrictEqual(Option.some(lsn));
        expect(res.timestamp).toStrictEqual(timestamp);
      }),
    );

    it.effect("parses a Begin message with a zero finalLsn as None", () =>
      Effect.gen(function* () {
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.begin({ finalLsn: Lsn.zero() });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Begin"));
        if (!Predicate.isTagged("Begin")(res)) return;
        expect(res.finalLsn).toStrictEqual(Option.none());
      }),
    );
  });

  describe("message", () => {
    it.effect(
      "parses a non-transactional Message and preserves flags, lsn, prefix, and content",
      () =>
        Effect.gen(function* () {
          const flags = 0;
          const lsn = randomLsn();
          const prefix = faker.string.sample();
          const content = faker.string.sample();
          const parser = yield* PgoutputParserV1.make();
          const buf = PgoutputMother.message({ flags, lsn, prefix, content });
          const res = yield* parser.parse(buf);

          expect(res).toSatisfy(Predicate.isTagged("Message"));
          if (!Predicate.isTagged("Message")(res)) return;
          expect(res.flags).toStrictEqual(flags);
          expect(res.transactional).toStrictEqual(false);
          expect(res.lsn).toStrictEqual(Option.some(lsn));
          expect(res.prefix).toStrictEqual(prefix);
          expect(res.contentLength).toStrictEqual(content.length);
          expect(res.content).toStrictEqual(Buffer.from(content));
        }),
    );

    it.effect("marks a Message transactional when flags is set", () =>
      Effect.gen(function* () {
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.message({ flags: 1 });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Message"));
        if (!Predicate.isTagged("Message")(res)) return;
        expect(res.flags).toStrictEqual(1);
        expect(res.transactional).toStrictEqual(true);
      }),
    );

    it.effect("parses a Message with a zero LSN as None", () =>
      Effect.gen(function* () {
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.message({ lsn: Lsn.zero() });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Message"));
        if (!Predicate.isTagged("Message")(res)) return;
        expect(res.lsn).toStrictEqual(Option.none());
      }),
    );

    it.effect("parses a Message with empty content", () =>
      Effect.gen(function* () {
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.message({ content: String.empty });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Message"));
        if (!Predicate.isTagged("Message")(res)) return;
        expect(res.contentLength).toStrictEqual(0);
        expect(res.content).toStrictEqual(Buffer.alloc(0));
      }),
    );
  });

  describe("commit", () => {
    it.effect("parses a Commit message and preserves flags, lsn, endLsn, and timestamp", () =>
      Effect.gen(function* () {
        const flags = 0;
        const lsn = randomLsn();
        const endLsn = randomLsn();
        const timestamp = randomTimestamp();
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.commit({ flags, lsn, endLsn, timestamp });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Commit"));
        if (!Predicate.isTagged("Commit")(res)) return;
        expect(res.flags).toStrictEqual(flags);
        expect(res.lsn).toStrictEqual(Option.some(lsn));
        expect(res.endLsn).toStrictEqual(Option.some(endLsn));
        expect(res.timestamp).toStrictEqual(timestamp);
      }),
    );

    it.effect("parses a Commit message with zero LSNs as None", () =>
      Effect.gen(function* () {
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.commit({ lsn: Lsn.zero(), endLsn: Lsn.zero() });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Commit"));
        if (!Predicate.isTagged("Commit")(res)) return;
        expect(res.lsn).toStrictEqual(Option.none());
        expect(res.endLsn).toStrictEqual(Option.none());
      }),
    );
  });

  describe("origin", () => {
    it.effect("parses an Origin message and preserves lsn and name", () =>
      Effect.gen(function* () {
        const lsn = randomLsn();
        const name = randomIdentifier();
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.origin({ lsn, name });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Origin"));
        if (!Predicate.isTagged("Origin")(res)) return;
        expect(res.lsn).toStrictEqual(Option.some(lsn));
        expect(res.name).toStrictEqual(name);
      }),
    );

    it.effect("parses an Origin message with a zero LSN as None", () =>
      Effect.gen(function* () {
        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.origin({ lsn: Lsn.zero() });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Origin"));
        if (!Predicate.isTagged("Origin")(res)) return;
        expect(res.lsn).toStrictEqual(Option.none());
      }),
    );
  });

  describe("relation", () => {
    it.effect("parses a Relation and preserves oid, namespace, name, columns, and keyColumns", () =>
      Effect.gen(function* () {
        const oid = generateOid();
        const namespace = randomIdentifier();
        const name = randomIdentifier();
        const [idColumn, noteColumn] = uniqueIdentifiers(2);
        const idDataTypeId = randomDataTypeId();
        const noteDataTypeId = randomDataTypeId();

        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.relation({
          oid,
          namespace,
          name,
          columns: [
            { name: idColumn, dataTypeId: idDataTypeId, flags: ColumnFlag.Key, typeModifier: -1 },
            { name: noteColumn, dataTypeId: noteDataTypeId },
          ],
        });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Relation"));
        if (!Predicate.isTagged("Relation")(res)) return;
        expect(res.oid).toStrictEqual(oid);
        expect(res.namespace).toStrictEqual(namespace);
        expect(res.name).toStrictEqual(name);
        expect(res.replicaIdentity).toStrictEqual(ReplicaIdentity.Default);
        expect(res.keyColumns).toStrictEqual([idColumn]);
        expect(res.columns).toHaveLength(2);
        expect(res.columns[0]).toMatchObject({
          flags: ColumnFlag.Key,
          name: idColumn,
          dataTypeId: idDataTypeId,
          typeModifier: -1,
          dataTypeName: Option.none(),
          dataTypeNamespace: Option.none(),
        });
        expect(res.columns[1]).toMatchObject({
          flags: ColumnFlag.None,
          name: noteColumn,
          dataTypeId: noteDataTypeId,
          dataTypeName: Option.none(),
          dataTypeNamespace: Option.none(),
        });
      }),
    );

    it.effect(
      "resolves dataTypeName and dataTypeNamespace from a previously parsed Type message",
      () =>
        Effect.gen(function* () {
          const dataTypeId = randomDataTypeId();
          const typeNamespace = randomIdentifier();
          const typeName = randomIdentifier();
          const columnName = randomIdentifier();

          const parser = yield* PgoutputParserV1.make();

          yield* parser.parse(
            PgoutputMother.type({ dataTypeId, namespace: typeNamespace, name: typeName }),
          );

          const res = yield* parser.parse(
            PgoutputMother.relation({
              columns: [{ name: columnName, dataTypeId }],
            }),
          );

          expect(res).toSatisfy(Predicate.isTagged("Relation"));
          if (!Predicate.isTagged("Relation")(res)) return;
          expect(res.columns[0].dataTypeName).toStrictEqual(Option.some(typeName));
          expect(res.columns[0].dataTypeNamespace).toStrictEqual(Option.some(typeNamespace));
        }),
    );

    it.effect.each(Object.values(ReplicaIdentity))(
      "decodes every ReplicaIdentity variant: %s",
      (replicaIdentity) =>
        Effect.gen(function* () {
          const parser = yield* PgoutputParserV1.make();

          const res = yield* parser.parse(
            PgoutputMother.relation({ oid: generateOid(), replicaIdentity }),
          );

          expect(res).toSatisfy(Predicate.isTagged("Relation"));
          if (!Predicate.isTagged("Relation")(res)) return;
          expect(res.replicaIdentity).toStrictEqual(replicaIdentity);
        }),
    );
  });

  describe("type", () => {
    it.effect("parses a Type message and preserves dataTypeId, namespace, and name", () =>
      Effect.gen(function* () {
        const dataTypeId = randomDataTypeId();
        const namespace = randomIdentifier();
        const name = randomIdentifier();

        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.type({ dataTypeId, namespace, name });
        const res = yield* parser.parse(buf);

        expect(res).toSatisfy(Predicate.isTagged("Type"));
        if (!Predicate.isTagged("Type")(res)) return;
        expect(res.dataTypeId).toStrictEqual(dataTypeId);
        expect(res.namespace).toStrictEqual(namespace);
        expect(res.name).toStrictEqual(name);
      }),
    );
  });

  describe("insert", () => {
    it.effect(
      "parses an Insert and decodes each TupleColumnValue kind (Null, Unchanged, Text, Binary)",
      () =>
        Effect.gen(function* () {
          const oid = generateOid();
          const [first, second, third, fourth] = uniqueIdentifiers(4);
          const columns = [first, second, third, fourth].map((name) => ({ name }));
          const newTuple = {
            [first]: TupleColumnValue.Text({ value: faker.string.alphanumeric() }),
            [second]: TupleColumnValue.Null(),
            [third]: TupleColumnValue.Unchanged(),
            [fourth]: TupleColumnValue.Binary({ value: Buffer.from(randomIdentifier()) }),
          };

          const parser = yield* PgoutputParserV1.make();

          yield* parser.parse(PgoutputMother.relation({ oid, columns }));

          const res = yield* parser.parse(PgoutputMother.insert({ oid, new: newTuple }));

          expect(res).toSatisfy(Predicate.isTagged("Insert"));
          if (!Predicate.isTagged("Insert")(res)) return;
          expect(res.new).toStrictEqual(newTuple);
        }),
    );

    it.effect("fails with MissingRelation when the relation was never registered", () =>
      Effect.gen(function* () {
        const oid = generateOid();
        const columnName = randomIdentifier();

        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.insert({ oid, new: { [columnName]: TupleColumnValue.Null() } });
        const exit = yield* parser.parse(buf).pipe(Effect.exit);

        expect(exit).toSatisfy(Exit.isFailure);
        expect(exit).toSatisfy(Exit.hasFails);
        if (Exit.isSuccess(exit)) return;
        const failure = pipe(exit.cause, Cause.findFail, Result.getOrThrow);
        expect(failure.error).toSatisfy(Predicate.isTagged("MissingRelation"));
      }),
    );
  });

  describe("update", () => {
    it.effect(
      "parses an Update keyed by the primary key and preserves the key and new tuples",
      () =>
        Effect.gen(function* () {
          const oid = generateOid();
          const [idColumn, noteColumn] = uniqueIdentifiers(2);
          const columns = [{ name: idColumn, flags: ColumnFlag.Key }, { name: noteColumn }];

          const tupleData = UpdateTupleData.Key({
            value: { [idColumn]: TupleColumnValue.Text({ value: faker.string.numeric() }) },
          });
          const newTuple = {
            [idColumn]: TupleColumnValue.Text({ value: faker.string.numeric() }),
            [noteColumn]: TupleColumnValue.Text({ value: randomIdentifier() }),
          };

          const parser = yield* PgoutputParserV1.make();

          yield* parser.parse(PgoutputMother.relation({ oid, columns }));

          const res = yield* parser.parse(PgoutputMother.update({ oid, tupleData, new: newTuple }));

          expect(res).toSatisfy(Predicate.isTagged("Update"));
          if (!Predicate.isTagged("Update")(res)) return;
          expect(res.relation.oid).toStrictEqual(oid);
          expect(res.tupleData).toStrictEqual(tupleData);
          expect(res.new).toStrictEqual(newTuple);
        }),
    );

    it.effect("parses an Update carrying the full old tuple (REPLICA IDENTITY FULL)", () =>
      Effect.gen(function* () {
        const oid = generateOid();
        const [idColumn, noteColumn] = uniqueIdentifiers(2);
        const columns = [{ name: idColumn, flags: ColumnFlag.Key }, { name: noteColumn }];

        const tupleData = UpdateTupleData.Old({
          value: {
            [idColumn]: TupleColumnValue.Text({ value: faker.string.numeric() }),
            [noteColumn]: TupleColumnValue.Text({ value: randomIdentifier() }),
          },
        });
        const newTuple = {
          [idColumn]: TupleColumnValue.Text({ value: faker.string.numeric() }),
          [noteColumn]: TupleColumnValue.Text({ value: randomIdentifier() }),
        };

        const parser = yield* PgoutputParserV1.make();

        yield* parser.parse(PgoutputMother.relation({ oid, columns }));

        const res = yield* parser.parse(PgoutputMother.update({ oid, tupleData, new: newTuple }));

        expect(res).toSatisfy(Predicate.isTagged("Update"));
        if (!Predicate.isTagged("Update")(res)) return;
        expect(res.tupleData).toStrictEqual(tupleData);
        expect(res.new).toStrictEqual(newTuple);
      }),
    );

    it.effect("parses an Update with no key or old tuple as None", () =>
      Effect.gen(function* () {
        const oid = generateOid();
        const columnName = randomIdentifier();
        const columns = [{ name: columnName }];

        const newTuple = { [columnName]: TupleColumnValue.Text({ value: randomIdentifier() }) };

        const parser = yield* PgoutputParserV1.make();

        yield* parser.parse(PgoutputMother.relation({ oid, columns }));

        const res = yield* parser.parse(
          PgoutputMother.update({ oid, tupleData: UpdateTupleData.None(), new: newTuple }),
        );

        expect(res).toSatisfy(Predicate.isTagged("Update"));
        if (!Predicate.isTagged("Update")(res)) return;
        expect(res.tupleData).toStrictEqual(UpdateTupleData.None());
        expect(res.new).toStrictEqual(newTuple);
      }),
    );

    it.effect("fails with MissingRelation when the relation was never registered", () =>
      Effect.gen(function* () {
        const oid = generateOid();
        const columnName = randomIdentifier();

        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.update({
          oid,
          tupleData: UpdateTupleData.None(),
          new: { [columnName]: TupleColumnValue.Null() },
        });
        const exit = yield* parser.parse(buf).pipe(Effect.exit);

        expect(exit).toSatisfy(Exit.isFailure);
        expect(exit).toSatisfy(Exit.hasFails);
        if (Exit.isSuccess(exit)) return;
        const failure = pipe(exit.cause, Cause.findFail, Result.getOrThrow);
        expect(failure.error).toSatisfy(Predicate.isTagged("MissingRelation"));
      }),
    );
  });

  describe("delete", () => {
    it.effect(
      "parses a Delete keyed by the primary key against a previously registered relation",
      () =>
        Effect.gen(function* () {
          const oid = generateOid();
          const idValue = faker.string.numeric();
          const name = randomIdentifier();

          const parser = yield* PgoutputParserV1.make();

          yield* parser.parse(PgoutputMother.relation({ oid, name }));

          const data = DeleteTupleData.Key({
            value: { id: TupleColumnValue.Text({ value: idValue }) },
          });
          const res = yield* parser.parse(PgoutputMother.delete({ oid, tupleData: data }));

          expect(res).toSatisfy(Predicate.isTagged("Delete"));
          if (!Predicate.isTagged("Delete")(res)) return;
          expect(res.relation.oid).toStrictEqual(oid);
          expect(res.relation.name).toStrictEqual(name);
          expect(res.relation.keyColumns).toStrictEqual(["id"]);
          expect(res.tupleData).toStrictEqual(data);
        }),
    );

    it.effect("fails with MissingRelation when the relation was never registered", () =>
      Effect.gen(function* () {
        const oid = generateOid();

        const key = faker.string.alpha();
        const data = DeleteTupleData.Key({ value: { [key]: TupleColumnValue.Null() } });

        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.delete({ oid, tupleData: data });
        const exit = yield* parser.parse(buf).pipe(Effect.exit);

        expect(exit).toSatisfy(Exit.isFailure);
        expect(exit).toSatisfy(Exit.hasFails);
        if (Exit.isSuccess(exit)) return;
        const failure = pipe(exit.cause, Cause.findFail, Result.getOrThrow);
        expect(failure.error).toSatisfy(Predicate.isTagged("MissingRelation"));
      }),
    );
  });

  describe("truncate", () => {
    it.effect("parses a Truncate for previously registered relations with no options", () =>
      Effect.gen(function* () {
        const [oidA, oidB] = faker.helpers.uniqueArray(generateOid, 2);

        const parser = yield* PgoutputParserV1.make();

        yield* parser.parse(PgoutputMother.relation({ oid: oidA }));
        yield* parser.parse(PgoutputMother.relation({ oid: oidB }));

        const res = yield* parser.parse(PgoutputMother.truncate({ relationOids: [oidA, oidB] }));

        expect(res).toSatisfy(Predicate.isTagged("Truncate"));
        if (!Predicate.isTagged("Truncate")(res)) return;
        expect(HashSet.size(res.options)).toStrictEqual(0);
        expect(res.relations.map((relation) => relation.oid)).toStrictEqual([oidA, oidB]);
      }),
    );

    it.effect("parses a Truncate with the Cascade option", () =>
      Effect.gen(function* () {
        const oid = generateOid();

        const parser = yield* PgoutputParserV1.make();

        yield* parser.parse(PgoutputMother.relation({ oid }));

        const res = yield* parser.parse(
          PgoutputMother.truncate({
            relationOids: [oid],
            options: HashSet.make(TruncateOption.Cascade),
          }),
        );

        expect(res).toSatisfy(Predicate.isTagged("Truncate"));
        if (!Predicate.isTagged("Truncate")(res)) return;
        expect(HashSet.has(res.options, TruncateOption.Cascade)).toBe(true);
        expect(HashSet.has(res.options, TruncateOption.RestartIdentity)).toBe(false);
      }),
    );

    it.effect("parses a Truncate with the RestartIdentity option", () =>
      Effect.gen(function* () {
        const oid = generateOid();

        const parser = yield* PgoutputParserV1.make();

        yield* parser.parse(PgoutputMother.relation({ oid }));

        const res = yield* parser.parse(
          PgoutputMother.truncate({
            relationOids: [oid],
            options: HashSet.make(TruncateOption.RestartIdentity),
          }),
        );

        expect(res).toSatisfy(Predicate.isTagged("Truncate"));
        if (!Predicate.isTagged("Truncate")(res)) return;
        expect(HashSet.has(res.options, TruncateOption.RestartIdentity)).toBe(true);
        expect(HashSet.has(res.options, TruncateOption.Cascade)).toBe(false);
      }),
    );

    it.effect("parses a Truncate with both Cascade and RestartIdentity", () =>
      Effect.gen(function* () {
        const oid = generateOid();

        const parser = yield* PgoutputParserV1.make();

        yield* parser.parse(PgoutputMother.relation({ oid }));

        const res = yield* parser.parse(
          PgoutputMother.truncate({
            relationOids: [oid],
            options: HashSet.make(TruncateOption.Cascade, TruncateOption.RestartIdentity),
          }),
        );

        expect(res).toSatisfy(Predicate.isTagged("Truncate"));
        if (!Predicate.isTagged("Truncate")(res)) return;
        expect(HashSet.has(res.options, TruncateOption.Cascade)).toBe(true);
        expect(HashSet.has(res.options, TruncateOption.RestartIdentity)).toBe(true);
      }),
    );

    it.effect("fails with MissingRelation when a relation was never registered", () =>
      Effect.gen(function* () {
        const oid = generateOid();

        const parser = yield* PgoutputParserV1.make();
        const buf = PgoutputMother.truncate({ relationOids: [oid] });
        const exit = yield* parser.parse(buf).pipe(Effect.exit);

        expect(exit).toSatisfy(Exit.isFailure);
        expect(exit).toSatisfy(Exit.hasFails);
        if (Exit.isSuccess(exit)) return;
        const failure = pipe(exit.cause, Cause.findFail, Result.getOrThrow);
        expect(failure.error).toSatisfy(Predicate.isTagged("MissingRelation"));
      }),
    );
  });
});
