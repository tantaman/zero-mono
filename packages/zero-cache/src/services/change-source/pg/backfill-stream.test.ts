import {describe, expect, test} from 'vitest';
import type {PublishedTableSpec} from '../../../db/specs.ts';
import {
  backfillCursor,
  BackfillResumeUnsupportedError,
} from './backfill-stream.ts';

function spec(): PublishedTableSpec {
  return {
    schema: 'public',
    name: 't',
    columns: {
      id: {dataType: 'int8'},
      name: {dataType: 'text'},
      code: {dataType: 'character varying'},
      u: {dataType: 'uuid'},
      flag: {dataType: 'bool'},
    },
    publications: {pub1: {rowFilter: null}},
  } as unknown as PublishedTableSpec;
}

describe('backfillCursor', () => {
  test('orders by the row key, bytewise for text-family columns', () => {
    expect(backfillCursor(spec(), ['id', 'name', 'code', 'u'])).toEqual({
      orderBy: `ORDER BY "id","name" COLLATE "C","code" COLLATE "C","u"`,
    });
  });

  test('resumeAfter produces a strictly-after row comparison', () => {
    expect(backfillCursor(spec(), ['id', 'name'], [5, 'bob'])).toEqual({
      orderBy: `ORDER BY "id","name" COLLATE "C"`,
      where: `("id","name" COLLATE "C") > (5,E'bob')`,
    });
  });

  test('string literals escape quotes and backslashes', () => {
    expect(backfillCursor(spec(), ['name'], [`a'b\\c`])).toEqual({
      orderBy: `ORDER BY "name" COLLATE "C"`,
      where: `("name" COLLATE "C") > (E'a''b\\\\c')`,
    });
  });

  test('boolean and numeric key values', () => {
    expect(backfillCursor(spec(), ['flag', 'id'], [false, 2.5]).where).toBe(
      `("flag","id") > (false,2.5)`,
    );
  });

  test('key length mismatch is unsupported', () => {
    expect(() => backfillCursor(spec(), ['id', 'name'], [5])).toThrow(
      BackfillResumeUnsupportedError,
    );
  });

  test.each([
    ['null', null],
    ['non-finite number', Infinity],
    ['NaN', NaN],
    ['array', [1]],
    ['object', {a: 1}],
  ])('%s key value is unsupported', (_name, val) => {
    expect(() => backfillCursor(spec(), ['id'], [val])).toThrow(
      BackfillResumeUnsupportedError,
    );
  });
});
