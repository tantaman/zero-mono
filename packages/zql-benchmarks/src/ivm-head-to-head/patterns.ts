/**
 * Query shapes for the Zero-vs-rindle IVM head-to-head.
 *
 * Each shape is written as a literal Zero `AST` that is the exact analogue of
 * the rindle fluent query in `rust/rindle-triangle-bench/src/main.rs`
 * (`Pattern::ast`). Writing the AST out rather than going through the typed
 * query builder keeps the two sides visibly aligned and avoids dragging a Zero
 * `Schema` (and its client/server name mapping) into a comparison that is about
 * the IVM engine, not the schema layer.
 *
 * `count` is deliberately absent: Zero's ZQL has no aggregation operator, so
 * rindle's `reduce`-backed group-by/count has no Zero counterpart to race.
 */
import type {AST, Condition} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import {ID_STEP, PUSH_ID, STREAM_ID_BASE, type TableData} from './data.ts';

/**
 * The "many rows filtered out" threshold. In chinook v1.4.5 only 160 / 3503
 * tracks (4.6%) run longer than 40 minutes, and they sit in just 10 / 347
 * albums (2.9%) — versus GenreId=1, which covers 1297 tracks (37%) across 117
 * albums (34%). The pair (`filter` vs `filter_rare`, `exists` vs
 * `exists_rare`) is what isolates selectivity from shape.
 */
export const RARE_MS = 2_400_000;

export type PatternLabel =
  | 'filter'
  | 'filter_rare'
  | 'top50'
  | 'join'
  | 'exists'
  | 'exists_rare';

export const ALL_PATTERNS: readonly PatternLabel[] = [
  'filter',
  'filter_rare',
  'top50',
  'join',
  'exists',
  'exists_rare',
];

const col = (name: string) => ({type: 'column', name}) as const;
const lit = (value: number) => ({type: 'literal', value}) as const;

const cmp = (name: string, op: '=' | '>', value: number): Condition => ({
  type: 'simple',
  op,
  left: col(name),
  right: lit(value),
});

/** `EXISTS (SELECT 1 FROM Track WHERE Track.AlbumId = Album.AlbumId AND ...)`. */
const albumHasTrack = (inner: Condition): Condition => ({
  type: 'correlatedSubquery',
  op: 'EXISTS',
  related: {
    correlation: {parentField: ['AlbumId'], childField: ['AlbumId']},
    subquery: {
      table: 'Track',
      alias: 'has_track',
      where: inner,
    },
  },
});

export function ast(pattern: PatternLabel): AST {
  switch (pattern) {
    case 'filter':
      return {table: 'Track', where: cmp('GenreId', '=', 1)};
    case 'filter_rare':
      return {table: 'Track', where: cmp('Milliseconds', '>', RARE_MS)};
    case 'top50':
      return {
        table: 'Track',
        orderBy: [['Milliseconds', 'desc']],
        limit: 50,
      };
    case 'join':
      return {
        table: 'Album',
        related: [
          {
            correlation: {parentField: ['AlbumId'], childField: ['AlbumId']},
            subquery: {table: 'Track', alias: 'tracks'},
          },
        ],
      };
    case 'exists':
      return {table: 'Album', where: albumHasTrack(cmp('GenreId', '=', 1))};
    case 'exists_rare':
      return {
        table: 'Album',
        where: albumHasTrack(cmp('Milliseconds', '>', RARE_MS)),
      };
    default:
      throw new Error(`unknown pattern ${String(pattern)}`);
  }
}

/** The view `Format` for a pattern — only `join` has a nested relationship. */
export function format(pattern: PatternLabel): Format {
  return pattern === 'join'
    ? {
        singular: false,
        relationships: {tracks: {singular: false, relationships: {}}},
      }
    : {singular: false, relationships: {}};
}

/** Tables the pattern reads. */
export function needs(pattern: PatternLabel): readonly string[] {
  return pattern === 'join' || pattern === 'exists' || pattern === 'exists_rare'
    ? ['Album', 'Track']
    : ['Track'];
}

/** The table the write stream targets. Every pattern writes Tracks. */
export const WRITE_TABLE = 'Track';

/** A Track row in declared column order, as a Zero `Row`. */
export function pushedTrack(
  id: number,
  album: number | null,
  genre: number | null,
  ms: number,
): Row {
  return {
    TrackId: id,
    Name: 'bench',
    AlbumId: album,
    MediaTypeId: 1,
    GenreId: genre,
    Composer: null,
    Milliseconds: ms,
    Bytes: 1000,
    UnitPrice: 0.99,
  };
}

/**
 * An album from the FIRST scaled copy that has no qualifying track for
 * `pattern`, so adding one flips the parent into the result and removing it
 * flips it back out — the EXISTS worst case. Mirrors rindle's `flip_album`.
 */
function flipAlbum(pattern: PatternLabel, data: TableData): number {
  const qualifying = new Set<number>();
  for (const t of data.Track) {
    const album = t.AlbumId;
    if (typeof album !== 'number' || album >= ID_STEP) {
      continue;
    }
    const hit =
      pattern === 'exists_rare'
        ? typeof t.Milliseconds === 'number' && t.Milliseconds > RARE_MS
        : t.GenreId === 1;
    if (hit) {
      qualifying.add(album);
    }
  }
  for (const a of data.Album) {
    const id = a.AlbumId;
    if (typeof id === 'number' && id < ID_STEP && !qualifying.has(id)) {
      return id;
    }
  }
  throw new Error(`no flip album found for ${pattern}`);
}

/**
 * The labeled push cases: `common` is the representative path, `worst` is the
 * expensive one (limit backfill / EXISTS flip). Split and never blended —
 * rindle fairness rule 2.
 */
export function pushCases(
  pattern: PatternLabel,
  data: TableData,
): readonly (readonly [string, Row])[] {
  switch (pattern) {
    case 'filter':
      return [['common', pushedTrack(PUSH_ID, 1, 1, 200_000)]];
    case 'filter_rare':
      return [
        // common: filtered out — the representative path when the predicate
        // rejects ~95% of writes.
        ['common', pushedTrack(PUSH_ID, 1, 1, 200_000)],
        // worst: passes the filter, so the view actually changes.
        ['worst', pushedTrack(PUSH_ID, 1, 1, RARE_MS + 1)],
      ];
    case 'top50':
      return [
        // common: outside the window — the cheap reject.
        ['common', pushedTrack(PUSH_ID, 1, 1, 1)],
        // worst: inside the window — displacement on add, limit backfill on
        // remove, every iteration.
        ['worst', pushedTrack(PUSH_ID, 1, 1, 900_000_000)],
      ];
    case 'join':
      return [['common', pushedTrack(PUSH_ID, 1, 1, 200_000)]];
    case 'exists':
      return [
        // common: album 1 already has genre-1 tracks — no flip.
        ['common', pushedTrack(PUSH_ID, 1, 1, 200_000)],
        // worst: first genre-1 track for a gate-empty album — parent flips.
        ['worst', pushedTrack(PUSH_ID, flipAlbum('exists', data), 1, 200_000)],
      ];
    case 'exists_rare':
      return [
        // common: a normal-length track — rejected by the inner filter, which
        // is what ~95% of writes look like for this shape.
        ['common', pushedTrack(PUSH_ID, 1, 1, 200_000)],
        // worst: a long track for a gate-empty album — parent flips.
        [
          'worst',
          pushedTrack(PUSH_ID, flipAlbum('exists_rare', data), 1, RARE_MS + 1),
        ],
      ];
    default:
      throw new Error(`unknown pattern ${String(pattern)}`);
  }
}

/**
 * The organic write stream: W Track rows whose values are sampled from the base
 * table by strided template selection, with fresh unique ids — so window hits
 * and EXISTS flips occur at their natural rate (rindle fairness rule 3).
 */
export function streamRows(data: TableData, w: number): Row[] {
  const base = data[WRITE_TABLE];
  const stride = 7919; // prime => cycles the table without clustering
  const out: Row[] = [];
  for (let i = 0; i < w; i++) {
    const t = base[(i * stride) % base.length];
    out.push({...t, TrackId: STREAM_ID_BASE + i});
  }
  return out;
}
