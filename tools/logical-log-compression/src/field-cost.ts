import {readdirSync, readFileSync, writeFileSync} from 'node:fs';
/* oxlint-disable no-console */
/**
 * Attributes a chunk's compressed size to individual row columns.
 *
 * For each column, the chunk is re-encoded with that column removed and
 * recompressed; the difference is the column's marginal cost on S3. This
 * separates "the row is wide" from "the row contains something incompressible"
 * -- a random UUID costs far more per byte than a timestamp or an id.
 *
 *   node src/field-cost.ts --chunks ./chunks --only pagila--insert-rental-batch100
 */
import {parseArgs} from 'node:util';
import {CODECS} from './codecs.ts';

const {values} = parseArgs({
  options: {
    chunks: {type: 'string', default: './chunks'},
    only: {type: 'string', multiple: true, default: []},
    out: {type: 'string', default: 'field-cost.json'},
  },
});

const zstd3 = CODECS.find(c => c.name === 'zstd-3')!;

type FieldCost = {
  column: string;
  rows: number;
  rawBytesPerRow: number;
  compressedBytesPerRow: number;
  shareOfCompressedChunk: number;
};

/**
 * A table's contribution to the chunk. A workload that touches one table can
 * still put several in the log: zbugs' `update_issue_modified_time_on_comment`
 * trigger ships a whole issue row for every comment inserted.
 */
type RelationCost = {
  relation: string;
  messages: number;
  rawBytes: number;
  /** Marginal compressed bytes: the chunk with these messages removed. */
  compressedBytes: number;
};

type ChunkFieldCost = {
  chunk: string;
  rawBytes: number;
  compressedBytes: number;
  ratio: number;
  relations: RelationCost[];
  columns: FieldCost[];
};

const RELATION_NAME = /"relation":\{[^}]*?"name":"((?:[^"\\]|\\.)*)"/;

/** The table a data message belongs to, or undefined for framing messages. */
function relationOf(line: string): string | undefined {
  if (!line.startsWith('["data"')) return undefined;
  return RELATION_NAME.exec(line)?.[1];
}

/** Re-encodes the chunk with `column` removed from every row payload. */
function without(
  lines: string[],
  column: string,
): {json: string; rows: number} {
  let rows = 0;
  const out = lines.map(line => {
    if (!line.includes(`"${column}"`)) return line;
    const msg = JSON.parse(line);
    if (msg[0] !== 'data') return line;
    let hit = false;
    for (const field of ['new', 'key'] as const) {
      const row = msg[1][field];
      if (row && typeof row === 'object' && column in row) {
        delete row[column];
        hit = true;
      }
    }
    if (!hit) return line;
    rows++;
    return JSON.stringify(msg);
  });
  return {json: out.join('\n') + '\n', rows};
}

const results: ChunkFieldCost[] = [];

for (const file of readdirSync(values.chunks!).sort()) {
  if (!file.endsWith('.ndjson')) continue;
  const key = file.replace(/\.ndjson$/, '');
  if (values.only!.length && !values.only!.includes(key)) continue;

  const raw = readFileSync(`${values.chunks}/${file}`);
  const lines = raw.toString('utf8').split('\n').filter(Boolean);
  const baseline = zstd3.compress(raw).length;

  // Column set, taken from the first data message.
  const columns = new Set<string>();
  for (const line of lines) {
    if (!line.startsWith('["data"')) continue;
    const msg = JSON.parse(line);
    for (const field of ['new', 'key'] as const) {
      if (msg[1][field]) {
        for (const c of Object.keys(msg[1][field])) {
          columns.add(c);
        }
      }
    }
    if (columns.size) break;
  }

  process.stdout.write(
    `${key}: ${(raw.length / baseline).toFixed(1)}x baseline\n`,
  );

  // Per-table split, measured the same leave-one-out way. A workload that
  // touches one table can still put several in the log.
  const byRelation = new Map<string, {messages: number; rawBytes: number}>();
  for (const line of lines) {
    const rel = relationOf(line);
    if (!rel) continue;
    const e = byRelation.get(rel) ?? {messages: 0, rawBytes: 0};
    e.messages++;
    e.rawBytes += Buffer.byteLength(line, 'utf8') + 1;
    byRelation.set(rel, e);
  }
  const relations: RelationCost[] = [];
  if (byRelation.size > 1) {
    for (const [relation, {messages, rawBytes}] of byRelation) {
      const kept = lines.filter(l => relationOf(l) !== relation);
      const compressedBytes =
        baseline - zstd3.compress(Buffer.from(kept.join('\n') + '\n')).length;
      relations.push({relation, messages, rawBytes, compressedBytes});
      console.log(
        `  [table] ${relation.padEnd(14)} ${messages} msgs  ` +
          `raw ${((100 * rawBytes) / raw.length).toFixed(0)}%  ` +
          `stored ${((100 * compressedBytes) / baseline).toFixed(0)}% of chunk`,
      );
    }
  }

  const costs: FieldCost[] = [];
  for (const column of columns) {
    const {json, rows} = without(lines, column);
    if (!rows) continue;
    const stripped = Buffer.from(json, 'utf8');
    const compressed = zstd3.compress(stripped).length;
    const cost = {
      column,
      rows,
      rawBytesPerRow: (raw.length - stripped.length) / rows,
      compressedBytesPerRow: (baseline - compressed) / rows,
      shareOfCompressedChunk: (baseline - compressed) / baseline,
    };
    costs.push(cost);
    console.log(
      `  ${column.padEnd(20)} raw ${cost.rawBytesPerRow.toFixed(1).padStart(6)} B/row  ` +
        `compressed ${cost.compressedBytesPerRow.toFixed(1).padStart(6)} B/row  ` +
        `${(100 * cost.shareOfCompressedChunk).toFixed(0).padStart(3)}% of chunk`,
    );
  }
  costs.sort((a, b) => b.compressedBytesPerRow - a.compressedBytesPerRow);
  relations.sort((a, b) => b.compressedBytes - a.compressedBytes);
  results.push({
    chunk: key,
    rawBytes: raw.length,
    compressedBytes: baseline,
    ratio: raw.length / baseline,
    relations,
    columns: costs,
  });
  writeFileSync(values.out!, JSON.stringify(results, null, 2));
}
console.log(`\nWrote ${results.length} chunks to ${values.out}`);
