import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
/* oxlint-disable no-console */
/**
 * Assembles results.json (and optionally field-cost.json) into a
 * self-contained HTML report.
 *
 *   node src/report.ts --results results.json --chunks ./chunks --out report.html
 */
import {parseArgs} from 'node:util';

const HERE = dirname(fileURLToPath(import.meta.url));

const {values} = parseArgs({
  options: {
    'results': {type: 'string', default: 'results.json'},
    'field-cost': {type: 'string', default: 'field-cost.json'},
    'chunks': {type: 'string', default: './chunks'},
    /** Chunk to quote as the wire-format sample. */
    'sample': {type: 'string', default: 'chinook--insert-track-single'},
    'out': {type: 'string', default: 'report.html'},
  },
});

const escape = (s: string) =>
  s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');

/** Index of the `}` closing the object that opens at `open`. */
function matchBrace(line: string, open: number): number {
  let depth = 0;
  let inStr = false;
  for (let i = open; i < line.length; i++) {
    const c = line[i];
    if (inStr) {
      if (c === '\\') i++;
      else if (c === '"') inStr = false;
    } else if (c === '"') inStr = true;
    else if (c === '{') depth++;
    else if (c === '}' && --depth === 0) return i;
  }
  return -1;
}

const HIGHLIGHTED = [
  ['"relation":', 'r'],
  ['"new":', 'p'],
  ['"key":', 'p'],
] as const;

/** Tints the relation block and the row payload so the split is visible. */
function highlight(line: string): string {
  const spans: {from: number; to: number; cls: string}[] = [];
  for (const [key, cls] of HIGHLIGHTED) {
    const at = line.indexOf(key);
    if (at < 0) continue;
    const open = line.indexOf('{', at);
    const close = matchBrace(line, open);
    if (open > 0 && close > 0) spans.push({from: at, to: close + 1, cls});
  }
  spans.sort((a, b) => a.from - b.from);

  let out = '';
  let cursor = 0;
  for (const {from, to, cls} of spans) {
    if (from < cursor) continue;
    out += escape(line.slice(cursor, from));
    out += `<span class="${cls}">${escape(line.slice(from, to))}</span>`;
    cursor = to;
  }
  return `<span class="k">${out + escape(line.slice(cursor))}</span>`;
}

/** The first complete transaction in the sample chunk. */
function sampleTransaction(): string {
  const path = `${values.chunks}/${values.sample}.ndjson`;
  if (!existsSync(path)) return '';
  const lines = readFileSync(path, {encoding: 'utf8'})
    .slice(0, 1 << 16)
    .split('\n');
  const begin = lines.findIndex(l => l.startsWith('["begin"'));
  const commit = lines.findIndex(
    (l, i) => i > begin && l.startsWith('["commit"'),
  );
  if (begin < 0 || commit < 0) return '';
  return lines
    .slice(begin, commit + 1)
    .map(highlight)
    .join('\n\n');
}

const results = JSON.parse(readFileSync(values.results!, 'utf8'));
const fieldCost = existsSync(values['field-cost']!)
  ? JSON.parse(readFileSync(values['field-cost']!, 'utf8'))
  : [];

const data = {results, fieldCost, sample: sampleTransaction()};
const html = readFileSync(resolve(HERE, '../report/template.html'), 'utf8')
  .replace('__DATA__', () =>
    JSON.stringify(data).replaceAll('</script', '<\\/script'),
  )
  .replace('__SCRIPT__', () =>
    readFileSync(resolve(HERE, '../report/client.js'), 'utf8'),
  );

writeFileSync(values.out!, html);
console.log(
  `Wrote ${values.out} (${(html.length / 1024).toFixed(0)} KiB, ` +
    `${results.length} workloads, ${fieldCost.length} field-cost chunks)`,
);
