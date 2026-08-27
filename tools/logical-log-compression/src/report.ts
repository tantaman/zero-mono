/* oxlint-disable no-console */
/**
 * Assembles results.json (and optionally field-cost.json) into a
 * self-contained HTML report.
 *
 *   node src/report.ts --results results.json --chunks ./chunks --out report.html
 */
import {parseArgs} from 'node:util';
import {existsSync, readFileSync, writeFileSync} from 'node:fs';
import {dirname, resolve} from 'node:path';
import {fileURLToPath} from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

const {values} = parseArgs({
  options: {
    results: {type: 'string', default: 'results.json'},
    'field-cost': {type: 'string', default: 'field-cost.json'},
    chunks: {type: 'string', default: './chunks'},
    /** Chunk to quote as the wire-format sample. */
    sample: {type: 'string', default: 'zbugs--insert-viewstate-batch100'},
    out: {type: 'string', default: 'report.html'},
  },
});

const escape = (s: string) =>
  s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');

/** Highlights the relation block and the row payload in a sample line. */
function highlight(line: string): string {
  return escape(line)
    .replace(/(&quot;relation&quot;:\{.*?\}\})/, '<span class="r">$1</span>')
    .replace(/(&quot;(?:new|key)&quot;:\{.*?\})/, '<span class="p">$1</span>')
    .replace(/^(\[&quot;\w+&quot;)/, '<span class="k">$1</span>');
}

function sampleTransaction(): string {
  const path = `${values.chunks}/${values.sample}.ndjson`;
  if (!existsSync(path)) return '';
  const fd = readFileSync(path, {encoding: 'utf8'}).slice(0, 8192).split('\n');
  const begin = fd.findIndex(l => l.startsWith('["begin"'));
  const commit = fd.findIndex((l, i) => i > begin && l.startsWith('["commit"'));
  const lines = [fd[begin], fd[begin + 1], fd[commit]].filter(Boolean);
  return lines.map(highlight).join('\n\n');
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
