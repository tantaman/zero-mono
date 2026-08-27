import {readFileSync, writeFileSync} from 'node:fs';
/* oxlint-disable no-console */
/**
 * Benchmarks additional codecs against saved chunks and merges the results
 * into an existing results.json, so the main run does not have to be redone.
 *
 *   node src/bench-extra.ts --chunks ./chunks --results results.json
 */
import {parseArgs} from 'node:util';
import {benchmark, EXTRA_CODECS, type CodecResult} from './codecs.ts';

const {values} = parseArgs({
  options: {
    chunks: {type: 'string', default: './chunks'},
    results: {type: 'string', default: 'results.json'},
    reps: {type: 'string', default: '3'},
  },
});

type Result = {dataset: string; workload: string; codecs: CodecResult[]};
const results = JSON.parse(readFileSync(values.results!, 'utf8')) as Result[];

for (const r of results) {
  const raw = readFileSync(
    `${values.chunks}/${r.dataset}--${r.workload}.ndjson`,
  );
  const added: string[] = [];
  for (const codec of EXTRA_CODECS) {
    if (r.codecs.some(c => c.codec === codec.name)) continue;
    const res = benchmark(codec, raw, Number(values.reps));
    r.codecs.push(res);
    added.push(`${codec.name} ${res.ratio.toFixed(1)}x`);
  }
  // Keep the codec list in a stable, readable order.
  r.codecs.sort((a, b) =>
    a.codec.localeCompare(b.codec, 'en', {numeric: true}),
  );
  console.log(
    `${r.dataset}/${r.workload}: ${added.join('  ') || 'up to date'}`,
  );
  writeFileSync(values.results!, JSON.stringify(results, null, 2));
}
console.log(`\nMerged into ${values.results}`);
