import {readdirSync, readFileSync, writeFileSync} from 'node:fs';
/* oxlint-disable no-console */
/**
 * Benchmarks codecs against previously captured chunks.
 *
 * Capture and compression are split so that captures can run concurrently
 * (they are Postgres-bound) while compression timings are measured serially
 * on an otherwise idle machine.
 *
 *   node src/bench-chunks.ts --chunks ./chunks --captures ./capture-*.json
 */
import {parseArgs} from 'node:util';
import {benchmark, CODECS} from './codecs.ts';
import {internRelations} from './intern.ts';

const {values} = parseArgs({
  options: {
    chunks: {type: 'string', default: './chunks'},
    captures: {type: 'string', multiple: true, default: []},
    reps: {type: 'string', default: '3'},
    out: {type: 'string', default: 'results.json'},
  },
});

const REPS = Number(values.reps);

type Capture = {dataset: string; workload: string; [k: string]: unknown};

const captures = new Map<string, Capture>();
for (const f of values.captures!) {
  for (const c of JSON.parse(readFileSync(f, 'utf8')) as Capture[]) {
    captures.set(`${c.dataset}--${c.workload}`, c);
  }
}

const results = [];
for (const file of readdirSync(values.chunks!).sort()) {
  if (!file.endsWith('.ndjson')) continue;
  const key = file.replace(/\.ndjson$/, '');
  const raw = readFileSync(`${values.chunks}/${file}`);
  process.stdout.write(`${key} (${(raw.length / (1 << 20)).toFixed(1)}MiB) `);

  const codecs = CODECS.map(c => benchmark(c, raw, REPS));

  // How much of the win is just the repeated `relation` block? Re-encode with
  // relations interned (declared once, then referenced by id) and re-measure.
  const interned = internRelations(raw);
  const internedZstd3 = benchmark(
    CODECS.find(c => c.name === 'zstd-3')!,
    interned,
    1,
  );

  // Ratio vs. chunk size: a smaller chunk gives the compressor less history to
  // match the repeated relation/framing bytes against, and costs more requests.
  const zstd3Codec = CODECS.find(c => c.name === 'zstd-3')!;
  const sizeSweep = [1, 2, 4, 8, 16, 32]
    .filter(mib => mib * (1 << 20) <= raw.length || mib === 16)
    .map(mib => {
      const n = Math.min(mib * (1 << 20), raw.length);
      const end = raw.lastIndexOf(0x0a, n - 1);
      const slice = raw.subarray(0, end + 1);
      const out = zstd3Codec.compress(slice);
      return {
        chunkMiB: mib,
        rawBytes: slice.length,
        compressedBytes: out.length,
        ratio: slice.length / out.length,
      };
    });

  const zstd3 = codecs.find(c => c.codec === 'zstd-3')!;
  console.log(
    `zstd-3 ${zstd3.ratio.toFixed(1)}x | interned raw -${(100 * (1 - interned.length / raw.length)).toFixed(0)}% -> ${(raw.length / internedZstd3.compressedBytes).toFixed(1)}x`,
  );

  results.push({
    ...(captures.get(key) ?? {
      dataset: key.split('--')[0],
      workload: key.split('--')[1],
    }),
    rawBytes: raw.length,
    codecs,
    sizeSweep,
    interned: {
      rawBytes: interned.length,
      zstd3Bytes: internedZstd3.compressedBytes,
      /** Effective ratio vs. the *original* raw bytes. */
      effectiveRatio: raw.length / internedZstd3.compressedBytes,
    },
  });
  writeFileSync(values.out!, JSON.stringify(results, null, 2));
}
console.log(`\nWrote ${results.length} results to ${values.out}`);
