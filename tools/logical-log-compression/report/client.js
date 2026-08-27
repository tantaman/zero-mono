const D = JSON.parse(document.getElementById('data').textContent);
const R = D.results;
const MiB = 1 << 20, GiB = 1 << 30, CHUNK = 16 * MiB;
const TTFB = 0.030, STREAM = 90e6;
const USD_GB_MO = 0.023, USD_1K_PUT = 0.005, USD_1K_GET = 0.0004;

const fx = (v, d = 1) => v.toLocaleString('en-US', {minimumFractionDigits: d, maximumFractionDigits: d});
const c3 = r => r.codecs.find(c => c.codec === 'zstd-3');
const el = (t, cls, html) => { const n = document.createElement(t); if (cls) n.className = cls; if (html != null) n.innerHTML = html; return n; };
const median = xs => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const label = r => `<span class="ds">${r.dataset}/</span>${r.workload}`;

// ---- masthead -------------------------------------------------------------
document.getElementById('runmeta').innerHTML = [
  `${R.length} workloads`, '3 schemas', '16 MiB chunks',
  '13 codec settings', 'real logical replication', 'Postgres 16',
].map(t => `<span class="tag">${t}</span>`).join('');

document.getElementById('sample').innerHTML = D.sample;

// ---- tiles ----------------------------------------------------------------
const ratios = R.map(r => c3(r).ratio);
const perChange = R.map(r => r.bytesPerMessage / c3(r).ratio);
const lo = R[ratios.indexOf(Math.min(...ratios))], hi = R[ratios.indexOf(Math.max(...ratios))];
const text = R.find(r => r.workload === 'insert-comment-batch50');
const oltp = R.filter(r => /mixed-oltp/.test(r.workload));

document.getElementById('tiles').innerHTML = [
  ['Ratio, zstd-3', `${fx(Math.min(...ratios))}–${fx(Math.max(...ratios))}×`,
    `Floor is ${lo.dataset}/${lo.workload}; ceiling is ${hi.dataset}/${hi.workload}.`],
  ['Median ratio', `${fx(median(ratios))}×`,
    `Across all ${R.length} chunks. A 16 MiB chunk lands at ${fx(median(R.map(r => c3(r).compressedBytes)) / MiB, 2)} MiB on S3.`],
  ['Text-heavy app', `${fx(c3(text).ratio)}×`,
    `zbugs comment inserts with real markdown bodies — the realistic worst case for a content app.`],
  ['Stored per row change', `${fx(Math.min(...perChange), 0)}–${fx(Math.max(...perChange), 0)} B`,
    `The number to multiply by your write rate. Raw is ${fx(Math.min(...R.map(r => r.bytesPerMessage)), 0)}–${fx(Math.max(...R.map(r => r.bytesPerMessage)), 0)} B.`],
].map(([k, v, n]) => `<div class="tile"><div class="k">${k}</div><div class="v">${v}</div><div class="n">${n}</div></div>`).join('');

// ---- grouped table helper -------------------------------------------------
function grouped(tableId, headers, rowFn) {
  const t = document.getElementById(tableId);
  t.innerHTML = `<thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
  const tb = el('tbody');
  for (const ds of ['chinook', 'zbugs', 'pagila']) {
    const rows = R.filter(r => r.dataset === ds);
    if (!rows.length) continue;
    tb.appendChild(el('tr', 'grp', `<td colspan="${headers.length}">${ds}</td>`));
    for (const r of rows) tb.appendChild(el('tr', null, rowFn(r)));
  }
  t.appendChild(tb);
}

// ---- ratios ---------------------------------------------------------------
const maxLog = Math.log(Math.max(...ratios));
grouped('ratios',
  ['Workload', 'Rows / txn', 'Raw B / change', 'Ratio (zstd-3)', 'Stored B / change', 'Chunk on S3'],
  r => {
    const c = c3(r), w = (Math.log(c.ratio) / maxLog) * 100;
    return `<td class="name">${label(r)}</td>
      <td>${r.txnSize ?? '—'}</td>
      <td>${fx(r.bytesPerMessage, 0)}</td>
      <td><span class="bar"><span class="track"><span class="fill" style="width:${w.toFixed(1)}%"></span></span><span class="lab">${fx(c.ratio)}×</span></span></td>
      <td>${fx(r.bytesPerMessage / c.ratio, 1)}</td>
      <td>${fx(c.compressedBytes / MiB, 2)} MiB</td>`;
  });

// ---- anatomy --------------------------------------------------------------
document.getElementById('legend').innerHTML = [
  ['var(--s1)', 'relation block (repeated on every message)'],
  ['var(--s2)', 'begin/commit framing (LSN, xid, commit time)'],
  ['var(--s3)', 'row data + message envelope'],
].map(([c, t]) => `<span><b style="background:${c}"></b>${t}</span>`).join('');

grouped('anatomy',
  ['Workload', 'Composition of the raw chunk', 'Relation', 'Framing', 'Row data', 'Interned raw', 'Interned + zstd-3'],
  r => {
    const rel = 100 * r.relationBytes / r.rawBytes;
    const fr = 100 * r.framingBytes / r.rawBytes;
    const pay = Math.max(0, 100 - rel - fr);
    const i = r.interned;
    return `<td class="name">${label(r)}</td>
      <td><span class="stack">
        <i style="width:${rel.toFixed(1)}%;background:var(--s1)"></i>
        <i style="width:${fr.toFixed(1)}%;background:var(--s2)"></i>
        <i style="width:${pay.toFixed(1)}%;background:var(--s3)"></i></span></td>
      <td>${fx(rel, 0)}%</td><td>${fx(fr, 0)}%</td><td>${fx(pay, 0)}%</td>
      <td>${i ? '−' + fx(100 * (1 - i.rawBytes / r.rawBytes), 0) + '%' : '—'}</td>
      <td>${i ? fx(i.effectiveRatio) + '×' : '—'}</td>`;
  });

// ---- transaction-size callout --------------------------------------------
(() => {
  const pairs = [['chinook', 'insert-track-batch100', 'insert-track-single'],
                 ['pagila', 'insert-rental-batch100', 'insert-rental-single']];
  const rows = pairs.map(([ds, big, one]) => {
    const b = R.find(r => r.dataset === ds && r.workload === big);
    const s = R.find(r => r.dataset === ds && r.workload === one);
    if (!b || !s) return '';
    return `<li><code>${ds}</code>: ${fx(c3(b).ratio)}× at 100 rows per transaction,
      ${fx(c3(s).ratio)}× at one — ${fx(c3(b).ratio / c3(s).ratio)}× worse, and
      ${fx((s.bytesPerMessage * 3 / c3(s).ratio))} stored bytes per row instead of
      ${fx(b.bytesPerMessage / c3(b).ratio)}.</li>`;
  }).join('');
  document.getElementById('txn-callout').innerHTML =
    `<span class="eyebrow">The dominant variable</span>
     <p>Transaction size, not schema, is the single biggest lever. Each transaction's
     <code>begin</code>/<code>commit</code> pair carries a commit LSN, an end LSN, an xid, a microsecond
     commit time and a watermark &mdash; roughly 330 bytes, of which ~80 are genuinely high-entropy and
     survive compression almost intact. That is a hard floor per transaction.</p>
     <ul>${rows}</ul>`;
})();

// ---- per-column cost ------------------------------------------------------
(() => {
  const host = document.getElementById('fieldcost');
  if (!D.fieldCost || !D.fieldCost.length) { host.remove(); return; }
  for (const ch of D.fieldCost) {
    const cols = [...ch.columns].sort((a, b) => b.compressedBytesPerRow - a.compressedBytesPerRow);
    const max = Math.max(...cols.map(c => c.compressedBytesPerRow));
    const stored = ch.compressedBytes / cols[0].rows;
    const wrap = el('div');
    wrap.style.marginBottom = '1.75rem';
    wrap.innerHTML =
      `<p class="eyebrow" style="margin-bottom:.5rem">${ch.chunk.replace('--', ' / ')}
        &nbsp;·&nbsp; ${fx(ch.ratio)}× &nbsp;·&nbsp; ${fx(stored)} stored B/row</p>
       <div class="scroll"><table>
         <thead><tr><th>Column</th><th>Raw B/row</th><th>Stored B/row</th>
           <th>Share of chunk</th><th>Compression on this column</th></tr></thead>
         <tbody>${cols.map(c => `<tr>
           <td class="name">${c.column}</td>
           <td>${fx(c.rawBytesPerRow)}</td>
           <td><span class="bar"><span class="track"><span class="fill" style="width:${(100 * Math.max(0, c.compressedBytesPerRow) / max).toFixed(1)}%"></span></span><span class="lab">${fx(c.compressedBytesPerRow)}</span></span></td>
           <td>${fx(100 * c.shareOfCompressedChunk, 0)}%</td>
           <td>${c.compressedBytesPerRow > 0.05 ? fx(c.rawBytesPerRow / c.compressedBytesPerRow) + '×' : '&gt; 100×'}</td>
         </tr>`).join('')}</tbody></table></div>`;
    host.appendChild(wrap);
  }
})();

// ---- codecs ---------------------------------------------------------------
(() => {
  const names = R[0].codecs.map(c => c.codec);
  const t = document.getElementById('codecs');
  t.innerHTML = `<thead><tr>${['Codec', 'Median ratio', 'Compress MiB/s', 'Decompress MiB/s', 'vs zstd-3 ratio'].map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
  const base = median(R.map(r => c3(r).ratio));
  const tb = el('tbody');
  for (const name of names) {
    const cs = R.map(r => r.codecs.find(c => c.codec === name));
    const ratio = median(cs.map(c => c.ratio));
    const isBase = name === 'zstd-3';
    tb.appendChild(el('tr', null,
      `<td class="name">${isBase ? `<b>${name}</b>` : name}</td>
       <td>${fx(ratio)}×</td>
       <td>${fx(median(cs.map(c => c.compressMiBs)), 0)}</td>
       <td>${fx(median(cs.map(c => c.decompressMiBs)), 0)}</td>
       <td>${ratio >= base ? '+' : '−'}${fx(Math.abs(100 * (ratio / base - 1)), 0)}%</td>`));
  }
  t.appendChild(tb);

  const z1 = median(R.map(r => r.codecs.find(c => c.codec === 'zstd-1').ratio));
  const z9 = median(R.map(r => r.codecs.find(c => c.codec === 'zstd-9').ratio));
  const g6 = median(R.map(r => r.codecs.find(c => c.codec === 'gzip-6').ratio));
  const b11 = median(R.map(r => r.codecs.find(c => c.codec === 'brotli-11').ratio));
  const b11s = median(R.map(r => r.codecs.find(c => c.codec === 'brotli-11').compressMiBs));
  document.getElementById('codec-note').innerHTML =
    `gzip is not competitive here — its 32&nbsp;KB window cannot reach back far enough to fold the
     repeated relation blocks together, so it lands at ${fx(g6)}× against zstd-3's ${fx(base)}×.
     zstd-1 is also a trap: ${fx(z1)}× for roughly the same throughput class. Going up to zstd-9 buys
     ${fx(Math.abs(100 * (z9 / base - 1)), 0)}% and costs most of the compression throughput; brotli-11
     reaches ${fx(b11)}× but at ${fx(b11s)}&nbsp;MiB/s it cannot keep up with a live change stream.
     Raising the zstd window to 16&nbsp;MiB (<code>zstd-3-win24</code>) changes nothing measurable, because
     the repeats it needs are only a few hundred bytes apart.`;
})();

// ---- chunk size -----------------------------------------------------------
(() => {
  const sizes = R[0].sizeSweep.map(p => p.chunkMiB);
  grouped('sizes', ['Workload', ...sizes.map(s => `${s} MiB`)], r =>
    `<td class="name">${label(r)}</td>` + sizes.map(s => {
      const p = r.sizeSweep.find(p => p.chunkMiB === s);
      return `<td>${p ? fx(p.ratio) + '×' : '—'}</td>`;
    }).join(''));

  const ref = R.find(r => r.dataset === 'zbugs' && r.workload === 'mixed-oltp-small-txn') ?? R[0];
  const at = bytes => {
    const pts = [...ref.sizeSweep].sort((a, b) => a.rawBytes - b.rawBytes);
    if (bytes <= pts[0].rawBytes) return pts[0].ratio;
    for (let i = 1; i < pts.length; i++) {
      if (bytes <= pts[i].rawBytes) {
        const t = (Math.log(bytes) - Math.log(pts[i - 1].rawBytes)) / (Math.log(pts[i].rawBytes) - Math.log(pts[i - 1].rawBytes));
        return pts[i - 1].ratio + t * (pts[i].ratio - pts[i - 1].ratio);
      }
    }
    return pts[pts.length - 1].ratio;
  };
  const rows = [[1000, '1k'], [10000, '10k'], [100000, '100k']].map(([rate, lab]) => {
    const cells = [1, 5, 30].map(sec => {
      const b = Math.min(CHUNK, rate * ref.bytesPerMessage * sec);
      return `${fx(b / MiB, 2)} MiB at ${fx(at(b))}×`;
    });
    return `<tr><td class="name">${lab} changes/s</td>${cells.map(c => `<td>${c}</td>`).join('')}</tr>`;
  }).join('');
  document.getElementById('rpo-callout').innerHTML =
    `<span class="eyebrow">A chunk only fills as fast as you write</span>
     <p>If you also flush on a time bound to cap the RPO, the effective object size is
     <code>min(16 MiB, rate × bytes-per-change × interval)</code>. Using
     <code>zbugs/mixed-oltp-small-txn</code> (${fx(ref.bytesPerMessage, 0)}&nbsp;B per change) as the
     reference:</p>
     <div class="scroll" style="margin-top:.75rem"><table>
       <thead><tr><th>Write rate</th><th>1s flush</th><th>5s flush</th><th>30s flush</th></tr></thead>
       <tbody>${rows}</tbody></table></div>
     <p style="margin-top:.85rem">At a small app's write rate a 1-second RPO produces sub-megabyte objects,
     which costs both ratio and a great many PUTs. The chunk size is really an RPO decision.</p>`;
})();

// ---- cost -----------------------------------------------------------------
(() => {
  const RATES = [1000, 10000, 100000];
  const t = document.getElementById('cost');
  t.innerHTML = `<thead><tr><th rowspan="2">Workload</th>` +
    RATES.map(r => `<th colspan="2">${r / 1000}k changes/s</th>`).join('') +
    `</tr><tr>` + RATES.map(() => `<th>7d stored</th><th>$ / month</th>`).join('') + `</tr></thead>`;
  const tb = el('tbody');
  for (const ds of ['chinook', 'zbugs', 'pagila']) {
    const rows = R.filter(r => r.dataset === ds);
    tb.appendChild(el('tr', 'grp', `<td colspan="${1 + RATES.length * 2}">${ds}</td>`));
    for (const r of rows) {
      const c = c3(r);
      const cells = RATES.map(rate => {
        const rawBps = rate * r.bytesPerMessage;
        const storedGB = (rawBps / c.ratio) * 86400 * 7 / 1e9;
        const puts = (rawBps / CHUNK) * 86400 * 30 / 1000;
        const usd = storedGB * USD_GB_MO + puts * USD_1K_PUT;
        return `<td>${fx(storedGB, 1)} GB</td><td>$${fx(usd, 2)}</td>`;
      }).join('');
      tb.appendChild(el('tr', null, `<td class="name">${label(r)}</td>${cells}`));
    }
  }
  t.appendChild(tb);

  const ref = R.find(r => r.dataset === 'zbugs' && r.workload === 'mixed-oltp-small-txn') ?? R[0];
  const c = c3(ref);
  const rawBps = 10000 * ref.bytesPerMessage;
  const storedGB = (rawBps / c.ratio) * 86400 * 7 / 1e9;
  const puts = (rawBps / CHUNK) * 86400 * 30 / 1000;
  document.getElementById('cost-note').innerHTML =
    `Storage dominates; PUTs are noise at 16&nbsp;MiB chunks. At 10k changes/s on
     <code>zbugs/mixed-oltp-small-txn</code> you retain ${fx(storedGB, 1)}&nbsp;GB for
     $${fx(storedGB * USD_GB_MO, 2)}/month and issue ${fx(puts * 1000 / 30 / 86400, 2)} PUTs/s
     &mdash; $${fx(puts * USD_1K_PUT, 2)}/month. The whole line item is
     $${fx(storedGB * USD_GB_MO + puts * USD_1K_PUT, 2)}/month per shard.`;
})();

// ---- restore --------------------------------------------------------------
(() => {
  const RESTORE = 100 * GiB, CONC = [8, 32, 128, 512];
  grouped('restore',
    ['Workload', 'Objects', 'Object size', 'Download', ...CONC.map(k => `c=${k}`), 'Decompress, 1 core'],
    r => {
      const c = c3(r);
      const objects = Math.ceil(RESTORE / CHUNK);
      const total = objects * c.compressedBytes;
      const per = c.compressedBytes / (TTFB + c.compressedBytes / STREAM);
      return `<td class="name">${label(r)}</td>
        <td>${objects.toLocaleString('en-US')}</td>
        <td>${fx(c.compressedBytes / MiB, 2)} MiB</td>
        <td>${fx(total / 1e9, 1)} GB</td>` +
        CONC.map(k => `<td>${fx(total / (per * k))} s</td>`).join('') +
        `<td>${fx(RESTORE / MiB / c.decompressMiBs)} s</td>`;
    });

  const t = document.getElementById('getsize');
  t.innerHTML = `<thead><tr>${['Object size', 'Per-stream MB/s', 'Streams for 10 Gbps', 'for 25 Gbps', 'for 100 Gbps'].map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
  const tb = el('tbody');
  for (const mb of [0.25, 0.5, 1, 2, 4, 8, 16, 32]) {
    const b = mb * 1e6, per = b / (TTFB + b / STREAM);
    tb.appendChild(el('tr', null,
      `<td class="name">${mb} MB</td><td>${fx(per / 1e6)}</td>` +
      [10, 25, 100].map(g => `<td>${Math.ceil(g * 1e9 / 8 / per)}</td>`).join('')));
  }
  t.appendChild(tb);
})();

// ---- implications ---------------------------------------------------------
(() => {
  const med = median(R.map(r => c3(r).ratio));
  const ref = R.find(r => r.dataset === 'zbugs' && r.workload === 'mixed-oltp-small-txn') ?? R[0];
  const rif = R.find(r => /replicaidentity-full/.test(r.workload));
  const base = R.find(r => r.workload === 'update-track-1col-batch100');
  const hi = R.find(r => r.workload === 'insert-comment-highentropy');
  const relMed = median(R.map(r => 100 * r.relationBytes / r.rawBytes));
  document.getElementById('implications').innerHTML = `
  <ul>
    <li><b>Budget by stored bytes per row change, not by ratio.</b> The ratio swings
      ${fx(Math.min(...R.map(r => c3(r).ratio)))}–${fx(Math.max(...R.map(r => c3(r).ratio)))}×
      across these workloads, but stored bytes per change stays inside a much tighter band. For a
      zbugs-shaped app, ${fx(ref.bytesPerMessage / c3(ref).ratio, 0)}&nbsp;B per row change is the planning number.</li>
    <li><b>Ship zstd-3.</b> Median ${fx(med)}× at
      ${fx(median(R.map(r => c3(r).compressMiBs)), 0)}&nbsp;MiB/s compress and
      ${fx(median(R.map(r => c3(r).decompressMiBs)), 0)}&nbsp;MiB/s decompress on one core. Nothing else on
      the curve is worth the throughput.</li>
    <li><b>Batch upstream transactions where you can.</b> Per-transaction framing is an incompressible
      floor of roughly 80 bytes. An app that commits one row at a time pays several times more per row
      than one that commits a hundred.</li>
    <li><b>Watch <code>REPLICA IDENTITY FULL</code>.</b> It ships the old row alongside the new one:
      ${base && rif ? `${fx(rif.bytesPerMessage / base.bytesPerMessage)}× the raw bytes and
      ${fx((rif.bytesPerMessage / c3(rif).ratio) / (base.bytesPerMessage / c3(base).ratio))}× the
      stored bytes per change` : 'materially more per change'} on an otherwise identical update.</li>
    <li><b>Restore is not download-bound.</b> Even at c=32 the download finishes long before a single
      core has decompressed the same log, and applying the changes is slower still. Size the restore path
      around decompress-and-apply parallelism, not the NIC.</li>
    <li><b>The format is redundant, but zstd already collects most of that rent.</b> The repeated
      <code>relation</code> block is a median ${fx(relMed, 0)}% of the raw chunk, and interning it
      removes a comparable fraction of the uncompressed stream &mdash; which matters for the in-process
      change log and for Postgres <code>changeLog</code> storage. Post-compression the gain is much
      smaller. Fix the format for the uncompressed paths, not for the S3 bill.</li>
    <li><b>The pessimistic bound is real but narrow.</b> Incompressible payloads
      (<code>${hi ? hi.workload : 'insert-comment-highentropy'}</code>) land at
      ${hi ? fx(c3(hi).ratio) : '—'}×. If a tenant stores encrypted or already-compressed blobs,
      assume roughly no compression for that traffic and size accordingly.</li>
  </ul>`;
  document.getElementById('caveats').innerHTML =
    `Caveats: ratios are per isolated workload, so a real mixed stream will land between these rows rather
     than at any one of them. Cost figures exclude base snapshots, versioning, lifecycle transitions and
     cross-region replication. Restore timings model S3 GET behaviour analytically from time-to-first-byte
     and per-connection throughput; they were not measured against S3.`;
})();
