const D = JSON.parse(document.getElementById('data').textContent);
const R = D.results;
const MiB = 1 << 20,
  GiB = 1 << 30,
  CHUNK = 16 * MiB;
const TTFB = 0.03,
  STREAM = 90e6;
const USD_GB_MO = 0.023,
  USD_1K_PUT = 0.005;

const fx = (v, d = 1) =>
  v.toLocaleString('en-US', {
    minimumFractionDigits: d,
    maximumFractionDigits: d,
  });
const c3 = r => r.codecs.find(c => c.codec === 'zstd-3');
const el = (t, cls, html) => {
  const n = document.createElement(t);
  if (cls) n.className = cls;
  if (html !== undefined && html !== null) n.innerHTML = html;
  return n;
};
const median = xs => xs.toSorted((a, b) => a - b)[Math.floor(xs.length / 2)];
const label = r => `<span class="ds">${r.dataset}/</span>${r.workload}`;

const DATA_TAGS = ['insert', 'update', 'delete', 'truncate', 'backfill'];
const REPLICA_IDENTITY_FULL = /replicaidentity-full/;
/** Row changes in the chunk. A message is not a row: a 1-row transaction
 *  emits three messages (begin, data, commit) for a single change. */
const rowsIn = r => {
  const tags = r.byTag ?? {};
  const total = Object.values(tags).reduce((a, b) => a + b, 0) || 1;
  const data = DATA_TAGS.reduce((a, t) => a + (tags[t] ?? 0), 0);
  return r.messages * (data / total);
};
const rawPerRow = r => r.rawBytes / rowsIn(r);
const storedPerRow = r => c3(r).compressedBytes / rowsIn(r);

// ---- masthead -------------------------------------------------------------
document.getElementById('runmeta').innerHTML = [
  `${R.length} workloads`,
  '3 schemas',
  '16 MiB chunks',
  '13 codec settings',
  'real logical replication',
  'Postgres 16',
]
  .map(t => `<span class="tag">${t}</span>`)
  .join('');

document.getElementById('sample').innerHTML = D.sample;

// ---- tiles ----------------------------------------------------------------
const ratios = R.map(r => c3(r).ratio);
const perChange = R.map(storedPerRow);
const lo = R[ratios.indexOf(Math.min(...ratios))],
  hi = R[ratios.indexOf(Math.max(...ratios))];
const byName = (ds, w) => R.find(r => r.dataset === ds && r.workload === w);
const text = byName('zbugs', 'insert-comment-batch50');

document.getElementById('tiles').innerHTML = [
  [
    'Ratio, zstd-3',
    `${fx(Math.min(...ratios))}–${fx(Math.max(...ratios))}×`,
    `Floor is ${lo.dataset}/${lo.workload}; ceiling is ${hi.dataset}/${hi.workload}.`,
  ],
  [
    'Median ratio',
    `${fx(median(ratios))}×`,
    `Across all ${R.length} chunks. A 16 MiB chunk lands at ${fx(median(R.map(r => c3(r).compressedBytes)) / MiB, 2)} MiB on S3.`,
  ],
  [
    'Text-heavy app',
    text ? `${fx(c3(text).ratio)}×` : '—',
    `zbugs comment inserts with real markdown bodies — the realistic case for a content app.`,
  ],
  [
    'Stored per row change',
    `${fx(Math.min(...perChange), 0)}–${fx(Math.max(...perChange), 0)} B`,
    `The number to multiply by your write rate. Raw is ${fx(Math.min(...R.map(rawPerRow)), 0)}–${fx(Math.max(...R.map(rawPerRow)), 0)} B per change.`,
  ],
]
  .map(
    ([k, v, n]) =>
      `<div class="tile"><div class="k">${k}</div><div class="v">${v}</div><div class="n">${n}</div></div>`,
  )
  .join('');

// ---- grouped table helper -------------------------------------------------
function grouped(tableId, headers, rowFn) {
  const t = document.getElementById(tableId);
  t.innerHTML = `<thead><tr>${headers.map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
  const tb = el('tbody');
  for (const ds of ['chinook', 'zbugs', 'pagila']) {
    const rows = R.filter(r => r.dataset === ds);
    if (!rows.length) continue;
    tb.appendChild(
      el('tr', 'grp', `<td colspan="${headers.length}">${ds}</td>`),
    );
    for (const r of rows) tb.appendChild(el('tr', null, rowFn(r)));
  }
  t.appendChild(tb);
}

// ---- ratios ---------------------------------------------------------------
const maxLog = Math.log(Math.max(...ratios));
grouped(
  'ratios',
  [
    'Workload',
    'Rows<br>/ txn',
    'Raw B<br>/ row change',
    'Ratio (zstd-3)',
    'Stored B<br>/ row change',
    'Chunk<br>on S3',
  ],
  r => {
    const c = c3(r),
      w = (Math.log(c.ratio) / maxLog) * 100;
    return `<td class="name">${label(r)}</td>
      <td>${r.txnSize ?? '—'}</td>
      <td>${fx(rawPerRow(r), 0)}</td>
      <td><span class="bar"><span class="track"><span class="fill" style="width:${w.toFixed(1)}%"></span></span><span class="lab">${fx(c.ratio)}×</span></span></td>
      <td>${fx(storedPerRow(r), 1)}</td>
      <td>${fx(c.compressedBytes / MiB, 2)} MiB</td>`;
  },
);

// ---- anatomy --------------------------------------------------------------
document.getElementById('legend').innerHTML = [
  ['var(--s1)', 'relation block (repeated on every message)'],
  ['var(--s2)', 'begin/commit framing (LSN, xid, commit time)'],
  ['var(--s3)', 'row data + message envelope'],
]
  .map(([c, t]) => `<span><b style="background:${c}"></b>${t}</span>`)
  .join('');

grouped(
  'anatomy',
  [
    'Workload',
    'Composition of the raw chunk',
    'Relation',
    'Framing',
    'Row data',
    'Interned<br>raw size',
    'Interned<br>+ zstd-3',
  ],
  r => {
    const rel = (100 * r.relationBytes) / r.rawBytes;
    const fr = (100 * r.framingBytes) / r.rawBytes;
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
  },
);

// ---- transaction-size callout --------------------------------------------
(() => {
  const pairs = [
    ['chinook', 'insert-track-batch100', 'insert-track-single'],
    ['pagila', 'insert-rental-batch100', 'insert-rental-single'],
  ];
  const floors = [];
  const rows = pairs
    .map(([ds, big, one]) => {
      const b = byName(ds, big);
      const s = byName(ds, one);
      if (!b || !s) return '';
      // One row per transaction adds ~1 begin+commit pair per row; the extra
      // stored bytes are what that framing costs after compression.
      floors.push(storedPerRow(s) - storedPerRow(b));
      return `<li><code>${ds}</code>: ${fx(c3(b).ratio)}× at 100 rows per transaction,
      ${fx(c3(s).ratio)}× at one. Per row change that is
      <b>${fx(storedPerRow(s))} stored bytes instead of ${fx(storedPerRow(b))}</b> —
      ${fx(storedPerRow(s) / storedPerRow(b))}× more expensive for exactly the same data.</li>`;
    })
    .join('');
  document.getElementById('txn-callout').innerHTML =
    `<span class="eyebrow">The dominant variable</span>
     <p>Transaction size, not schema, is the single biggest lever. Each transaction's
     <code>begin</code>/<code>commit</code> pair carries a commit LSN, an end LSN, an xid, a microsecond
     commit time and a watermark. Those values are unique to the transaction, so they survive
     compression almost intact &mdash; a measured floor of
     ${fx(Math.min(...floors), 0)}&ndash;${fx(Math.max(...floors), 0)} stored bytes per transaction,
     no matter what the transaction contains.</p>
     <ul>${rows}</ul>`;
  window.__floor = floors;

  const cc = (D.fieldCost ?? []).find(
    c => c.chunk === 'zbugs--insert-comment-batch50',
  );
  const trig = cc?.relations?.find(r => r.relation === 'issue');
  const host = document.getElementById('trigger-callout');
  if (!trig || !host) {
    host?.remove();
    return;
  }
  host.innerHTML = `<span class="eyebrow">Triggers amplify the log</span>
     <p>The <code>insert-comment</code> workload writes to one table. The chunk contains two.
     zbugs has a real trigger, <code>update_issue_modified_time_on_comment</code>, that bumps
     <code>issue.modified</code> whenever a comment is inserted &mdash; and Postgres logical
     replication ships the <em>whole</em> issue row for that one-column update, title and
     description included.</p>
     <p>The result: ${fx((100 * trig.rawBytes) / cc.rawBytes, 0)}% of the raw chunk and
     ${fx((100 * trig.compressedBytes) / cc.compressedBytes, 0)}% of the stored chunk is the
     trigger's side effect, not the comment. Posting a comment costs
     ${fx(cc.compressedBytes / (cc.relations.find(r => r.relation === 'comment')?.messages ?? 1))}
     stored bytes rather than the ${fx((cc.compressedBytes - trig.compressedBytes) / (cc.relations.find(r => r.relation === 'comment')?.messages ?? 1))}
     the comment itself accounts for. Audit triggers before sizing anything.</p>`;
})();

// ---- per-column cost ------------------------------------------------------
(() => {
  const host = document.getElementById('fieldcost');
  if (!D.fieldCost || !D.fieldCost.length) {
    host.remove();
    return;
  }
  for (const ch of D.fieldCost) {
    const cols = ch.columns.toSorted(
      (a, b) => b.compressedBytesPerRow - a.compressedBytesPerRow,
    );
    const max = Math.max(...cols.map(c => c.compressedBytesPerRow));
    const wrap = el('div');
    wrap.style.marginBottom = '2rem';

    const rels = ch.relations ?? [];
    const relTable = rels.length
      ? `<div class="scroll" style="margin-bottom:.6rem"><table>
           <thead><tr><th>Table in this chunk</th><th>Messages</th>
             <th>Share of raw</th><th>Share of stored</th></tr></thead>
           <tbody>${rels
             .map(
               r => `<tr>
             <td class="name">${r.relation}</td>
             <td>${r.messages.toLocaleString('en-US')}</td>
             <td>${fx((100 * r.rawBytes) / ch.rawBytes, 0)}%</td>
             <td>${fx((100 * r.compressedBytes) / ch.compressedBytes, 0)}%</td>
           </tr>`,
             )
             .join('')}</tbody></table></div>`
      : '';

    wrap.innerHTML =
      `<p class="eyebrow" style="margin-bottom:.5rem">${ch.chunk.replace('--', ' / ')}
        &nbsp;·&nbsp; ${fx(ch.ratio)}× &nbsp;·&nbsp; ${fx(ch.compressedBytes / MiB, 2)} MiB stored</p>` +
      relTable +
      `<div class="scroll"><table>
         <thead><tr><th>Column</th><th>Raw B<br>per row</th><th>Stored B<br>per row</th>
           <th>Share of<br>chunk</th><th>Compression on<br>this column</th></tr></thead>
         <tbody>${cols
           .map(
             c => `<tr>
           <td class="name">${c.column}</td>
           <td>${fx(c.rawBytesPerRow)}</td>
           <td><span class="bar"><span class="track"><span class="fill" style="width:${((100 * Math.max(0, c.compressedBytesPerRow)) / max).toFixed(1)}%"></span></span><span class="lab">${fx(c.compressedBytesPerRow)}</span></span></td>
           <td>${fx(100 * c.shareOfCompressedChunk, 0)}%</td>
           <td>${c.compressedBytesPerRow > 0.05 ? fx(c.rawBytesPerRow / c.compressedBytesPerRow) + '×' : '&gt; 100×'}</td>
         </tr>`,
           )
           .join('')}</tbody></table></div>`;
    host.appendChild(wrap);
  }
})();

// ---- codecs ---------------------------------------------------------------
(() => {
  const names = R[0].codecs.map(c => c.codec);
  const ratioOf = (r, n) => (r.codecs.find(c => c.codec === n) ?? {}).ratio;
  const medOf = (n, key) =>
    median(
      R.map(r => (r.codecs.find(c => c.codec === n) ?? {})[key]).filter(
        v => v !== undefined,
      ),
    );

  // Compared per workload, then summarised. The ratio of two medians is not
  // the median of the ratios: these chunks are bimodal, so the two disagree.
  const rows = names.map(name => {
    const gains = R.map(r => ratioOf(r, name) / ratioOf(r, 'zstd-3'));
    return {
      name,
      ratio: medOf(name, 'ratio'),
      gain: median(gains) - 1,
      lo: Math.min(...gains) - 1,
      hi: Math.max(...gains) - 1,
      comp: medOf(name, 'compressMiBs'),
      dec: medOf(name, 'decompressMiBs'),
    };
  });

  // Best median gain among settings that still compress an order of magnitude
  // faster than any realistic change stream AND are never materially worse
  // than zstd-3 on any single workload. brotli-4 has a better median than
  // several zstd settings but is up to 24% worse on some chunks, which is not
  // a trade worth making for a backup format.
  const FLOOR = 100,
    DOWNSIDE = -0.05;
  const pick = rows
    .filter(r => r.comp >= FLOOR && r.lo >= DOWNSIDE)
    .reduce((a, b) => (b.gain > a.gain ? b : a));
  const pct = v => `${v >= 0 ? '+' : '−'}${fx(Math.abs(100 * v), 0)}%`;

  const t = document.getElementById('codecs');
  t.innerHTML = `<thead><tr>${[
    'Codec',
    'Median<br>ratio',
    'Median vs<br>zstd-3',
    'Range vs zstd-3',
    'Compress<br>MiB/s',
    'Decompress<br>MiB/s',
  ]
    .map(h => `<th>${h}</th>`)
    .join('')}</tr></thead>`;
  const tb = el('tbody');
  for (const r of rows) {
    const on = r.name === pick.name;
    const nm = on ? `<b>${r.name}</b>` : r.name;
    tb.appendChild(
      el(
        'tr',
        null,
        `<td class="name">${nm}</td>
       <td>${fx(r.ratio)}×</td>
       <td>${on ? `<b>${pct(r.gain)}</b>` : pct(r.gain)}</td>
       <td>${pct(r.lo)} … ${pct(r.hi)}</td>
       <td>${fx(r.comp, 0)}</td>
       <td>${fx(r.dec, 0)}</td>`,
      ),
    );
  }
  t.appendChild(tb);

  const get = n => rows.find(r => r.name === n);
  const g6 = get('gzip-6'),
    z1 = get('zstd-1'),
    w24 = get('zstd-3-win24');
  const b9 = get('brotli-9'),
    b11 = get('brotli-11'),
    z19 = get('zstd-19');
  const rateMiBs = (100000 * median(R.map(rawPerRow))) / MiB;

  document.getElementById('codec-note').innerHTML =
    `<b>gzip is out.</b> Its 32&nbsp;KB window cannot reach back far enough to fold the repeated
     relation blocks together: ${pct(g6.gain)} against zstd-3, and as much as ${pct(g6.lo)} on the
     structure-heavy chunks. zstd-1 looks like a free speedup — its median gain is ${pct(z1.gain)} — but it gives up as much as
     ${pct(z1.lo)} on exactly the chunks where compression matters most.
     <br><br>
     <b>Raise the zstd window whatever level you pick.</b> zstd defaults to a 2&nbsp;MiB window at
     level 3 and 4&nbsp;MiB at level 9, so most of a 16&nbsp;MiB chunk is out of reach.
     <code>ZSTD_c_windowLog&nbsp;=&nbsp;24</code> costs nothing measurable
     (${fx(w24.comp, 0)} vs ${fx(get('zstd-3').comp, 0)}&nbsp;MiB/s at level 3) and never hurts.
     Its median gain is only ${pct(w24.gain)}, but on the chunks where long-range matching matters it
     reaches ${pct(w24.hi)} — free insurance rather than a headline win.
     <br><br>
     <b>Throughput is not the binding constraint, so buy ratio with CPU.</b> A shard writing 100k row
     changes a second produces roughly ${fx(rateMiBs, 0)}&nbsp;MiB/s of logical log.
     <code>${pick.name}</code> compresses ${fx(pick.comp / rateMiBs, 0)}× faster than that, is
     ${pct(pick.gain)} on storage against zstd-3, and is never worse than ${pct(pick.lo)} on any
     single chunk — which is why it beats <code>brotli-4</code>, whose better median
     (${pct(get('brotli-4').gain)}) hides a ${pct(get('brotli-4').lo)} worst case. If storage matters
     more than CPU, <code>brotli-9</code> is ${pct(b9.gain)} at ${fx(b9.comp, 0)}&nbsp;MiB/s — still
     ahead of the stream, but with much less headroom. Past that the curve goes vertical:
     zstd-19 is ${pct(z19.gain)} at ${fx(z19.comp)}&nbsp;MiB/s and brotli-11 ${pct(b11.gain)} at
     ${fx(b11.comp)}&nbsp;MiB/s — neither can keep up with a live change stream.
     <br><br>
     <b>Decompression is flat.</b> Every setting in the table lands between
     ${fx(Math.min(...rows.map(r => r.dec)), 0)} and ${fx(Math.max(...rows.map(r => r.dec)), 0)}&nbsp;MiB/s,
     so a higher compression level costs nothing on restore.`;

  window.__pick = pick.name;
})();

// ---- chunk size -----------------------------------------------------------
(() => {
  const sizes = R[0].sizeSweep.map(p => p.chunkMiB);
  const at1 = r => r.sizeSweep.find(p => p.chunkMiB === 1);
  const at16 = r => r.sizeSweep.find(p => p.chunkMiB === 16);
  grouped(
    'sizes',
    ['Workload', ...sizes.map(s => `${s} MiB`), '16 vs 1 MiB'],
    r => {
      const gain = at1(r) && at16(r) ? at16(r).ratio / at1(r).ratio - 1 : null;
      return (
        `<td class="name">${label(r)}</td>` +
        sizes
          .map(s => {
            const p = r.sizeSweep.find(p => p.chunkMiB === s);
            return `<td>${p ? fx(p.ratio) + '×' : '—'}</td>`;
          })
          .join('') +
        `<td>${gain === null ? '—' : `${gain >= 0 ? '+' : '−'}${fx(Math.abs(100 * gain), 0)}%`}</td>`
      );
    },
  );

  const ref = byName('zbugs', 'mixed-oltp-small-txn') ?? R[0];
  const at = bytes => {
    const pts = ref.sizeSweep.toSorted((a, b) => a.rawBytes - b.rawBytes);
    if (bytes <= pts[0].rawBytes) return pts[0].ratio;
    for (let i = 1; i < pts.length; i++) {
      if (bytes <= pts[i].rawBytes) {
        const t =
          (Math.log(bytes) - Math.log(pts[i - 1].rawBytes)) /
          (Math.log(pts[i].rawBytes) - Math.log(pts[i - 1].rawBytes));
        return pts[i - 1].ratio + t * (pts[i].ratio - pts[i - 1].ratio);
      }
    }
    return pts.at(-1).ratio;
  };
  const rows = [
    [1000, '1k'],
    [10000, '10k'],
    [100000, '100k'],
  ]
    .map(([rate, lab]) => {
      const cells = [1, 5, 30].map(sec => {
        const b = Math.min(CHUNK, rate * rawPerRow(ref) * sec);
        return `${fx(b / MiB, 2)} MiB at ${fx(at(b))}×`;
      });
      return `<tr><td class="name">${lab} changes/s</td>${cells.map(c => `<td>${c}</td>`).join('')}</tr>`;
    })
    .join('');
  const gains = R.map(r => (at16(r)?.ratio ?? 1) / (at1(r)?.ratio ?? 1) - 1);
  const best = R[gains.indexOf(Math.max(...gains))];
  document.getElementById('sizes-note').innerHTML =
    `The spread is the whole point: <code>${best.dataset}/${best.workload}</code> gains
     ${fx(100 * Math.max(...gains), 0)}% going from 1&nbsp;MiB to 16&nbsp;MiB, while the
     entropy-dominated chunks move by a few percent in either direction. The workloads that lose a
     little at 16&nbsp;MiB are the ones already compressing past 90×, where it costs nothing.`;

  document.getElementById('rpo-callout').innerHTML =
    `<span class="eyebrow">A chunk only fills as fast as you write</span>
     <p>If you also flush on a time bound to cap the RPO, the effective object size is
     <code>min(16 MiB, rate × bytes-per-change × interval)</code>. Using
     <code>zbugs/mixed-oltp-small-txn</code> as the reference (${fx(rawPerRow(ref), 0)}&nbsp;B of raw log per row change):</p>
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
  t.innerHTML =
    `<thead><tr><th rowspan="2">Workload</th>` +
    RATES.map(r => `<th colspan="2">${r / 1000}k changes/s</th>`).join('') +
    `</tr><tr>` +
    RATES.map(() => `<th>7d stored</th><th>$ / month</th>`).join('') +
    `</tr></thead>`;
  const tb = el('tbody');
  for (const ds of ['chinook', 'zbugs', 'pagila']) {
    const rows = R.filter(r => r.dataset === ds);
    tb.appendChild(
      el('tr', 'grp', `<td colspan="${1 + RATES.length * 2}">${ds}</td>`),
    );
    for (const r of rows) {
      const c = c3(r);
      const cells = RATES.map(rate => {
        const rawBps = rate * rawPerRow(r);
        const storedGB = ((rawBps / c.ratio) * 86400 * 7) / 1e9;
        const puts = ((rawBps / CHUNK) * 86400 * 30) / 1000;
        const usd = storedGB * USD_GB_MO + puts * USD_1K_PUT;
        return `<td>${fx(storedGB, 1)} GB</td><td>$${fx(usd, 2)}</td>`;
      }).join('');
      tb.appendChild(
        el('tr', null, `<td class="name">${label(r)}</td>${cells}`),
      );
    }
  }
  t.appendChild(tb);

  const ref = byName('zbugs', 'mixed-oltp-small-txn') ?? R[0];
  const c = c3(ref);
  const rawBps = 10000 * rawPerRow(ref);
  const storedGB = ((rawBps / c.ratio) * 86400 * 7) / 1e9;
  const puts = ((rawBps / CHUNK) * 86400 * 30) / 1000;
  document.getElementById('cost-note').innerHTML =
    `Storage dominates; PUTs are noise at 16&nbsp;MiB chunks. At 10k changes/s on
     <code>zbugs/mixed-oltp-small-txn</code> you retain ${fx(storedGB, 1)}&nbsp;GB for
     $${fx(storedGB * USD_GB_MO, 2)}/month and issue ${fx((puts * 1000) / 30 / 86400, 2)} PUTs/s
     &mdash; $${fx(puts * USD_1K_PUT, 2)}/month. The whole line item is
     $${fx(storedGB * USD_GB_MO + puts * USD_1K_PUT, 2)}/month per shard.`;
})();

// ---- restore --------------------------------------------------------------
(() => {
  const RESTORE = 100 * GiB,
    CONC = [8, 32, 128, 512];
  grouped(
    'restore',
    [
      'Workload',
      'Objects',
      'Object<br>size',
      'Download',
      ...CONC.map(k => `c=${k}`),
      'Decompress<br>1 core',
    ],
    r => {
      const c = c3(r);
      const objects = Math.ceil(RESTORE / CHUNK);
      const total = objects * c.compressedBytes;
      const per = c.compressedBytes / (TTFB + c.compressedBytes / STREAM);
      return (
        `<td class="name">${label(r)}</td>
        <td>${objects.toLocaleString('en-US')}</td>
        <td>${fx(c.compressedBytes / MiB, 2)} MiB</td>
        <td>${fx(total / 1e9, 1)} GB</td>` +
        CONC.map(k => `<td>${fx(total / (per * k))} s</td>`).join('') +
        `<td>${fx(RESTORE / MiB / c.decompressMiBs)} s</td>`
      );
    },
  );

  // The uncapped table above ignores the NIC; say what it would actually take.
  const ref = byName('zbugs', 'mixed-oltp-small-txn') ?? R[0];
  const rc = c3(ref);
  const objects = Math.ceil(RESTORE / CHUNK);
  const total = objects * rc.compressedBytes;
  const per = rc.compressedBytes / (TTFB + rc.compressedBytes / STREAM);
  const gbps = k => (per * k * 8) / 1e9;
  const capped = (k, nic) => total / Math.min(per * k, (nic * 1e9) / 8);
  document.getElementById('restore-note').innerHTML =
    `Those columns are uncapped. On <code>${ref.dataset}/${ref.workload}</code>, c=128 implies
     ${fx(gbps(128))}&nbsp;Gbps of aggregate ingress and c=512 implies ${fx(gbps(512))}&nbsp;Gbps —
     past what most instance types provide. Against a 25&nbsp;Gbps NIC the download floors at
     ${fx(capped(512, 25))}&nbsp;s; against 10&nbsp;Gbps, ${fx(capped(512, 10))}&nbsp;s. A single core
     still spends ${fx(RESTORE / MiB / rc.decompressMiBs)}&nbsp;s decompressing the same log, and
     applying the changes to the replica is slower again. Concurrency past roughly c=32 buys nothing
     the rest of the pipeline can use.`;

  const t = document.getElementById('getsize');
  t.innerHTML = `<thead><tr>${['Object size', 'Per-stream MB/s', 'Streams for 10 Gbps', 'for 25 Gbps', 'for 100 Gbps'].map(h => `<th>${h}</th>`).join('')}</tr></thead>`;
  const tb = el('tbody');
  for (const mb of [0.25, 0.5, 1, 2, 4, 8, 16, 32]) {
    const b = mb * 1e6,
      per = b / (TTFB + b / STREAM);
    tb.appendChild(
      el(
        'tr',
        null,
        `<td class="name">${mb} MB</td><td>${fx(per / 1e6)}</td>` +
          [10, 25, 100]
            .map(g => `<td>${Math.ceil((g * 1e9) / 8 / per)}</td>`)
            .join(''),
      ),
    );
  }
  t.appendChild(tb);
})();

// ---- implications ---------------------------------------------------------
(() => {
  const ref = byName('zbugs', 'mixed-oltp-small-txn') ?? R[0];
  const rif = R.find(r => REPLICA_IDENTITY_FULL.test(r.workload));
  const base = byName('chinook', 'update-track-1col-batch100');
  const single = byName('chinook', 'insert-track-single');
  const batch = byName('chinook', 'insert-track-batch100');
  const hi = byName('zbugs', 'insert-comment-highentropy');
  const relMed = median(R.map(r => (100 * r.relationBytes) / r.rawBytes));
  document.getElementById('implications').innerHTML = `
  <ul>
    <li><b>Budget by stored bytes per row change, not by ratio.</b> The ratio swings
      ${fx(Math.min(...R.map(r => c3(r).ratio)))}–${fx(Math.max(...R.map(r => c3(r).ratio)))}×
      across these workloads, but stored bytes per change stays inside a much tighter band. For a
      zbugs-shaped app, ${fx(storedPerRow(ref), 0)}&nbsp;B per row change is the planning number.</li>
    <li><b>Ship <code>${window.__pick ?? 'zstd-9'}</code>.</b> zstd's default window is smaller than
      the chunk, so set <code>ZSTD_c_windowLog&nbsp;=&nbsp;24</code> at whatever level you choose &mdash;
      it costs nothing and occasionally wins a lot. Compression throughput has an order of magnitude of
      headroom over any realistic change rate, so the level is a CPU-budget decision, not a
      keep-up one.</li>
    <li><b>Batch upstream transactions where you can.</b> The <code>begin</code>/<code>commit</code>
      pair costs a measured ${
        window.__floor
          ? `${fx(Math.min(...window.__floor), 0)}–${fx(Math.max(...window.__floor), 0)}`
          : '~65'
      } stored bytes per transaction that no compressor can remove. Committing one row at a
      time makes an insert ${
        single && batch
          ? fx(storedPerRow(single) / storedPerRow(batch)) + '×'
          : 'several times'
      } more expensive on S3 than committing a hundred.</li>
    <li><b>Audit triggers before you size anything.</b> A trigger that touches another table turns
      one logical write into two, and Postgres ships the whole triggered row, not the column that
      changed. zbugs' <code>update_issue_modified_time_on_comment</code> makes
      ${(() => {
        const cc = (D.fieldCost ?? []).find(
          c => c.chunk === 'zbugs--insert-comment-batch50',
        );
        const t = cc?.relations?.find(r => r.relation === 'issue');
        return t
          ? `${fx((100 * t.compressedBytes) / cc.compressedBytes, 0)}% of the stored bytes`
          : 'a large share of the log';
      })()} in the comment-insert chunk a side effect rather than the comment.</li>
    <li><b>Watch <code>REPLICA IDENTITY FULL</code>.</b> It ships the old row alongside the new one:
      ${
        base && rif
          ? `${fx(rawPerRow(rif) / rawPerRow(base))}× the raw bytes and
      ${fx(storedPerRow(rif) / storedPerRow(base))}× the stored bytes per row change`
          : 'materially more per change'
      } on an otherwise identical update.</li>
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
