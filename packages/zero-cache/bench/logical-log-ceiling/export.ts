// Builds the base replica and writes a zero-parse binary log for the C harness.
//
// Layout:
//   [0]      Header      magic, version, stride, nRecords, arenaOff
//   [64]     Record[]    fixed-stride, table-specific payload, strings as
//                        {u32 off, u32 len} into the arena
//   [arena]  raw string bytes
//
// Fixed stride means the reader advances by pointer arithmetic and casts; no
// tokenizing, no length prefixes to follow, no allocation. Strings are bound
// with SQLITE_STATIC straight out of the mapping.
import {mkdirSync, writeFileSync, existsSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {createSilentLogContext} from '/home/user/zero-mono/packages/shared/src/logging-test-utils.ts';
import {Database} from '/home/user/zero-mono/packages/zqlite/src/db.ts';
import {StatementRunner} from '/home/user/zero-mono/packages/zero-cache/src/db/statements.ts';
import {ChangeProcessor} from '/home/user/zero-mono/packages/zero-cache/src/services/replicator/change-processor.ts';
import {initReplicationState} from '/home/user/zero-mono/packages/zero-cache/src/services/replicator/schema/replication-state.ts';
import {versionToLexi} from '/home/user/zero-mono/packages/zero-cache/src/types/lexi-version.ts';
import {
  createTableMessage, generateLog, INDEX_SPECS, issueRow, commentRow,
  mulberry32, RELATIONS, replayEntries, SPECS, type Table,
} from '/home/user/zero-mono/packages/zero-cache/src/services/replicator/logical-log-fixture.ts';

const lc = createSilentLogContext();
const DIR = process.env.CEILING_DIR ?? `${tmpdir()}/zero-logical-log-ceiling`;
mkdirSync(DIR, {recursive: true});
const BASE = `${DIR}/base.db`;
const ROWS = Number(process.env.ROWS ?? 2_000_000);
const LOG_MB = Number(process.env.LOG_MB ?? 24);
const COALESCE = Number(process.env.COALESCE ?? 256);

function pragmas(db: Database) {
  db.pragma('locking_mode = EXCLUSIVE');
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.pragma('busy_timeout = 30000');
}
const mkProc = (db: Database) =>
  new ChangeProcessor(new StatementRunner(db), 'backup' as never, (_, e) => { throw e; });

const live = new Map<Table, string[]>([['issue', []], ['comment', []]]);
if (!existsSync(BASE)) {
  const db = new Database(lc, BASE);
  pragmas(db);
  initReplicationState(db, ['ceiling'], versionToLexi(0));
  const proc = mkProc(db);
  proc.processMessage(lc, ['begin', {tag: 'begin'}, {commitWatermark: versionToLexi(1)}]);
  for (const s of SPECS) proc.processMessage(lc, ['data', createTableMessage(s)]);
  for (const spec of INDEX_SPECS) proc.processMessage(lc, ['data', {tag: 'create-index', spec}]);
  proc.processMessage(lc, ['commit', {tag: 'commit'}, {watermark: versionToLexi(1)}]);
  const rand = mulberry32(0x5eed);
  let seq = 0, wm = 2;
  while (seq < ROWS) {
    const stop = Math.min(ROWS, seq + 20_000);
    const w = versionToLexi(wm++);
    proc.processMessage(lc, ['begin', {tag: 'begin'}, {commitWatermark: w}]);
    for (; seq < stop; seq++) {
      const t: Table = rand() < 0.45 ? 'issue' : 'comment';
      const id = `${t}-b${seq}`;
      live.get(t)!.push(id);
      proc.processMessage(lc, ['data', {tag: 'insert', relation: RELATIONS[t],
        new: t === 'issue' ? issueRow(rand, id, seq) : commentRow(rand, id, seq)}]);
    }
    proc.processMessage(lc, ['commit', {tag: 'commit'}, {watermark: w}]);
  }
  db.close();
  writeFileSync(`${BASE}.ids.json`, JSON.stringify({issue: live.get('issue'), comment: live.get('comment')}));
  console.error(`base built: ${ROWS} rows`);
} else {
  const j = JSON.parse(readFileSync(`${BASE}.ids.json`, 'utf8'));
  live.set('issue', j.issue); live.set('comment', j.comment);
}

const WORKLOAD = (process.env.WORKLOAD ?? 'mixed') as Parameters<typeof generateLog>[0];
const log = generateLog(WORKLOAD, LOG_MB * 1048576, 0xc0ffee, 0,
  Math.ceil(ROWS / 20_000) + 16, live);
const entries = replayEntries(log, COALESCE);

// ---- encode ----
const STRIDE = 96;
const OP = {INSERT: 1, UPDATE: 2, DELETE: 3, BEGIN: 4, COMMIT: 5} as const;
const ISSUE_STR = ['id', 'title', 'description', 'creatorID', 'assigneeID', 'visibility'];
const ISSUE_INT = ['shortID', 'open', 'created', 'modified'];
const COMMENT_STR = ['id', 'issueID', 'creatorID', 'body'];
const COMMENT_INT = ['created'];

const arena: Buffer[] = [];
let arenaLen = 0;
const interned = new Map<string, number>();
function put(s: string): [number, number] {
  const cached = interned.get(s);
  const b = Buffer.from(s, 'utf8');
  if (cached !== undefined) return [cached, b.length];
  const off = arenaLen;
  arena.push(b); arenaLen += b.length;
  interned.set(s, off);
  return [off, b.length];
}

const recs: Buffer[] = [];
function rec(op: number, table: number): Buffer {
  const b = Buffer.alloc(STRIDE);
  b.writeUInt8(op, 0); b.writeUInt8(table, 1);
  recs.push(b);
  return b;
}
function setStr(b: Buffer, slot: number, v: unknown, nullBit: number) {
  if (v === null || v === undefined) { b.writeUInt16LE(b.readUInt16LE(2) | (1 << nullBit), 2); return; }
  const [off, len] = put(String(v));
  b.writeUInt32LE(off, 8 + slot * 8); b.writeUInt32LE(len, 12 + slot * 8);
}
function setInt(b: Buffer, base: number, slot: number, v: unknown) {
  b.writeBigInt64LE(BigInt(typeof v === 'boolean' ? (v ? 1 : 0) : Math.trunc(Number(v ?? 0))), base + slot * 8);
}

let nData = 0;
for (const json of entries) {
  const msg = JSON.parse(json);
  if (msg[0] === 'begin') { const b = rec(OP.BEGIN, 0); setStr(b, 0, msg[2].commitWatermark, 0); continue; }
  if (msg[0] === 'commit') { const b = rec(OP.COMMIT, 0); setStr(b, 0, msg[2].watermark, 0); continue; }
  const c = msg[1];
  const table = c.relation.name === 'issue' ? 0 : 1;
  if (c.tag === 'delete') { const b = rec(OP.DELETE, table); setStr(b, 0, c.key.id, 0); nData++; continue; }
  const row = c.new;
  const b = rec(c.tag === 'insert' ? OP.INSERT : OP.UPDATE, table);
  const strs = table === 0 ? ISSUE_STR : COMMENT_STR;
  const ints = table === 0 ? ISSUE_INT : COMMENT_INT;
  strs.forEach((col, i) => setStr(b, i, row[col], i));
  // ints start after the string slots: 8 + 6*8 = 56 for issue, 8 + 4*8 = 40 for comment
  const intBase = 8 + strs.length * 8;
  ints.forEach((col, i) => setInt(b, intBase, i, row[col]));
  nData++;
}

const HDR = 64;
const header = Buffer.alloc(HDR);
header.write('ZLOG0001', 0, 'ascii');
header.writeUInt32LE(STRIDE, 8);
header.writeUInt32LE(recs.length, 12);
header.writeUInt32LE(HDR + recs.length * STRIDE, 16);   // arena offset
header.writeUInt32LE(arenaLen, 20);
const out = Buffer.concat([header, ...recs, ...arena]);
writeFileSync(`${DIR}/log.bin`, out);

writeFileSync(`${DIR}/meta.json`, JSON.stringify({
  jsonBytes: log.bytes, changes: log.changes, dataRecords: nData,
  records: recs.length, binBytes: out.length, arenaBytes: arenaLen,
  logMB: log.bytes / 1048576,
}, null, 2));
console.log(`records=${recs.length} data=${nData} json=${(log.bytes/1048576).toFixed(1)}MB bin=${(out.length/1048576).toFixed(1)}MB arena=${(arenaLen/1048576).toFixed(1)}MB`);
