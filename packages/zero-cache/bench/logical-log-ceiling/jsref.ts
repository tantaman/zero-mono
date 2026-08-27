// Applies the *same* log the C harness gets, via the best JS applier, to a copy
// of the same base replica, and prints the same checksum.
import {copyFileSync, rmSync, readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {Statement} from '../../../zqlite/src/db.ts';
import {generateLog, replayEntries, SPECS} from '../../src/services/replicator/logical-log-fixture.ts';
import type {Table} from '../../src/services/replicator/logical-log-fixture.ts';

const lc = createSilentLogContext();
const DIR = process.env.CEILING_DIR ?? `${tmpdir()}/zero-logical-log-ceiling`;
const ROWS = Number(process.env.ROWS ?? 2_000_000);
const j = JSON.parse(readFileSync(`${DIR}/base.db.ids.json`, 'utf8'));
const live = new Map<Table, readonly string[]>([['issue', j.issue], ['comment', j.comment]]);
const WORKLOAD = (process.env.WORKLOAD ?? 'mixed') as Parameters<typeof generateLog>[0];
const log = generateLog(WORKLOAD, Number(process.env.LOG_MB ?? 24) * 1048576, 0xc0ffee, 0,
  Math.ceil(ROWS / 20_000) + 16, live);
const entries = replayEntries(log, Number(process.env.COALESCE ?? 256));

type Shape = {cols: string[]; insert: Statement; update: Statement};
function same(a: readonly string[], b: readonly string[]) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}
class Direct {
  #db; #shapes = new Map<string, Shape[]>(); #dels = new Map<string, Statement>();
  #begin; #commit; #setv; #version = '';
  constructor(db: Database) {
    this.#db = db;
    this.#begin = db.prepare('BEGIN IMMEDIATE');
    this.#commit = db.prepare('COMMIT');
    this.#setv = db.prepare(`UPDATE "_zero.replicationState" SET stateVersion = ?`);
  }
  begin(w: string) { this.#version = w; this.#begin.run(); }
  commit(w: string) { this.#setv.run(w); this.#commit.run(); }
  #shape(table: string, cols: readonly string[]): Shape {
    let list = this.#shapes.get(table);
    if (!list) { list = []; this.#shapes.set(table, list); }
    for (let i = 0; i < list.length; i++) {
      if (same(list[i]!.cols, cols)) { if (i) { const s = list[i]!; list.splice(i,1); list.unshift(s); } return list[0]!; }
    }
    const owned = [...cols];
    const names = [...owned, '_0_version'].map(c => `"${c}"`).join(',');
    const slots = new Array(owned.length + 1).fill('?').join(',');
    const sets = [...owned, '_0_version'].map(c => `"${c}"=?`).join(',');
    const shape: Shape = {cols: owned,
      insert: this.#db.prepare(`INSERT OR REPLACE INTO "${table}" (${names}) VALUES (${slots})`),
      update: this.#db.prepare(`UPDATE "${table}" SET ${sets} WHERE "id"=?`)};
    list.unshift(shape); return shape;
  }
  insert(t: string, cols: readonly string[], vals: unknown[]) {
    const s = this.#shape(t, cols); vals.push(this.#version); s.insert.run(vals);
  }
  update(t: string, cols: readonly string[], vals: unknown[], key: unknown) {
    const s = this.#shape(t, cols); vals.push(this.#version, key);
    if (s.update.run(vals).changes === 0) { vals.length -= 2; this.insert(t, cols, vals); }
  }
  delete(t: string, key: unknown) {
    let s = this.#dels.get(t);
    if (!s) { s = this.#db.prepare(`DELETE FROM "${t}" WHERE "id"=?`); this.#dels.set(t, s); }
    s.run(key);
  }
}

for (let rep = 0; rep < 3; rep++) {
  const work = `${DIR}/jswork-${rep}.db`;
  copyFileSync(`${DIR}/base.db`, work);
  const db = new Database(lc, work);
  db.pragma('locking_mode = EXCLUSIVE'); db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF'); db.pragma('busy_timeout = 30000');
  const a = new Direct(db);
  const cols: string[] = [], vals: unknown[] = [];
  let applied = 0;
  const t0 = performance.now();
  for (const e of entries) {
    const m = JSON.parse(e);
    if (m[0] === 'begin') { a.begin(m[2].commitWatermark); continue; }
    if (m[0] === 'commit') { a.commit(m[2].watermark); continue; }
    const c = m[1], table = c.relation.name;
    if (c.tag === 'delete') { a.delete(table, c.key.id); applied++; continue; }
    const row = c.new; cols.length = 0; vals.length = 0;
    for (const k in row) { const v = row[k]; cols.push(k); vals.push(typeof v === 'boolean' ? (v?1:0) : v); }
    if (c.tag === 'insert') a.insert(table, cols, vals); else a.update(table, cols, vals, row.id);
    applied++;
  }
  const ms = performance.now() - t0;
  const parts: string[] = [];
  for (const spec of SPECS) {
    const [r] = db.prepare(`SELECT COUNT(*) n, SUM(LENGTH(COALESCE("id",''))) i, SUM(LENGTH(COALESCE("created",0))) c, COUNT(DISTINCT "_0_version") v FROM "${spec.name}"`).all<any>();
    parts.push(`${spec.name}:${r.n}/${r.i}/${r.c}/${r.v}`);
  }
  const [sv] = db.prepare(`SELECT stateVersion FROM "_zero.replicationState"`).all<any>();
  parts.push(`v:${sv.stateVersion}`);
  console.log(`applied=${applied}  ${ms.toFixed(1)} ms  ${(ms*1000/applied).toFixed(2)} us/change  checksum=${parts.join(' ')}`);
  db.close(); rmSync(work, {force: true});
}
