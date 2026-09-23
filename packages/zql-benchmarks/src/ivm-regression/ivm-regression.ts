/**
 * Cross-commit IVM performance harness: hydration and push (query
 * maintenance) for limit, related, and related + exists queries.
 *
 * Unlike the vitest benchmarks this is a plain node script, so that
 * `compare-commits.sh` can copy it into a worktree of any commit and measure
 * that commit's zql / zqlite / zero-cache sources with the same workload.
 *
 *   node --expose-gc ivm-regression.ts <mode> <out.jsonl> [query-filter]
 *
 * Modes:
 *   memory  MemorySource + MemoryStorage + ArrayView (zero-client)
 *   zqlite  TableSource + file-backed DatabaseStorage + ArrayView (server IVM)
 *   driver  zero-cache PipelineDriver: addQuery() for hydration and
 *           advance() over replicated transactions for pushes
 *
 * Each result line is JSON with the median/mean/p10/p90 time in ms and, from
 * a separate untimed pass, the operator storage calls and SQLite statements
 * per operation.
 *
 * Env: IVM_REGRESSION_LABEL, IVM_REGRESSION_MIN_TIME_MS,
 * IVM_REGRESSION_MIN_ITERS, IVM_REGRESSION_MAX_ITERS, IVM_REGRESSION_WARMUP,
 * IVM_REGRESSION_KIND (hydrate|push), IVM_REGRESSION_WORKLOAD (substring),
 * IVM_REGRESSION_HEAP=1 (also report the heap retained by a hydrated query;
 * needs --expose-gc).
 */

import {appendFileSync, mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {listTables} from '../../../zero-cache/src/db/lite-tables.ts';
import {InspectorDelegate} from '../../../zero-cache/src/server/inspector-delegate.ts';
import {populateFromExistingTables} from '../../../zero-cache/src/services/replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../../zero-cache/src/services/replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
} from '../../../zero-cache/src/services/replicator/test-utils.ts';
import {
  PipelineDriver,
  type Timer,
} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../../../zero-cache/src/services/view-syncer/snapshotter.ts';
import {versionToLexi} from '../../../zero-cache/src/types/lexi-version.ts';
import type {RowValue} from '../../../zero-cache/src/types/row-key.ts';
import {upstreamSchema} from '../../../zero-cache/src/types/shards.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {ValueType} from '../../../zero-schema/src/table-schema.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import type {Storage} from '../../../zql/src/ivm/operator.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
  type Source,
  type SourceChange,
} from '../../../zql/src/ivm/source.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../../zql/src/query/query.ts';
import {QueryDelegateImpl as MemoryQueryDelegate} from '../../../zql/src/query/test/query-delegate.ts';
import {
  DatabaseStorage,
  type ClientGroupStorage,
} from '../../../zqlite/src/database-storage.ts';
import {Database, Statement} from '../../../zqlite/src/db.ts';
import {toSQLiteTypeName} from '../../../zqlite/src/table-source.ts';
import {newQueryDelegate} from '../../../zqlite/src/test/source-factory.ts';

type Mode = 'memory' | 'zqlite' | 'driver';

const [, , modeArg = 'memory', OUT = '/dev/stdout', FILTER = ''] = process.argv;
if (modeArg !== 'memory' && modeArg !== 'zqlite' && modeArg !== 'driver') {
  throw new Error(`Unknown mode ${modeArg}`);
}
const MODE: Mode = modeArg;
const env = process.env;
const LABEL = env.IVM_REGRESSION_LABEL ?? 'unknown';
const MIN_TIME_MS = Number(env.IVM_REGRESSION_MIN_TIME_MS ?? 1200);
const MIN_ITERS = Number(env.IVM_REGRESSION_MIN_ITERS ?? 15);
const MAX_ITERS = Number(env.IVM_REGRESSION_MAX_ITERS ?? 3000);
const WARMUP = Number(env.IVM_REGRESSION_WARMUP ?? 5);
const COUNT_ITERS = 3;

const gc: () => void =
  (globalThis as {gc?: (() => void) | undefined}).gc ?? (() => {});
const lc = createSilentLogContext();

// ---------------------------------------------------------------------------
// Schema (zbugs-like)
// ---------------------------------------------------------------------------

const user = table('user')
  .columns({id: string(), name: string()})
  .primaryKey('id');
const project = table('project')
  .columns({id: string(), name: string()})
  .primaryKey('id');
const issue = table('issue')
  .columns({
    id: string(),
    projectID: string(),
    ownerID: string(),
    title: string(),
    open: boolean(),
    modified: number(),
    created: number(),
  })
  .primaryKey('id');
const comment = table('comment')
  .columns({
    id: string(),
    issueID: string(),
    authorID: string(),
    body: string(),
    created: number(),
  })
  .primaryKey('id');
const label = table('label')
  .columns({id: string(), name: string()})
  .primaryKey('id');
const issueLabel = table('issueLabel')
  .columns({issueID: string(), labelID: string()})
  .primaryKey('issueID', 'labelID');

const issueRelationships = relationships(issue, ({one, many}) => ({
  owner: one({sourceField: ['ownerID'], destField: ['id'], destSchema: user}),
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
  comments: many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: comment,
  }),
  labels: many(
    {sourceField: ['id'], destField: ['issueID'], destSchema: issueLabel},
    {sourceField: ['labelID'], destField: ['id'], destSchema: label},
  ),
}));
const commentRelationships = relationships(comment, ({one}) => ({
  author: one({sourceField: ['authorID'], destField: ['id'], destSchema: user}),
}));

const schema = createSchema({
  tables: [user, project, issue, comment, label, issueLabel],
  relationships: [issueRelationships, commentRelationships],
});
const z = createBuilder(schema);

const TABLES = [
  'user',
  'project',
  'issue',
  'comment',
  'label',
  'issueLabel',
] as const;
type TableName = (typeof TABLES)[number];

// ---------------------------------------------------------------------------
// Data: 20k issues, ~100k comments, ~40k issue labels (deterministic)
// ---------------------------------------------------------------------------

function mulberry32(seed: number) {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const pad = (n: number, w = 6) => String(n).padStart(w, '0');

const NUM_USERS = 200;
const NUM_PROJECTS = 5;
const NUM_LABELS = 30;
const NUM_ISSUES = 20_000;

function genData(): Record<TableName, Row[]> {
  const rnd = mulberry32(42);
  const data: Record<TableName, Row[]> = {
    user: [],
    project: [],
    issue: [],
    comment: [],
    label: [],
    issueLabel: [],
  };
  for (let i = 0; i < NUM_USERS; i++) {
    data.user.push({id: 'u' + pad(i, 3), name: 'user-' + i});
  }
  for (let i = 0; i < NUM_PROJECTS; i++) {
    data.project.push({id: 'p' + i, name: 'project-' + i});
  }
  for (let i = 0; i < NUM_LABELS; i++) {
    data.label.push({id: 'l' + pad(i, 2), name: 'label-' + pad(i, 2)});
  }
  // A random permutation for `modified` so that it is not correlated with id.
  const perm = Array.from({length: NUM_ISSUES}, (_, i) => i);
  for (let i = perm.length - 1; i > 0; i--) {
    const j = Math.floor(rnd() * (i + 1));
    [perm[i], perm[j]] = [perm[j], perm[i]];
  }
  let commentID = 0;
  for (let i = 0; i < NUM_ISSUES; i++) {
    const id = 'i' + pad(i);
    data.issue.push({
      id,
      projectID: 'p' + Math.floor(rnd() * NUM_PROJECTS),
      ownerID: 'u' + pad(Math.floor(rnd() * NUM_USERS), 3),
      title: 'issue title ' + i,
      open: rnd() < 0.7,
      modified: 1_000_000 + perm[i] * 10,
      created: 1_000_000 + i,
    });
    const numComments = Math.floor(rnd() * 11);
    for (let c = 0; c < numComments; c++) {
      data.comment.push({
        id: 'c' + pad(commentID++, 7),
        issueID: id,
        authorID: 'u' + pad(Math.floor(rnd() * NUM_USERS), 3),
        body: 'comment body ' + commentID,
        created: Math.floor(rnd() * 1_000_000),
      });
    }
    const numLabels = Math.floor(rnd() * 5);
    const seen = new Set<string>();
    for (let l = 0; l < numLabels; l++) {
      const labelID = 'l' + pad(Math.floor(rnd() * NUM_LABELS), 2);
      if (!seen.has(labelID)) {
        seen.add(labelID);
        data.issueLabel.push({issueID: id, labelID});
      }
    }
  }
  return data;
}

const data = genData();

const issuesByModifiedDesc = data.issue.toSorted(
  (a, b) => (b.modified as number) - (a.modified as number),
);
const openByModifiedDesc = issuesByModifiedDesc.filter(i => i.open);
const labelsOfIssue = new Map<string, Set<string>>();
for (const il of data.issueLabel) {
  const issueID = il.issueID as string;
  let s = labelsOfIssue.get(issueID);
  if (!s) {
    s = new Set();
    labelsOfIssue.set(issueID, s);
  }
  s.add(il.labelID as string);
}
/** In the window of every limit query without an exists. */
const TOP_OPEN = openByModifiedDesc[0];
/** Outside of every window. */
const LOW_ISSUE = must(issuesByModifiedDesc.at(-1));
/** Adding label l00 moves this issue to the top of the exists(label-00) window. */
const TOP_OPEN_WITHOUT_L00 = must(
  openByModifiedDesc.find(i => !labelsOfIssue.get(i.id as string)?.has('l00')),
);
const MAX_MODIFIED = issuesByModifiedDesc[0].modified as number;
const FANOUT_OWNER = 'u001';
const fanoutOwnerIssues = data.issue.filter(
  i => i.ownerID === FANOUT_OWNER,
).length;

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

const QUERIES: Record<string, AnyQuery> = {
  'L1 limit(100)': z.issue.orderBy('modified', 'desc').limit(100),
  'L2 where+limit(100)': z.issue
    .where('open', true)
    .orderBy('modified', 'desc')
    .limit(100),
  'R1 limit(100).related(owner)': z.issue
    .orderBy('modified', 'desc')
    .limit(100)
    .related('owner'),
  'R2 limit(100).related(comments.limit(10))': z.issue
    .orderBy('modified', 'desc')
    .limit(100)
    .related('comments', c => c.orderBy('created', 'desc').limit(10)),
  'R3 zbugs list: owner+labels+comments(10)+author': z.issue
    .where('open', true)
    .orderBy('modified', 'desc')
    .limit(100)
    .related('owner')
    .related('labels')
    .related('comments', c =>
      c.orderBy('created', 'desc').limit(10).related('author'),
    ),
  'R4 no limit: where(project).related(owner) ~4k rows': z.issue
    .where('projectID', 'p0')
    .related('owner'),
  'E1 exists(labels) limit(100) +owner+labels': z.issue
    .whereExists('labels', l => l.where('name', 'label-00'), {flip: false})
    .orderBy('modified', 'desc')
    .limit(100)
    .related('owner')
    .related('labels'),
  'E2 flipped exists(labels) limit(100) +owner+labels': z.issue
    .whereExists('labels', l => l.where('name', 'label-00'), {flip: true})
    .orderBy('modified', 'desc')
    .limit(100)
    .related('owner')
    .related('labels'),
  'E3 exists(comments by user) limit(50) +comments(10)': z.issue
    .whereExists('comments', c => c.where('authorID', 'u000'), {flip: false})
    .orderBy('modified', 'desc')
    .limit(50)
    .related('comments', c => c.orderBy('created', 'desc').limit(10)),
  'E4 or(cmp, exists) limit(100) +owner': z.issue
    .where(({or, cmp, exists}) =>
      or(
        cmp('ownerID', 'u000'),
        exists('labels', l => l.where('name', 'label-00'), {flip: false}),
      ),
    )
    .orderBy('modified', 'desc')
    .limit(100)
    .related('owner'),
  'E5 or(cmp, flipped exists) limit(100) +owner': z.issue
    .where(({or, cmp, exists}) =>
      or(
        cmp('ownerID', 'u000'),
        exists('labels', l => l.where('name', 'label-00'), {flip: true}),
      ),
    )
    .orderBy('modified', 'desc')
    .limit(100)
    .related('owner'),
  'E6 no limit: exists(labels) +owner (permission-style)': z.issue
    .whereExists('labels', l => l.where('name', 'label-00'), {flip: false})
    .related('owner'),
};

/** Keeps a TableSource alive for `t` in the PipelineDriver. */
function anchorQuery(t: TableName): AnyQuery {
  switch (t) {
    case 'user':
      return z.user.where('id', '__none__');
    case 'project':
      return z.project.where('id', '__none__');
    case 'issue':
      return z.issue.where('id', '__none__');
    case 'comment':
      return z.comment.where('id', '__none__');
    case 'label':
      return z.label.where('id', '__none__');
    case 'issueLabel':
      return z.issueLabel.where('issueID', '__none__');
  }
}

// ---------------------------------------------------------------------------
// Workloads: a forward and an inverse transaction, so the data returns to its
// original state after every cycle. Both directions are timed separately.
// ---------------------------------------------------------------------------

type Mutation =
  | {table: TableName; op: 'add'; row: Row}
  | {table: TableName; op: 'remove'; row: Row}
  | {table: TableName; op: 'edit'; row: Row; old: Row};

type Workload = {
  name: string;
  fwd: Mutation[];
  inv: Mutation[];
  /** Only run for queries whose name matches. */
  only?: RegExp | undefined;
};

const EXISTS_QUERY = /^E/;

function edit(t: TableName, old: Row, patch: Row): [Mutation, Mutation] {
  const row = {...old, ...patch};
  return [
    {table: t, op: 'edit', row, old},
    {table: t, op: 'edit', row: old, old: row},
  ];
}

function makeWorkloads(): Workload[] {
  const newIssue: Row = {
    id: 'i-new',
    projectID: 'p0',
    ownerID: 'u000',
    title: 'new issue',
    open: true,
    modified: MAX_MODIFIED + 5,
    created: 9_999_999,
  };
  const newComment = (issueID: string): Row => ({
    id: 'c-new',
    issueID,
    authorID: 'u000',
    body: 'new comment',
    created: 2_000_000,
  });
  const workloads: Workload[] = [
    {
      name: 'issue add/remove at top of window',
      fwd: [{table: 'issue', op: 'add', row: newIssue}],
      inv: [{table: 'issue', op: 'remove', row: newIssue}],
    },
  ];
  {
    const [fwd, inv] = edit('issue', TOP_OPEN, {modified: 1});
    workloads.push({
      name: 'issue edit: top row leaves window / returns',
      fwd: [fwd],
      inv: [inv],
    });
  }
  {
    const [fwd, inv] = edit('issue', TOP_OPEN, {title: 'retitled'});
    workloads.push({
      name: 'issue edit title, in window',
      fwd: [fwd],
      inv: [inv],
    });
  }
  {
    const [fwd, inv] = edit('issue', LOW_ISSUE, {title: 'retitled'});
    workloads.push({
      name: 'issue edit title, outside window',
      fwd: [fwd],
      inv: [inv],
    });
  }
  {
    const c = newComment(TOP_OPEN.id as string);
    workloads.push({
      name: 'comment add/remove, parent in window',
      fwd: [{table: 'comment', op: 'add', row: c}],
      inv: [{table: 'comment', op: 'remove', row: c}],
    });
  }
  {
    const c = newComment(LOW_ISSUE.id as string);
    workloads.push({
      name: 'comment add/remove, parent outside window',
      fwd: [{table: 'comment', op: 'add', row: c}],
      inv: [{table: 'comment', op: 'remove', row: c}],
    });
  }
  {
    const u = must(data.user.find(u => u.id === FANOUT_OWNER));
    const [fwd, inv] = edit('user', u, {name: 'renamed'});
    workloads.push({
      name: `user edit (owns ${fanoutOwnerIssues} issues)`,
      fwd: [fwd],
      inv: [inv],
    });
  }
  {
    const il: Row = {issueID: TOP_OPEN_WITHOUT_L00.id, labelID: 'l00'};
    workloads.push({
      name: 'issueLabel add/remove (issue enters/leaves exists)',
      fwd: [{table: 'issueLabel', op: 'add', row: il}],
      inv: [{table: 'issueLabel', op: 'remove', row: il}],
    });
  }
  {
    // 10 removals in one transaction => 10 refills.
    const top = openByModifiedDesc.slice(0, 10);
    workloads.push({
      name: 'bulk: remove top 10 issues / re-add (one tx)',
      fwd: top.map(row => ({table: 'issue', op: 'remove', row})),
      inv: top.map(row => ({table: 'issue', op: 'add', row})),
    });
  }
  {
    // Every issue with label-00 stops (and then starts again) satisfying the
    // exists: mass removal from the window, then mass refill.
    const l = must(data.label.find(l => l.id === 'l00'));
    const [fwd, inv] = edit('label', l, {name: 'renamed-label'});
    workloads.push({
      name: 'label rename: all exists(label-00) rows leave / return',
      fwd: [fwd],
      inv: [inv],
      only: EXISTS_QUERY,
    });
  }
  return workloads;
}

const WORKLOADS = makeWorkloads();

// ---------------------------------------------------------------------------
// Operation counting, only enabled in separate untimed passes.
// ---------------------------------------------------------------------------

type Counters = {
  stmt: number;
  sget: number;
  sset: number;
  sdel: number;
  sscan: number;
};
const counters: Counters = {stmt: 0, sget: 0, sset: 0, sdel: 0, sscan: 0};
const counterNames = Object.keys(counters) as (keyof Counters)[];
let countStorageOps = false;

function resetCounters() {
  for (const k of counterNames) {
    counters[k] = 0;
  }
}

function maybeCounting(s: Storage): Storage {
  if (!countStorageOps) {
    return s;
  }
  return {
    get: (key, def) => {
      counters.sget++;
      return s.get(key, def);
    },
    set: (key, value) => {
      counters.sset++;
      s.set(key, value);
    },
    del: key => {
      counters.sdel++;
      s.del(key);
    },
    scan: options => {
      counters.sscan++;
      return s.scan(options);
    },
  };
}

type StatementMethod = 'run' | 'get' | 'all' | 'iterate';
const statementMethods: StatementMethod[] = ['run', 'get', 'all', 'iterate'];
const statementProto = Statement.prototype as unknown as Record<
  StatementMethod,
  (this: Statement, ...args: unknown[]) => unknown
>;
// Prototype methods are not enumerable, so copy them one by one.
const originalStatementMethods = Object.fromEntries(
  statementMethods.map(m => [m, statementProto[m]]),
) as typeof statementProto;

function countStatements(enable: boolean) {
  for (const m of statementMethods) {
    const orig = originalStatementMethods[m];
    statementProto[m] = enable
      ? function (this: Statement, ...args: unknown[]) {
          counters.stmt++;
          return orig.apply(this, args);
        }
      : orig;
  }
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

type Pipeline = {rows: number; teardown: () => void};

interface Backend {
  /** Builds and hydrates the query; `rows` counts nested rows too. */
  hydrate(q: AnyQuery): Pipeline;
  /** Writes the transaction to the data store without timing it (driver). */
  prepare(muts: Mutation[]): void;
  /** Applies (pushes) the prepared transaction; this is what is timed. */
  push(muts: Mutation[]): number;
}

function toSourceChange(m: Mutation): SourceChange {
  switch (m.op) {
    case 'add':
      return makeSourceChangeAdd(m.row);
    case 'remove':
      return makeSourceChangeRemove(m.row);
    case 'edit':
      return makeSourceChangeEdit(m.row, m.old);
  }
}

function countRows(v: unknown): number {
  if (Array.isArray(v)) {
    let n = 0;
    for (const x of v) {
      n += countRows(x);
    }
    return n;
  }
  if (v && typeof v === 'object') {
    let n = 1;
    for (const x of Object.values(v)) {
      if (x && typeof x === 'object') {
        n += countRows(x);
      }
    }
    return n;
  }
  return 0;
}

function materialize(delegate: QueryDelegate, q: AnyQuery, after: () => void) {
  const view = delegate.materialize(q);
  return {
    rows: countRows(view.data),
    teardown: () => {
      view.destroy();
      after();
    },
  };
}

function makeMemoryBackend(): Backend {
  const sources: Record<string, Source> = {};
  for (const t of TABLES) {
    const s = schema.tables[t];
    const source = new MemorySource(s.name, s.columns, s.primaryKey);
    for (const row of data[t]) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[t] = source;
  }
  const delegate = new MemoryQueryDelegate({sources});
  const createStorage = delegate.createStorage.bind(delegate);
  delegate.createStorage = () => maybeCounting(createStorage());
  return {
    hydrate: q => materialize(delegate, q, () => {}),
    prepare: () => {},
    push(muts) {
      for (const m of muts) {
        consume(sources[m.table].push(toSourceChange(m)));
      }
      return 0;
    },
  };
}

function replicaTypeName(type: ValueType): string {
  // The replica uses upstream (PG) type names, which zero-cache maps back to
  // ZQL types.
  switch (type) {
    case 'boolean':
      return 'BOOL';
    case 'number':
      return 'INTEGER';
    default:
      return 'TEXT';
  }
}

function createTables(db: Database, forReplica: boolean) {
  const stmts: string[] = [];
  for (const t of TABLES) {
    const s = schema.tables[t];
    const cols = Object.entries(s.columns).map(
      ([name, c]) =>
        `"${name}" ${forReplica ? replicaTypeName(c.type) : toSQLiteTypeName(c.type)}`,
    );
    if (forReplica) {
      cols.push('_0_version TEXT NOT NULL');
    }
    const pk = s.primaryKey.map(k => `"${k}"`).join(', ');
    stmts.push(`CREATE TABLE "${t}" (${cols.join(', ')}, PRIMARY KEY (${pk}))`);
  }
  stmts.push(
    'CREATE INDEX issue_modified ON issue (modified)',
    'CREATE INDEX issue_open_modified ON issue (open, modified)',
    'CREATE INDEX issue_owner ON issue (ownerID)',
    'CREATE INDEX issue_project ON issue (projectID)',
    'CREATE INDEX comment_issue ON comment (issueID)',
    'CREATE INDEX comment_issue_created ON comment (issueID, created)',
    'CREATE INDEX comment_author ON comment (authorID)',
    'CREATE INDEX issueLabel_label ON issueLabel (labelID)',
    'CREATE INDEX label_name ON label (name)',
  );
  db.exec(stmts.join(';\n') + ';');
}

function sqliteValue(v: unknown) {
  return typeof v === 'boolean' ? (v ? 1 : 0) : v;
}

function loadRows(db: Database, forReplica: boolean) {
  db.exec('BEGIN');
  for (const t of TABLES) {
    const cols = Object.keys(schema.tables[t].columns);
    const allCols = forReplica ? [...cols, '_0_version'] : cols;
    const stmt = db.prepare(
      `INSERT INTO "${t}" (${allCols.map(c => `"${c}"`).join(', ')}) ` +
        `VALUES (${allCols.map(() => '?').join(', ')})`,
    );
    for (const row of data[t]) {
      const values = cols.map(c => sqliteValue(row[c]));
      if (forReplica) {
        values.push('123');
      }
      stmt.run(...values);
    }
  }
  db.exec('COMMIT');
  db.exec('ANALYZE');
}

const tmpDirs: string[] = [];
function tmp(): string {
  const d = mkdtempSync(join(tmpdir(), 'ivm-regression-'));
  tmpDirs.push(d);
  return d;
}
process.on('exit', () => {
  for (const d of tmpDirs) {
    rmSync(d, {recursive: true, force: true});
  }
});

/**
 * Operator storage like zero-cache's: a file-backed DatabaseStorage. Each
 * pipeline gets a fresh client group so that storage from earlier iterations
 * does not accumulate.
 */
function makeOperatorStorage(dir: string) {
  const storage = DatabaseStorage.create(lc, join(dir, 'storage.db'));
  let n = 0;
  let current = storage.createClientGroupStorage('cg' + n++);
  return {
    createStorage: () => maybeCounting(current.createStorage()),
    fresh: () => {
      current.destroy();
      current = storage.createClientGroupStorage('cg' + n++);
    },
    storage,
  };
}

function makeZqliteBackend(): Backend {
  const dir = tmp();
  const dbPath = join(dir, 'replica.db');
  const setup = new Database(lc, dbPath);
  setup.pragma('journal_mode = WAL2');
  createTables(setup, false);
  loadRows(setup, false);
  setup.close();

  const db = new Database(lc, dbPath);
  db.exec('BEGIN CONCURRENT');
  const delegate = newQueryDelegate(lc, testLogConfig, db, schema);
  const operatorStorage = makeOperatorStorage(dir);
  delegate.createStorage = operatorStorage.createStorage;
  return {
    hydrate: q => materialize(delegate, q, operatorStorage.fresh),
    prepare: () => {},
    push(muts) {
      for (const m of muts) {
        consume(must(delegate.getSource(m.table)).push(toSourceChange(m)));
      }
      return 0;
    },
  };
}

function makeDriverBackend(): Backend {
  const dir = tmp();
  const dbPath = join(dir, 'replica.db');
  const shardID = {appID: 'zeroz', shardNum: 1};
  const replica = new Database(lc, dbPath);
  replica.pragma('journal_mode = wal2');
  initReplicationState(replica, ['zero_data'], '123');
  replica.exec(`
    CREATE TABLE "${upstreamSchema(shardID)}.mutations" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "mutationID"     INTEGER,
      "result"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
    );`);
  createTables(replica, true);
  loadRows(replica, true);
  populateFromExistingTables(replica, listTables(replica, false));
  const replicator = fakeReplicator(lc, replica);
  const messages = new ReplicationMessages({
    user: 'id',
    project: 'id',
    issue: 'id',
    comment: 'id',
    label: 'id',
    issueLabel: ['issueID', 'labelID'],
  });

  const operatorStorage = makeOperatorStorage(dir);
  const anchorStorage = operatorStorage.storage.createClientGroupStorage('a');
  let buildingAnchors = true;
  const cgStorage: ClientGroupStorage = {
    createStorage: () =>
      buildingAnchors
        ? anchorStorage.createStorage()
        : operatorStorage.createStorage(),
    destroy: () => {},
  };

  const driver = new PipelineDriver(
    lc,
    testLogConfig,
    new Snapshotter(lc, dbPath, {appID: shardID.appID}),
    shardID,
    cgStorage,
    'ivm-regression',
    new InspectorDelegate(undefined),
    () => 1_000_000,
    true, // enablePlanner, as in production
  );
  driver.init(schema);
  const timer: Timer = {elapsedLap: () => 0, totalElapsed: () => 0};

  // Anchor queries keep a TableSource alive for every table, so that removing
  // the measured query does not tear the sources down.
  for (const t of TABLES) {
    const ast = asQueryInternals(anchorQuery(t)).ast;
    consume(driver.addQuery('anchor-' + t, 'anchor-' + t, ast, timer));
  }
  buildingAnchors = false;

  let version = 1_000_000;
  let queryNum = 0;
  const toMessage = (m: Mutation) => {
    const row: Record<string, JSONValue> = {};
    for (const [k, v] of Object.entries(m.row)) {
      row[k] = sqliteValue(v) as JSONValue;
    }
    switch (m.op) {
      case 'add':
        return messages.insert(m.table, row as RowValue);
      case 'edit':
        return messages.update(m.table, row as RowValue);
      case 'remove': {
        const key: Record<string, JSONValue> = {};
        for (const k of schema.tables[m.table].primaryKey) {
          key[k] = row[k];
        }
        return messages.delete(m.table, key);
      }
    }
  };
  return {
    hydrate(q) {
      const ast = asQueryInternals(q).ast;
      const queryID = 'q' + queryNum++;
      let rows = 0;
      for (const c of driver.addQuery('h' + queryID, queryID, ast, timer)) {
        if (c !== 'yield') {
          rows++;
        }
      }
      return {
        rows,
        teardown: () => {
          driver.removeQuery(queryID);
          operatorStorage.fresh();
        },
      };
    },
    prepare(muts) {
      replicator.processTransaction(
        versionToLexi(version++),
        ...muts.map(toMessage),
      );
    },
    push() {
      let n = 0;
      for (const c of driver.advance(timer).changes) {
        if (c !== 'yield') {
          n++;
        }
      }
      return n;
    },
  };
}

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

function stats(samples: number[]) {
  const s = samples.toSorted((a, b) => a - b);
  const q = (p: number) => s[Math.floor(p * (s.length - 1))];
  return {
    n: s.length,
    median: q(0.5),
    mean: s.reduce((a, b) => a + b, 0) / s.length,
    p10: q(0.1),
    p90: q(0.9),
    min: s[0],
  };
}

function emit(rec: Record<string, unknown>) {
  const line = JSON.stringify({label: LABEL, mode: MODE, ...rec});
  appendFileSync(OUT, line + '\n');
  process.stderr.write(line + '\n');
}

/** Slow cases (hundreds of ms per iteration) get fewer iterations. */
function minIters(samples: number[]): number {
  return samples.length > 0 && samples[0] > 150 ? 5 : MIN_ITERS;
}
function countItersFor(samples: number[]): number {
  return samples.length > 0 && samples[0] > 50 ? 1 : COUNT_ITERS;
}
function keepGoing(samples: number[], start: number): boolean {
  return (
    samples.length < MAX_ITERS &&
    (samples.length < minIters(samples) ||
      performance.now() - start < MIN_TIME_MS)
  );
}
function warmup(f: () => void) {
  const start = performance.now();
  for (let i = 0; i < WARMUP; i++) {
    if (i > 0 && performance.now() - start > 1500) {
      break;
    }
    f();
  }
}

function withCounting<T>(f: () => T): T {
  countStorageOps = true;
  countStatements(true);
  try {
    return f();
  } finally {
    countStatements(false);
    countStorageOps = false;
  }
}

function perOp(c: Counters, n: number) {
  return Object.fromEntries(counterNames.map(k => [k, c[k] / n]));
}

function benchHydration(backend: Backend, query: string, q: AnyQuery) {
  let rows = -1;
  warmup(() => {
    const p = backend.hydrate(q);
    rows = p.rows;
    p.teardown();
  });
  gc();
  const samples: number[] = [];
  const start = performance.now();
  while (keepGoing(samples, start)) {
    const t0 = performance.now();
    const p = backend.hydrate(q);
    samples.push(performance.now() - t0);
    p.teardown();
  }
  const n = countItersFor(samples);
  const counts = withCounting(() => {
    resetCounters();
    for (let i = 0; i < n; i++) {
      backend.hydrate(q).teardown();
    }
    return perOp(counters, n);
  });
  // Heap retained by a hydrated pipeline (operator state, views). Operator
  // storage in SQLite (zqlite, driver) is not on the heap.
  let heapBytes: number | undefined;
  if (env.IVM_REGRESSION_HEAP) {
    gc();
    const before = process.memoryUsage().heapUsed;
    const p = backend.hydrate(q);
    gc();
    heapBytes = process.memoryUsage().heapUsed - before;
    p.teardown();
  }
  emit({kind: 'hydrate', query, rows, ...stats(samples), counts, heapBytes});
}

function benchPush(backend: Backend, query: string, q: AnyQuery, w: Workload) {
  const step = (muts: Mutation[]): [ms: number, out: number] => {
    backend.prepare(muts);
    const t0 = performance.now();
    const out = backend.push(muts);
    return [performance.now() - t0, out];
  };

  let pipeline = backend.hydrate(q);
  try {
    let outFwd = 0;
    let outInv = 0;
    warmup(() => {
      outFwd = step(w.fwd)[1];
      outInv = step(w.inv)[1];
    });
    gc();
    const fwd: number[] = [];
    const inv: number[] = [];
    const start = performance.now();
    while (keepGoing(fwd, start)) {
      fwd.push(step(w.fwd)[0]);
      inv.push(step(w.inv)[0]);
    }
    pipeline.teardown();

    // Counting pass on a fresh pipeline (built with counting storage).
    const n = countItersFor(fwd);
    const [countsFwd, countsInv] = withCounting(() => {
      pipeline = backend.hydrate(q);
      const cf: Counters = {stmt: 0, sget: 0, sset: 0, sdel: 0, sscan: 0};
      const ci: Counters = {...cf};
      const addTo = (acc: Counters) => {
        for (const k of counterNames) {
          acc[k] += counters[k];
        }
      };
      for (let i = 0; i < n; i++) {
        resetCounters();
        step(w.fwd);
        addTo(cf);
        resetCounters();
        step(w.inv);
        addTo(ci);
      }
      return [perOp(cf, n), perOp(ci, n)];
    });
    pipeline.teardown();

    const common = {kind: 'push', query, workload: w.name};
    emit({
      ...common,
      dir: 'fwd',
      out: outFwd,
      ...stats(fwd),
      counts: countsFwd,
    });
    emit({
      ...common,
      dir: 'inv',
      out: outInv,
      ...stats(inv),
      counts: countsInv,
    });
  } catch (e) {
    emit({kind: 'error', query, workload: w.name, error: String(e)});
  }
}

function main() {
  const backend =
    MODE === 'memory'
      ? makeMemoryBackend()
      : MODE === 'zqlite'
        ? makeZqliteBackend()
        : makeDriverBackend();

  const kind = env.IVM_REGRESSION_KIND;
  const workloadFilter = env.IVM_REGRESSION_WORKLOAD;
  for (const [name, q] of Object.entries(QUERIES)) {
    if (FILTER && !name.includes(FILTER)) {
      continue;
    }
    if (!kind || kind === 'hydrate') {
      try {
        benchHydration(backend, name, q);
      } catch (e) {
        emit({kind: 'error', query: name, error: String(e)});
      }
    }
    if (!kind || kind === 'push') {
      for (const w of WORKLOADS) {
        if (
          (workloadFilter && !w.name.includes(workloadFilter)) ||
          (w.only && !w.only.test(name))
        ) {
          continue;
        }
        benchPush(backend, name, q, w);
      }
    }
  }
}

main();
