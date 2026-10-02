/**
 * Building a Zero IVM pipeline over either leaf, in either delivery mode.
 *
 * The two modes mirror rindle's, so the comparison never puts a contestant that
 * HOLDS a result against one that only STREAMS changes:
 *
 * - **view mode** — terminate the pipeline in an `ArrayView`, Zero's real
 *   materialized view and the client deliverable. Races rindle's view mode.
 * - **sink mode** — terminate it in a flattening `Output` that expands each
 *   change tree into flat per-row operations and counts them, which is the
 *   shape zero-cache's view-syncer consumes (`PipelineDriver` → `Streamer` →
 *   `RowChange`), minus the CVR bookkeeping. Races rindle's sink mode.
 */
import type {LogConfig} from '../../../otel/src/log-options.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {
  buildPipeline,
  type BuilderDelegate,
} from '../../../zql/src/builder/builder.ts';
import {ArrayView} from '../../../zql/src/ivm/array-view.ts';
import {ChangeType} from '../../../zql/src/ivm/change-type.ts';
import type {Change} from '../../../zql/src/ivm/change.ts';
import type {Node} from '../../../zql/src/ivm/data.ts';
import type {FilterInput} from '../../../zql/src/ivm/filter-operators.ts';
import {MemorySource} from '../../../zql/src/ivm/memory-source.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {
  Input,
  InputBase,
  Output,
  Storage,
} from '../../../zql/src/ivm/operator.ts';
import type {Source, SourceInput} from '../../../zql/src/ivm/source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeRemove,
} from '../../../zql/src/ivm/source.ts';
import type {Stream} from '../../../zql/src/ivm/stream.ts';
import {consume} from '../../../zql/src/ivm/stream.ts';
import type {View} from '../../../zql/src/ivm/view.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import {
  columnsOf,
  primaryKeyOf,
  sqliteDbFor,
  tableDef,
  type LogContext,
  type TableData,
} from './data.ts';
import {ast, format, needs, type PatternLabel} from './patterns.ts';

export type Leaf = 'memory' | 'table';
export type Mode = 'view' | 'sink';
/** Where operators keep their per-node state. */
export type StorageKind = 'memory' | 'sqlite';

// ---------------------------------------------------------------------------
// Delegate
// ---------------------------------------------------------------------------

class BenchDelegate implements BuilderDelegate {
  readonly enableNotExists = true;
  readonly #sources: Record<string, Source>;
  readonly #newStorage: () => Storage;
  readonly #storages: Storage[] = [];

  constructor(sources: Record<string, Source>, newStorage: () => Storage) {
    this.#sources = sources;
    this.#newStorage = newStorage;
  }

  getSource(tableName: string): Source | undefined {
    return this.#sources[tableName];
  }

  createStorage(): Storage {
    const s = this.#newStorage();
    this.#storages.push(s);
    return s;
  }

  decorateInput(input: Input): Input {
    return input;
  }

  decorateFilterInput(input: FilterInput): FilterInput {
    return input;
  }

  decorateSourceInput(input: SourceInput): Input {
    return input;
  }

  addEdge(_source: InputBase, _dest: InputBase): void {}
}

// ---------------------------------------------------------------------------
// Sink: flatten a change tree into per-row operations, the zero-cache shape
// ---------------------------------------------------------------------------

function countNode(node: Node): number {
  let n = 1;
  for (const rel of Object.values(node.relationships)) {
    for (const child of rel()) {
      if (child !== 'yield') {
        n += countNode(child);
      }
    }
  }
  return n;
}

function countChange(change: Change): number {
  switch (change[0]) {
    case ChangeType.ADD:
    case ChangeType.REMOVE:
      return countNode(change[1]);
    case ChangeType.EDIT:
      return 1;
    case ChangeType.CHILD:
      return countChange(change[2].change);
    default:
      return 1;
  }
}

/**
 * The analogue of rindle's change sink.
 *
 * `changes` counts TOP-LEVEL changes — the same thing rindle's
 * `take_sink_changes()` returns a `Vec` of, so the parity check compares like
 * with like. `rowOps` counts the flattened per-row operations underneath, which
 * is what a real consumer (zero-cache's `Streamer`) actually writes out. The
 * relationship streams are walked either way: they are lazy and single-use, and
 * a sink that skipped them would be charging the engine less work than the
 * production path does.
 */
export class CountingSink implements Output {
  changes = 0;
  rowOps = 0;
  readonly #input: Input;

  constructor(input: Input) {
    this.#input = input;
    input.setOutput(this);
  }

  /** Hydration: the initial change set, handed off (counted, not held). */
  hydrate(): number {
    let changes = 0;
    let rowOps = 0;
    for (const node of this.#input.fetch({})) {
      if (node !== 'yield') {
        changes++;
        rowOps += countNode(node);
      }
    }
    this.changes = changes;
    this.rowOps = rowOps;
    return changes;
  }

  push(change: Change): Stream<'yield'> {
    this.changes++;
    this.rowOps += countChange(change);
    return [];
  }

  /** Per-write drain: how many top-level changes this write produced. */
  take(): number {
    const n = this.changes;
    this.changes = 0;
    this.rowOps = 0;
    return n;
  }

  destroy(): void {
    this.#input.destroy();
  }
}

// ---------------------------------------------------------------------------
// Leak check
// ---------------------------------------------------------------------------

/**
 * A `Source` that counts live connections.
 *
 * This exists because of a bug that silently corrupted an entire benchmark run:
 * `ArrayView.destroy()` calls only its `onDestroy` callback — in real Zero the
 * view factory supplies one that tears the pipeline down — so a harness that
 * builds an `ArrayView` directly and never sets `onDestroy` leaks a source
 * connection per build. `Source.push` fans out to every live connection, so
 * after the hydrate loop the push measurements were reporting the cost of
 * pushing into hundreds of abandoned pipelines: `top50` on the SQLite leaf read
 * 801 ms per write instead of ~1 ms.
 *
 * `assertNoLeak` is called at the seams so that failure mode is loud and
 * immediate rather than a plausible-looking 17,000× result.
 */
class ConnectionCounter {
  live = 0;
}

class LeakCheckedSource implements Source {
  readonly #inner: Source;
  readonly #counter: ConnectionCounter;

  constructor(inner: Source, counter: ConnectionCounter) {
    this.#inner = inner;
    this.#counter = counter;
  }

  get tableSchema() {
    return this.#inner.tableSchema;
  }

  connect(
    sort: Parameters<Source['connect']>[0],
    filters?: Parameters<Source['connect']>[1],
    splitEditKeys?: Parameters<Source['connect']>[2],
    debug?: Parameters<Source['connect']>[3],
  ): SourceInput {
    const input = this.#inner.connect(sort, filters, splitEditKeys, debug);
    this.#counter.live++;
    let destroyed = false;
    return {
      ...input,
      fetch: req => input.fetch(req),
      setOutput: output => input.setOutput(output),
      getSchema: () => input.getSchema(),
      destroy: () => {
        if (!destroyed) {
          destroyed = true;
          this.#counter.live--;
        }
        input.destroy();
      },
    };
  }

  push(change: Parameters<Source['push']>[0]) {
    return this.#inner.push(change);
  }

  genPush(change: Parameters<Source['genPush']>[0]) {
    return this.#inner.genPush(change);
  }
}

// ---------------------------------------------------------------------------
// Sources
// ---------------------------------------------------------------------------

export type Sources = {
  readonly leaf: Leaf;
  /** Template memory sources; each pipeline gets a `fork()` of these. */
  readonly mem: Record<string, MemorySource>;
  /** Shared table sources (one SQLite connection each), like rindle. */
  readonly tab: Record<string, LeakCheckedSource>;
  readonly dbs: Database[];
  readonly storage: () => Storage;
  /** Live source connections, shared sources and per-pipeline forks alike. */
  liveConnections(): number;
  readonly counter: ConnectionCounter;
  close(): void;
};

/**
 * Throw if a pipeline was built and not torn down. Called after every loop that
 * builds pipelines, so a teardown regression cannot quietly inflate the numbers.
 */
export function assertNoLeak(srcs: Sources, where: string): void {
  const live = srcs.liveConnections();
  if (live !== 0) {
    throw new Error(
      `${where}: ${live} source connection(s) leaked — a pipeline was built ` +
        `without being destroyed, which would make every later push fan out ` +
        `into abandoned pipelines and report nonsense`,
    );
  }
}

export function buildSources(
  lc: LogContext,
  logConfig: LogConfig,
  data: TableData,
  leaf: Leaf,
  storageKind: StorageKind,
): Sources {
  const mem: Record<string, MemorySource> = {};
  const tab: Record<string, LeakCheckedSource> = {};
  const dbs: Database[] = [];
  // One counter for every source this cell hands to a pipeline — shared sources
  // and per-pipeline memory forks alike. A plain counter, not a registry of
  // sources: holding forks alive to count them would itself distort the heap
  // measurement it exists to protect.
  const counter = new ConnectionCounter();

  for (const name of Object.keys(data)) {
    const def = tableDef(name);
    const columns = columnsOf(def);
    const pk = primaryKeyOf(def);
    if (leaf === 'memory') {
      const src = new MemorySource(name, columns, pk);
      for (const row of data[name]) {
        consume(src.push(makeSourceChangeAdd(row)));
      }
      mem[name] = src;
    } else {
      const db = sqliteDbFor(lc, logConfig, def, data[name]);
      dbs.push(db);
      tab[name] = new LeakCheckedSource(
        new TableSource(lc, logConfig, db, name, columns, pk),
        counter,
      );
    }
  }

  let storage: () => Storage;
  let storageDb: Database | undefined;
  if (storageKind === 'sqlite') {
    // zero-cache keeps operator state in SQLite (`DatabaseStorage`), not on the
    // JS heap. Same seam rindle's `OperatorStorage` occupies.
    storageDb = new Database(lc, ':memory:');
    storageDb.prepare(CREATE_STORAGE_TABLE).run();
    const cgs = new DatabaseStorage(storageDb).createClientGroupStorage(
      'bench',
    );
    storage = () => cgs.createStorage();
  } else {
    storage = () => new MemoryStorage();
  }

  return {
    leaf,
    mem,
    tab,
    dbs,
    storage,
    counter,
    liveConnections: () => counter.live,
    close() {
      for (const db of dbs) {
        db.close();
      }
      storageDb?.close();
    },
  };
}

/**
 * A fresh per-pipeline source map. Memory sources are forked per pipeline (as
 * rindle's are), so each fork gets its own leak counter; table sources are
 * shared, so their counter is the long-lived one.
 */
function sourcesFor(srcs: Sources, tables: readonly string[]) {
  const out: Record<string, Source> = {};
  for (const t of tables) {
    out[t] =
      srcs.leaf === 'memory'
        ? new LeakCheckedSource(srcs.mem[t].fork(), srcs.counter)
        : srcs.tab[t];
  }
  return out;
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

export type BuiltView = {
  readonly view: ArrayView<View>;
  readonly rows: number;
  /** The sources THIS pipeline reads — writes must go here, not to a template. */
  readonly sources: Record<string, Source>;
  destroy(): void;
};

export function buildView(pattern: PatternLabel, srcs: Sources): BuiltView {
  const sources = sourcesFor(srcs, needs(pattern));
  const delegate = new BenchDelegate(sources, srcs.storage);
  const input = buildPipeline(ast(pattern), delegate, 'q');
  // `ArrayView.destroy()` only invokes its `onDestroy` callback; the pipeline
  // teardown is the callback's job. Zero's own view factories pass one that
  // destroys the input — so must we, or every build leaks a source connection.
  const view = new ArrayView<View>(input, format(pattern), true, () => {});
  view.onDestroy = () => input.destroy();
  const data = view.data;
  return {
    view,
    rows: Array.isArray(data) ? data.length : 0,
    sources,
    destroy: () => view.destroy(),
  };
}

export type BuiltSink = {
  readonly sink: CountingSink;
  readonly sources: Record<string, Source>;
  destroy(): void;
};

export function buildSink(pattern: PatternLabel, srcs: Sources): BuiltSink {
  const sources = sourcesFor(srcs, needs(pattern));
  const delegate = new BenchDelegate(sources, srcs.storage);
  const input = buildPipeline(ast(pattern), delegate, 'q');
  const sink = new CountingSink(input);
  return {sink, sources, destroy: () => sink.destroy()};
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/**
 * One write = one transaction = one maintenance step (rindle fairness rule 4).
 * For the memory leaf the write goes to the forked source the pipeline holds;
 * for the table leaf it goes to the shared `TableSource`.
 */
export function pushAdd(src: Source, row: Row): void {
  consume(src.push(makeSourceChangeAdd(row)));
}

export function pushRemove(src: Source, row: Row): void {
  consume(src.push(makeSourceChangeRemove(row)));
}
