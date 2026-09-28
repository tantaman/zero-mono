import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import {deepEqual, type JSONValue} from '../../../../shared/src/json.ts';
import {must} from '../../../../shared/src/must.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
  type SourceChange,
} from '../../../../zql/src/ivm/source.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import type {LogConfig} from '../../config/zero-config.ts';
import {computeZqlSpecs, mustGetTableSpec} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec, LiteTableSpec} from '../../db/specs.ts';
import type {StatementRunner} from '../../db/statements.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {Subscription} from '../../types/subscription.ts';
import {coalesceReplicaStates, Notifier} from '../replicator/notifier.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {advancementTimeout} from './advancement-timeout.ts';
import {
  ResetPipelinesSignal,
  type Change,
  type Snapshotter,
} from './snapshotter.ts';

/**
 * The client groups of a sync worker take part in a {@link SharedSnapshot}
 * through this interface (implemented by the `PipelineDriver`).
 */
export interface SharedSnapshotMember {
  /**
   * Whether the member has pipelines, i.e. connections to the shared
   * sources, through which it receives the changes of a round.
   */
  hasPipelines(): boolean;

  /** The time it took to hydrate the member's pipelines. */
  totalHydrationTimeMs(): number;

  /** Called before the first change of a round's diff is pushed. */
  beginRound(round: RoundProgress): void;

  /**
   * Moves the row changes that the member's pipelines have output since the
   * last call into the member's buffer for the round. Called after each step
   * of a push to a shared source, if the member was marked with
   * {@link SharedSnapshot.markOutput()} in the meantime.
   */
  drainRound(): void;

  /** Called when the round's diff ends, whether or not it completed. */
  endRound(): void;

  /**
   * Whether the member should yield, for a read of a shared source that it
   * makes outside of a round (i.e. while hydrating a query).
   */
  shouldYield(): boolean;
}

/** How far a round's diff has progressed. */
export type RoundProgress = {
  /** The number of changes in the diff. */
  readonly numChanges: number;
  /** The number of changes processed so far. */
  readonly pos: number;
  /** The table of the change being pushed. */
  readonly currentTable: string | undefined;
};

/** What a round did, as received by the members that were parked for it. */
export type RoundOutcome =
  | {
      readonly type: 'advanced';
      readonly version: string;
      readonly numChanges: number;
    }
  | {
      /**
       * The round was abandoned (e.g. a schema change or an advancement
       * timeout). Every member must reset its pipelines, which are no longer
       * consistent with the shared sources.
       */
      readonly type: 'reset';
      readonly version: string;
      readonly signal: ResetPipelinesSignal;
    }
  | {
      /** The round failed unexpectedly. The shared sources were reset. */
      readonly type: 'failed';
      readonly version: string;
      readonly error: unknown;
    };

/**
 * The timer of a round, with which it measures its progress and yields the
 * thread. (This is the view-syncer's `TimeSliceTimer`.)
 */
export interface RoundTimer {
  start(): Promise<unknown>;
  yieldProcess(msgForTesting?: string): Promise<void>;
  elapsedLap(): number;
  totalElapsed(): number;
}

type RoundContext = RoundProgress & {
  readonly timer: RoundTimer;
  readonly totalHydrationTimeMs: number;
  pos: number;
  currentTable: string | undefined;
  currentChangeStartMs: number | undefined;
};

type Round = {
  phase: 'gathering' | 'diffing';
  readonly startMs: number;
  /** The members that are parked for the round, by when they parked. */
  readonly parked: Map<SharedSnapshotMember, (o: RoundOutcome) => void>;
  /** Members that registered, and then parked, after the diff began. */
  readonly late: ((o: RoundOutcome) => void)[];
};

/** Passed to {@link RoundTimer.yieldProcess}, for testing. */
export const YIELD_MESSAGE = 'yield in shared advancement';

/** A round that waits longer than this for its members is logged. */
const SLOW_GATHERING_LOG_THRESHOLD_MS = 1000;

/**
 * One snapshot of the replica, and one {@link TableSource} per table, shared
 * by all of the client groups of a sync worker, which it advances together.
 *
 * Without it, every client group has a `Snapshotter` of its own (with two
 * SQLite connections), its own sources, and advances by itself: it reads the
 * change log, and each change (including its previous values), and writes the
 * change to its own snapshot, once per client group. With it, the change log
 * is diffed once per worker, and each change is read, written, and pushed to
 * the shared sources once, which push it to the pipelines of every client
 * group. Each client group still has its own pipelines, CVR, and pokes.
 *
 * ### Rounds
 *
 * The shared sources can only move to the next snapshot when no client group
 * is using them, and every client group must process the changes of one
 * snapshot (i.e. update its CVR and poke its clients) before it can do
 * anything else with its pipelines. So the client groups advance in lockstep,
 * one "round" at a time:
 *
 * 1. A notification that the replica has changed starts a round, which the
 *    SharedSnapshot relays to every client group (see {@link subscribe()}).
 * 2. Each client group, upon its notification, takes the lock that it holds
 *    for anything it does with its pipelines, and {@link park}s.
 * 3. Once every registered client group is parked, the change log is diffed
 *    and its changes are pushed to the shared sources. The changes that each
 *    client group's pipelines output are buffered for it.
 * 4. The client groups are released, and each processes its changes and
 *    releases its lock.
 *
 * The cost is head-of-line blocking: a round waits for the slowest client
 * group to park, e.g. one that is hydrating a query or waiting on a query
 * transform, and the next round waits for every client group to finish
 * processing the changes of this one.
 *
 * Since a round only diffs when every client group is parked, a client group
 * that is not parked (e.g. one hydrating a query) never sees the shared
 * sources in the middle of a round.
 *
 * ### Failures
 *
 * The pipelines of a client group that fail while a change is pushed (e.g. a
 * scalar subquery whose value changed) are cut off from the rest of the
 * round, and the client group resets them; the other client groups are not
 * affected. A diff that is abandoned (a schema change, a truncation, a change
 * of permissions, or taking longer than rehydrating every pipeline would)
 * resets the pipelines of every client group.
 *
 * ### Limitations
 *
 * * The client groups must agree on the primary key of each table that their
 *   queries read, since they share its source.
 * * Changes are written through to the snapshot (i.e. `deferIvmWrites` does
 *   not apply), which is done once per worker.
 */
export class SharedSnapshot {
  readonly #lc: LogContext;
  readonly #logConfig: LogConfig;
  readonly #snapshotter: Snapshotter;
  readonly #yieldThresholdMs: () => number;
  readonly #newTimer: () => RoundTimer;

  readonly #notifier = new Notifier();
  readonly #members = new Set<SharedSnapshotMember>();
  readonly #sources = new Map<string, TableSource>();
  /** Members that output row changes since the last drain. */
  readonly #output = new Set<SharedSnapshotMember>();

  #tableSpecs = new Map<string, LiteAndZqlSpec>();
  #fullTables = new Map<string, LiteTableSpec>();
  #allTableNames = new Set<string>();
  #replicaVersion: string | undefined;
  #specsEpoch = 0;

  /** A notification of a replica change that has not started a round yet. */
  #pending: ReplicaState | undefined;
  #round: Round | undefined;
  /** Set while a round is diffing. */
  #diffing: RoundContext | undefined;
  /** The signal with which the diff in progress is being abandoned. */
  #abort: ResetPipelinesSignal | undefined;
  /** The member running outside of a round, e.g. hydrating a query. */
  #running: SharedSnapshotMember | undefined;

  readonly #roundGatherTime = getOrCreateLatencyHistogram(
    'sync',
    'ivm.shared-round-gather-time',
    'Time a round of the shared snapshot waits for every client group of the ' +
      'sync worker to be ready to advance (i.e. head-of-line blocking).',
  );
  readonly #roundTime = getOrCreateLatencyHistogram(
    'sync',
    'ivm.shared-round-time',
    'Time to diff the replica and push its changes to the pipelines of every ' +
      'client group of a sync worker, in a round of the shared snapshot.',
  );
  readonly #roundResets = getOrCreateCounter(
    'sync',
    'ivm.shared-round-resets',
    'Number of rounds of the shared snapshot that were abandoned, resetting ' +
      'the pipelines of every client group of the sync worker.',
  );
  readonly #conflictRowsDeleted = getOrCreateCounter(
    'sync',
    'ivm.conflict-rows-deleted',
    'Number of rows deleted because they conflicted with added row',
  );

  /**
   * @param snapshotter A Snapshotter that is not used by anything else.
   * @param newTimer Creates the timer for a round.
   */
  constructor(
    lc: LogContext,
    logConfig: LogConfig,
    snapshotter: Snapshotter,
    yieldThresholdMs: () => number,
    newTimer: () => RoundTimer,
  ) {
    this.#lc = lc.withContext('component', 'shared-snapshot');
    this.#logConfig = logConfig;
    this.#snapshotter = snapshotter;
    this.#yieldThresholdMs = yieldThresholdMs;
    this.#newTimer = newTimer;
  }

  /**
   * Starts a round for each notification from `replicaStates` (coalescing
   * those that arrive while a round is in progress). Rounds are relayed to
   * the client groups through {@link subscribe()}.
   */
  async relay(replicaStates: Source<ReplicaState>): Promise<void> {
    for await (const state of replicaStates) {
      this.#pending = this.#pending
        ? coalesceReplicaStates(state, this.#pending)
        : state;
      this.#maybeStartRound();
    }
  }

  /**
   * The notifications for a client group, one for each round (and one on
   * subscribing, if there has been a round), upon each of which it must
   * {@link park()} once it has been {@link register()}ed.
   */
  subscribe(): Subscription<ReplicaState> {
    return this.#notifier.subscribe();
  }

  /**
   * Adds a member, which must then {@link park()} for every round, until it
   * is {@link unregister()}ed. Initializes the snapshot to the head of the
   * replica if this is the first member.
   *
   * A member can register while a round is diffing, in which case it must
   * not use the shared sources until it has parked, which releases it when
   * the diff is done.
   */
  register(member: SharedSnapshotMember) {
    assert(!this.#members.has(member), 'already registered');
    if (!this.#snapshotter.initialized()) {
      this.#snapshotter.init();
      this.#computeSpecs();
    }
    this.#members.add(member);
  }

  unregister(member: SharedSnapshotMember) {
    if (!this.#members.delete(member)) {
      return;
    }
    this.#output.delete(member);
    if (this.#running === member) {
      this.#running = undefined;
    }
    const round = this.#round;
    const release = round?.parked.get(member);
    if (release && round?.phase === 'gathering') {
      round.parked.delete(member);
      release(this.#currentOutcome());
    }
    this.#maybeRunRound();
  }

  /**
   * Parks a member for the next round, resolving when the round is done.
   * If no round is pending, resolves right away, as the member is at the
   * current snapshot.
   *
   * A member parks once per notification from {@link subscribe()}, holding
   * whatever lock it holds for using its pipelines, and must not use them
   * until this resolves.
   */
  park(member: SharedSnapshotMember): Promise<RoundOutcome> {
    assert(this.#members.has(member), 'must register() before park()');
    if (this.#round === undefined) {
      if (this.#pending === undefined) {
        return Promise.resolve(this.#currentOutcome());
      }
      this.#maybeStartRound();
    }
    const round = must(this.#round);
    const {promise, resolve} = resolver<RoundOutcome>();
    if (round.phase === 'gathering') {
      assert(!round.parked.has(member), 'already parked');
      round.parked.set(member, resolve);
      this.#maybeRunRound();
    } else {
      // A member that registered after the diff began (see register()), so
      // it has nothing to receive from it.
      assert(
        !round.parked.has(member) && !member.hasPipelines(),
        'parked during a diff',
      );
      round.late.push(resolve);
    }
    return promise;
  }

  /** The version of the current snapshot. */
  get version(): string {
    return this.#snapshotter.current().version;
  }

  get replicaVersion(): string {
    return must(this.#replicaVersion, 'not initialized');
  }

  /** The current snapshot, for reads that do not go through the sources. */
  get db(): StatementRunner {
    return this.#snapshotter.current().db;
  }

  /**
   * Incremented whenever the table specs are recomputed (i.e. when a round
   * is abandoned, or when the snapshot moves past a schema change). Members
   * must reset when it changes.
   */
  get specsEpoch(): number {
    return this.#specsEpoch;
  }

  get tableSpecs(): ReadonlyMap<string, LiteAndZqlSpec> {
    return this.#tableSpecs;
  }

  get fullTables(): ReadonlyMap<string, LiteTableSpec> {
    return this.#fullTables;
  }

  /**
   * Returns the shared source of the table, creating it if necessary. Every
   * member that reads the table must use the same primary key for it.
   */
  getSource(table: string, primaryKey: PrimaryKey): TableSource {
    const existing = this.#sources.get(table);
    if (existing) {
      const shared = existing.tableSchema.primaryKey;
      if (!deepEqual(shared, primaryKey)) {
        throw new Error(
          `The client groups on a sync worker share the source of the ` +
            `"${table}" table, so they must agree on its primary key, but ` +
            `<${primaryKey.join(',')}> differs from <${shared.join(',')}>.`,
        );
      }
      return existing;
    }
    const {zqlSpec} = mustGetTableSpec(this.#tableSpecs, table);
    const source = new TableSource(
      this.#lc,
      this.#logConfig,
      this.#snapshotter.current().db.db,
      table,
      zqlSpec,
      primaryKey,
      () => this.#shouldYield(),
      // Pipelines only read tables through their connections, and the sources
      // are moved to the next snapshot after every round.
      {skipUnobservableChanges: true},
    );
    this.#sources.set(table, source);
    this.#lc.debug?.(`created shared TableSource for ${table}`);
    return source;
  }

  /** The shared source of the table, if a pipeline reads it. */
  getSourceIfExists(table: string): TableSource | undefined {
    return this.#sources.get(table);
  }

  /** Drops the sources that no pipeline reads. */
  pruneUnusedSources() {
    for (const [table, source] of this.#sources) {
      if (!source.hasConnections()) {
        this.#sources.delete(table);
      }
    }
  }

  /**
   * The rows read by all of the shared sources, over their lifetime. Rows
   * read by one member cannot be told apart from those read by another that
   * runs at the same time.
   */
  rowsRead(): number {
    let total = 0;
    for (const source of this.#sources.values()) {
      total += source.rowsRead;
    }
    return total;
  }

  sourceTables(): Iterable<string> {
    return this.#sources.keys();
  }

  /**
   * Runs `fn` on behalf of `member` outside of a round (e.g. a step of
   * hydrating a query), so that its reads of the shared sources yield when
   * the member says to.
   */
  runAs<T>(member: SharedSnapshotMember, fn: () => T): T {
    const prev = this.#running;
    this.#running = member;
    try {
      return fn();
    } finally {
      this.#running = prev;
    }
  }

  /**
   * Called by a member when its pipelines output a row change during a
   * round, so that it is drained after the current step of the push.
   */
  markOutput(member: SharedSnapshotMember) {
    this.#output.add(member);
  }

  /**
   * Whether `e` was thrown to abandon the diff in progress, and must be
   * propagated, rather than be treated as a failure of the member whose
   * pipeline it was thrown through.
   */
  isRoundAbort(e: unknown): boolean {
    return e !== undefined && e === this.#abort;
  }

  #computeSpecs() {
    const {db} = this.#snapshotter.current();
    const tableSpecs = new Map<string, LiteAndZqlSpec>();
    const fullTables = new Map<string, LiteTableSpec>();
    computeZqlSpecs(
      this.#lc,
      db.db,
      {includeBackfillingColumns: false},
      tableSpecs,
      fullTables,
    );
    this.#tableSpecs = tableSpecs;
    this.#fullTables = fullTables;
    this.#allTableNames = new Set(fullTables.keys());
    this.#replicaVersion = getSubscriptionState(db).replicaVersion;
    this.#specsEpoch++;
  }

  #currentOutcome(): RoundOutcome {
    return {type: 'advanced', version: this.version, numChanges: 0};
  }

  #maybeStartRound() {
    if (this.#round !== undefined || this.#pending === undefined) {
      return;
    }
    const state = this.#pending;
    this.#pending = undefined;
    this.#round = {
      phase: 'gathering',
      startMs: performance.now(),
      parked: new Map(),
      late: [],
    };
    // Each subscriber parks upon this, once it has registered.
    void this.#notifier.notifySubscribers(state);
    this.#maybeRunRound();
  }

  #maybeRunRound() {
    const round = this.#round;
    if (round?.phase !== 'gathering') {
      return;
    }
    for (const member of this.#members) {
      if (!round.parked.has(member)) {
        return;
      }
    }
    round.phase = 'diffing';
    void this.#runRound(round);
  }

  async #runRound(round: Round) {
    const members = [...round.parked.keys()];
    let outcome: RoundOutcome | undefined;
    try {
      const gatherTimeMs = performance.now() - round.startMs;
      this.#roundGatherTime.recordMs(gatherTimeMs);
      if (gatherTimeMs > SLOW_GATHERING_LOG_THRESHOLD_MS) {
        this.#lc.info?.(
          `round waited ${gatherTimeMs.toFixed(0)} ms for ${members.length} ` +
            `client groups to be ready to advance`,
        );
      }
      outcome = await this.#advance(members);
    } catch (e) {
      this.#lc.error?.(`shared advancement failed`, e);
      this.#sources.clear();
      outcome = {type: 'failed', version: this.version, error: e};
    } finally {
      this.#round = undefined;
    }
    // Parked members imply an initialized snapshot, and thus an outcome.
    if (outcome) {
      for (const release of round.parked.values()) {
        release(outcome);
      }
    }
    for (const release of round.late) {
      release(this.#currentOutcome());
    }
    this.#maybeStartRound();
  }

  /**
   * Advances the snapshot to the head of the replica, pushing the changes to
   * the shared sources.
   */
  async #advance(
    members: SharedSnapshotMember[],
  ): Promise<RoundOutcome | undefined> {
    if (!this.#snapshotter.initialized()) {
      assert(members.length === 0, 'members registered without a snapshot');
      return undefined;
    }
    if (![...this.#sources.values()].some(s => s.hasConnections())) {
      // Nothing observes the changes, so move to head without diffing.
      const {prev, curr} = this.#snapshotter.advanceWithoutDiff();
      if (curr.schemaChangedSince(prev.version)) {
        this.#sources.clear();
        this.#computeSpecs();
      } else {
        for (const source of this.#sources.values()) {
          source.setDB(curr.db.db);
        }
      }
      return {type: 'advanced', version: curr.version, numChanges: 0};
    }

    const timer = this.#newTimer();
    await timer.start();
    const diff = this.#snapshotter.advance(
      this.#tableSpecs,
      this.#allTableNames,
      this.#sources,
    );
    const {prev, curr} = diff;
    this.#lc.debug?.(
      `advancing ${members.length} client groups ${prev.version} => ` +
        `${curr.version}: ${diff.changes} changes`,
    );
    const round: RoundContext = {
      timer,
      totalHydrationTimeMs: members.reduce(
        (total, m) => total + m.totalHydrationTimeMs(),
        0,
      ),
      numChanges: diff.changes,
      pos: 0,
      currentTable: undefined,
      currentChangeStartMs: undefined,
    };
    this.#diffing = round;
    for (const member of members) {
      member.beginRound(round);
    }
    try {
      for (const change of diff) {
        // Progress is checked each time a row is fetched from a source during
        // a push, but some pushes read no rows.
        if (this.#checkRound(round)) {
          await timer.yieldProcess(YIELD_MESSAGE);
        }
        round.currentChangeStartMs = timer.totalElapsed();
        round.currentTable = change.table;
        try {
          await this.#pushChange(change);
        } finally {
          round.pos++;
          round.currentChangeStartMs = undefined;
        }
        this.#checkRound(round, false);
      }
      for (const source of this.#sources.values()) {
        source.setDB(curr.db.db);
      }
      return {
        type: 'advanced',
        version: curr.version,
        numChanges: diff.changes,
      };
    } catch (e) {
      if (!(e instanceof ResetPipelinesSignal)) {
        throw e;
      }
      this.#lc.info?.(`resetting all client groups: ${e.message}`);
      this.#roundResets.add(1, {reason: e.reason});
      // The sources hold the changes pushed so far, and the pipelines of
      // every member are reset, so start over at `curr`.
      this.#sources.clear();
      this.#computeSpecs();
      return {type: 'reset', version: curr.version, signal: e};
    } finally {
      this.#diffing = undefined;
      this.#abort = undefined;
      this.#drain();
      for (const member of members) {
        member.endRound();
      }
      this.#roundTime.recordMs(timer.totalElapsed());
    }
  }

  async #pushChange({table, prevValues, nextValue}: Change) {
    const source = this.#sources.get(table);
    if (!source) {
      return; // no pipelines read from this table
    }
    const {primaryKey} = source.tableSchema;
    let editOldRow: Row | undefined = undefined;
    for (const prevValue of prevValues) {
      if (nextValue && sameKey(primaryKey, prevValue, nextValue)) {
        editOldRow = prevValue;
      } else {
        if (nextValue) {
          this.#conflictRowsDeleted.add(1);
        }
        await this.#push(source, makeSourceChangeRemove(prevValue));
      }
    }
    if (nextValue) {
      await this.#push(
        source,
        editOldRow
          ? makeSourceChangeEdit(nextValue, editOldRow)
          : makeSourceChangeAdd(nextValue),
      );
    }
  }

  async #push(source: TableSource, change: SourceChange) {
    for (const val of source.genPush(change)) {
      this.#drain();
      if (val === 'yield') {
        await must(this.#diffing).timer.yieldProcess(YIELD_MESSAGE);
      }
    }
    this.#drain();
  }

  #drain() {
    for (const member of this.#output) {
      member.drainRound();
    }
    this.#output.clear();
  }

  #shouldYield(): boolean {
    const round = this.#diffing;
    if (round) {
      return this.#checkRound(round);
    }
    return this.#running?.shouldYield() ?? false;
  }

  /**
   * Abandons the round if it is taking longer than rehydrating the pipelines
   * of every member would. Otherwise returns whether to yield.
   */
  #checkRound(round: RoundContext, checkYield = true): boolean {
    const {timer, currentChangeStartMs, pos, numChanges, totalHydrationTimeMs} =
      round;
    const elapsed = timer.totalElapsed();
    const timeout = advancementTimeout({
      elapsedMs: elapsed,
      currentChangeElapsedMs:
        currentChangeStartMs === undefined
          ? undefined
          : elapsed - currentChangeStartMs,
      pos,
      numChanges,
      totalHydrationTimeMs,
    });
    if (timeout) {
      this.#abort = timeout;
      throw timeout;
    }
    return checkYield && timer.elapsedLap() > this.#yieldThresholdMs();
  }
}

function sameKey(primaryKey: PrimaryKey, a: Row, b: Row): boolean {
  return primaryKey.every(col =>
    deepEqual(a[col] as JSONValue, b[col] as JSONValue),
  );
}
