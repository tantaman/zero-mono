import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import type {ShardID} from '../../types/shards.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {PipelineDriver, type RowChange} from './pipeline-driver.ts';
import {SharedSnapshot, type RoundTimer} from './shared-snapshot.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';
import {TimeSliceTimer} from './view-syncer.ts';

const shardID: ShardID = {appID: 'zeroz', shardNum: 1};

const issues = table('issues')
  .columns({id: string(), title: string()})
  .primaryKey('id');
const comments = table('comments')
  .columns({id: string(), issueID: string(), upvotes: number()})
  .primaryKey('id');
const clientSchema = createSchema({tables: [issues, comments]});

const ISSUES_AND_COMMENTS: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  related: [
    {
      system: 'client',
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [['id', 'asc']],
      },
    },
  ],
};

const TOP_COMMENT: AST = {
  table: 'comments',
  orderBy: [
    ['upvotes', 'desc'],
    ['id', 'asc'],
  ],
  limit: 1,
};

const ISSUE_1_COMMENTS: AST = {
  table: 'comments',
  orderBy: [['id', 'asc']],
  where: {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'issueID'},
    right: {type: 'literal', value: '1'},
  },
};

/** Issues whose comment 10 is on them, resolved as a scalar subquery. */
const ISSUES_WITH_COMMENT_10: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'correlatedSubquery',
    op: 'EXISTS',
    scalar: true,
    related: {
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table: 'comments',
        orderBy: [['id', 'asc']],
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: '10'},
        },
      },
    },
  },
};

const messages = new ReplicationMessages({issues: 'id', comments: 'id'});

const NO_TIME_TIMER: RoundTimer = {
  start: () => Promise.resolve(),
  yieldProcess: () => Promise.resolve(),
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

type Queries = Record<string, AST>;

/** Orders the changes of an advancement for comparison. */
function sorted(changes: readonly RowChange[]) {
  const key = (c: RowChange) =>
    `${c.queryID}:${c.table}:${JSON.stringify(c.rowKey)}:${c.type}`;
  return changes.toSorted((a, b) => key(a).localeCompare(key(b)));
}

describe('view-syncer/shared-snapshot', () => {
  let lc: LogContext;
  let dbFile: DbFile;
  let replicator: FakeReplicator;
  let storage: DatabaseStorage;
  let shared: SharedSnapshot;
  let newTimer: () => RoundTimer;

  beforeEach(() => {
    lc = new LogContext('error', undefined, new TestLogSink());
    dbFile = new DbFile('shared_snapshot_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');

    const storageDB = new Database(lc, ':memory:');
    storageDB.prepare(CREATE_STORAGE_TABLE).run();
    storage = new DatabaseStorage(storageDB);

    const db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE issues (id TEXT PRIMARY KEY, title TEXT, _0_version TEXT NOT NULL);
      CREATE TABLE comments (
        id TEXT PRIMARY KEY,
        issueID "TEXT|NOT_NULL",
        upvotes INTEGER,
        _0_version TEXT NOT NULL
      );
      INSERT INTO issues (id, title, _0_version) VALUES ('1', 'one', '123');
      INSERT INTO issues (id, title, _0_version) VALUES ('2', 'two', '123');
      INSERT INTO comments (id, issueID, upvotes, _0_version) VALUES ('10', '1', 5, '123');
      INSERT INTO comments (id, issueID, upvotes, _0_version) VALUES ('20', '2', 7, '123');
    `);
    replicator = fakeReplicator(lc, db);

    newTimer = () => NO_TIME_TIMER;
    shared = new SharedSnapshot(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, shardID),
      () => 200,
      () => newTimer(),
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
    dbFile.delete();
  });

  function newDriver(
    id: string,
    snapshot: Snapshotter | SharedSnapshot = shared,
  ): PipelineDriver {
    return new PipelineDriver(
      lc,
      testLogConfig,
      snapshot,
      shardID,
      storage.createClientGroupStorage(id),
      id,
      new InspectorDelegate(undefined),
      () => 200,
    );
  }

  function hydrate(driver: PipelineDriver, queries: Queries): RowChange[] {
    const rows: RowChange[] = [];
    for (const [id, ast] of Object.entries(queries)) {
      for (const change of driver.addQuery(
        `${id}-hash`,
        id,
        ast,
        new TimeSliceTimer(lc).startWithoutYielding(),
      )) {
        if (change !== 'yield') {
          rows.push(change);
        }
      }
    }
    return rows;
  }

  /** A client group on its own snapshot, for comparison. */
  function privateDriver(id: string, queries: Queries) {
    // Its operator storage must not collide with the shared client group's.
    const driver = newDriver(
      `private-${id}`,
      new Snapshotter(lc, dbFile.path, shardID),
    );
    driver.init(clientSchema);
    hydrate(driver, queries);
    return driver;
  }

  function sharedDriver(id: string, queries: Queries) {
    const driver = newDriver(id);
    driver.init(clientSchema);
    hydrate(driver, queries);
    return driver;
  }

  function privateChanges(driver: PipelineDriver): RowChange[] {
    const changes: RowChange[] = [];
    for (const change of driver.advance(NO_TIME_TIMER).changes) {
      if (change !== 'yield') {
        changes.push(change);
      }
    }
    return changes;
  }

  let upstream: Subscription<ReplicaState>;
  beforeEach(() => {
    upstream = Subscription.create<ReplicaState>();
    void shared.relay(upstream);
  });

  /**
   * Signals a replica change, as the Notifier would, once the shared snapshot
   * has received it.
   */
  async function notify() {
    await upstream.push({state: 'version-ready'}).result;
  }

  test('advances client groups together, writing each change once', async () => {
    const groups: Record<string, Queries> = {
      a: {q1: ISSUES_AND_COMMENTS, q2: TOP_COMMENT},
      b: {q1: ISSUES_AND_COMMENTS},
      c: {q3: ISSUE_1_COMMENTS, q2: TOP_COMMENT},
    };
    const privates = Object.fromEntries(
      Object.entries(groups).map(([id, qs]) => [id, privateDriver(id, qs)]),
    );
    const shareds = Object.fromEntries(
      Object.entries(groups).map(([id, qs]) => [id, sharedDriver(id, qs)]),
    );

    const transactions = [
      [
        '124',
        messages.insert('comments', {id: '11', issueID: '1', upvotes: 9}),
      ],
      [
        '125',
        messages.update('comments', {id: '10', issueID: '2', upvotes: 1}),
      ],
      [
        '126',
        messages.delete('comments', {id: '20'}),
        messages.insert('issues', {id: '3', title: 'three'}),
      ],
      ['127', messages.update('issues', {id: '1', title: 'uno'})],
    ] as const;

    for (const [version, ...changes] of transactions) {
      replicator.processTransaction(version, ...changes);

      const expected = Object.fromEntries(
        Object.entries(privates).map(([id, d]) => [
          id,
          sorted(privateChanges(d)),
        ]),
      );

      const genPush = vi.spyOn(TableSource.prototype, 'genPush');
      await notify();
      const results = await Promise.all(
        Object.values(shareds).map(d => d.advanceShared()),
      );
      // Each change is pushed (and written) once per worker, not once per
      // client group.
      expect(genPush.mock.calls.length).toBeLessThanOrEqual(changes.length * 2);
      genPush.mockRestore();

      expect(
        Object.fromEntries(
          Object.keys(shareds).map((id, i) => [id, sorted(results[i].changes)]),
        ),
      ).toEqual(expected);
      for (const result of results) {
        expect(result.version).toBe(version);
      }
      for (const [id, d] of Object.entries(shareds)) {
        expect(d.currentVersion()).toBe(version);
        for (const queryID of Object.keys(groups[id])) {
          expect(d.rowSetSignature(queryID)).toBe(
            privates[id].rowSetSignature(queryID),
          );
        }
      }
    }
  });

  test('client groups share one source per table', () => {
    const connect = vi.spyOn(TableSource.prototype, 'connect');
    sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    sharedDriver('b', {q1: ISSUES_AND_COMMENTS});
    const sources = new Set(connect.mock.contexts);
    expect(sources.size).toBe(2); // issues and comments
    expect(connect).toHaveBeenCalledTimes(4);
  });

  test('a round waits for every registered client group', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const b = sharedDriver('b', {q2: TOP_COMMENT});

    replicator.processTransaction(
      '124',
      messages.insert('comments', {id: '11', issueID: '1', upvotes: 9}),
    );
    await notify();
    let aDone = false;
    const aResult = a.advanceShared().then(r => {
      aDone = true;
      return r;
    });
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(aDone).toBe(false);
    // b can still hydrate while the round is gathering.
    expect(hydrate(b, {q3: ISSUE_1_COMMENTS})).toHaveLength(1);

    const bResult = await b.advanceShared();
    expect((await aResult).changes).toHaveLength(1);
    // Sorted by query, table, row key and type.
    expect(sorted(bResult.changes)).toEqual([
      {
        type: 0,
        queryID: 'q2',
        table: 'comments',
        rowKey: {id: '11'},
        row: {id: '11', issueID: '1', upvotes: 9, _0_version: '124'},
      },
      {
        type: 1,
        queryID: 'q2',
        table: 'comments',
        rowKey: {id: '20'},
        row: undefined,
      },
      {
        type: 0,
        queryID: 'q3',
        table: 'comments',
        rowKey: {id: '11'},
        row: {id: '11', issueID: '1', upvotes: 9, _0_version: '124'},
      },
    ]);
  });

  test('a client group that parks with no round pending is released', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    sharedDriver('b', {q2: TOP_COMMENT});
    expect(await a.advanceShared()).toEqual({
      version: '123',
      numChanges: 0,
      changes: [],
    });
  });

  test('a destroyed client group no longer holds up rounds', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const b = sharedDriver('b', {q2: TOP_COMMENT});
    replicator.processTransaction(
      '124',
      messages.update('issues', {id: '1', title: 'uno'}),
    );
    await notify();
    const aResult = a.advanceShared();
    b.destroy();
    expect((await aResult).changes).toMatchObject([
      {type: 2, queryID: 'q1', rowKey: {id: '1'}},
    ]);
  });

  test('a failed pipeline resets only its client group', async () => {
    const a = sharedDriver('a', {q1: ISSUES_WITH_COMMENT_10});
    const b = sharedDriver('b', {q2: ISSUE_1_COMMENTS});

    // Moves comment 10 to issue 2, which changes a's scalar subquery.
    replicator.processTransaction(
      '124',
      messages.update('comments', {id: '10', issueID: '2', upvotes: 5}),
    );
    await notify();
    const [aResult, bResult] = await Promise.allSettled([
      a.advanceShared(),
      b.advanceShared(),
    ]);
    expect(aResult.status).toBe('rejected');
    expect((aResult as PromiseRejectedResult).reason).toBeInstanceOf(
      ResetPipelinesSignal,
    );
    expect((aResult as PromiseRejectedResult).reason.reason).toBe(
      'scalar-subquery',
    );
    expect(bResult).toMatchObject({
      status: 'fulfilled',
      value: {
        version: '124',
        changes: [{type: 1, queryID: 'q2', rowKey: {id: '10'}}],
      },
    });

    // a rehydrates at the new snapshot, and both carry on.
    a.reset(clientSchema);
    expect(a.advanceWithoutDiff()).toBe('124');
    expect(hydrate(a, {q1: ISSUES_WITH_COMMENT_10})).toMatchObject([
      {queryID: 'q1', rowKey: {id: '2'}},
      {queryID: 'q1', rowKey: {id: '10'}},
    ]);

    replicator.processTransaction(
      '125',
      messages.update('issues', {id: '2', title: 'dos'}),
    );
    await notify();
    const [a2, b2] = await Promise.all([a.advanceShared(), b.advanceShared()]);
    expect(a2.changes).toMatchObject([
      {type: 2, queryID: 'q1', rowKey: {id: '2'}},
    ]);
    expect(b2.changes).toEqual([]);
  });

  test('a truncation resets every client group', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const b = sharedDriver('b', {q2: TOP_COMMENT});
    const epoch = shared.specsEpoch;

    replicator.processTransaction('124', messages.truncate('comments'));
    await notify();
    const results = await Promise.allSettled([
      a.advanceShared(),
      b.advanceShared(),
    ]);
    for (const result of results) {
      expect(result.status).toBe('rejected');
      expect((result as PromiseRejectedResult).reason.reason).toBe(
        'truncation',
      );
    }
    expect(shared.specsEpoch).toBe(epoch + 1);

    for (const d of [a, b]) {
      d.reset(clientSchema);
      expect(d.advanceWithoutDiff()).toBe('124');
    }
    expect(hydrate(a, {q1: ISSUES_AND_COMMENTS})).toHaveLength(2);
    expect(hydrate(b, {q2: TOP_COMMENT})).toHaveLength(0);
  });

  test('a client group that hydrated before a reset of the specs must reset', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const b = newDriver('b');
    b.init(clientSchema);

    replicator.processTransaction('124', messages.truncate('comments'));
    await notify();
    await Promise.allSettled([a.advanceShared(), b.advanceShared()]);

    expect(() => b.advanceWithoutDiff()).toThrow(ResetPipelinesSignal);
    b.reset(clientSchema);
    expect(b.advanceWithoutDiff()).toBe('124');
  });

  test('moves to head without a diff when nothing reads the changes', async () => {
    const a = newDriver('a');
    a.init(clientSchema);
    const advance = vi.spyOn(Snapshotter.prototype, 'advance');

    replicator.processTransaction(
      '124',
      messages.update('issues', {id: '1', title: 'uno'}),
    );
    await notify();
    expect(await a.advanceShared()).toEqual({
      version: '124',
      numChanges: 0,
      changes: [],
    });
    expect(advance).not.toHaveBeenCalled();
    expect(hydrate(a, {q1: ISSUES_AND_COMMENTS})[0]).toMatchObject({
      row: {id: '1', title: 'uno', _0_version: '124'},
    });
  });

  test('a client group that registers during a diff joins after it', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const started = resolver<void>();
    const release = resolver<void>();
    newTimer = () => ({
      ...NO_TIME_TIMER,
      start: () => {
        started.resolve();
        return release.promise;
      },
    });

    replicator.processTransaction(
      '124',
      messages.update('issues', {id: '1', title: 'uno'}),
    );
    await notify();
    const aResult = a.advanceShared();
    await started.promise;

    // The round is diffing, so b is released once it is done.
    const b = newDriver('b');
    b.init(clientSchema);
    let bDone = false;
    const bResult = b.advanceShared().then(r => {
      bDone = true;
      return r;
    });
    await new Promise(resolve => setTimeout(resolve, 10));
    expect(bDone).toBe(false);

    release.resolve();
    expect((await aResult).changes).toHaveLength(1);
    expect(await bResult).toEqual({version: '124', numChanges: 0, changes: []});
    expect(hydrate(b, {q1: ISSUES_AND_COMMENTS})[0]).toMatchObject({
      row: {id: '1', title: 'uno', _0_version: '124'},
    });
  });

  test('relays one notification per round to client groups', async () => {
    const a = sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const b = sharedDriver('b', {q2: TOP_COMMENT});
    const aNotifications = shared.subscribe();
    const bNotifications = shared.subscribe();
    const aIt = aNotifications[Symbol.asyncIterator]();
    const bIt = bNotifications[Symbol.asyncIterator]();

    replicator.processTransaction(
      '124',
      messages.update('issues', {id: '1', title: 'uno'}),
    );
    await notify();
    await aIt.next();
    const aResult = a.advanceShared();

    // Notifications during a round are coalesced into the next round.
    await notify();
    await notify();

    await bIt.next();
    const bResult = b.advanceShared();
    expect(await aResult).toMatchObject({
      version: '124',
      changes: [{type: 2, queryID: 'q1', rowKey: {id: '1'}}],
    });
    expect(await bResult).toMatchObject({version: '124', changes: []});

    // The next round, which advances to the head of the replica.
    await Promise.all([aIt.next(), bIt.next()]);
    replicator.processTransaction(
      '125',
      messages.update('issues', {id: '2', title: 'dos'}),
    );
    const [a2, b2] = await Promise.all([a.advanceShared(), b.advanceShared()]);
    expect(a2).toMatchObject({
      version: '125',
      changes: [{type: 2, queryID: 'q1', rowKey: {id: '2'}}],
    });
    expect(b2).toMatchObject({version: '125', changes: []});

    // And no more.
    expect(aNotifications.queued).toBe(0);
    expect(bNotifications.queued).toBe(0);
    expect(await a.advanceShared()).toEqual({
      version: '125',
      numChanges: 0,
      changes: [],
    });

    aNotifications.cancel();
    bNotifications.cancel();
  });

  test('client groups must agree on primary keys', () => {
    dbFile
      .connect(lc)
      .exec('CREATE UNIQUE INDEX comments_issue_id ON comments (issueID, id)');
    sharedDriver('a', {q1: ISSUES_AND_COMMENTS});
    const b = newDriver('b');
    b.init(
      createSchema({
        tables: [
          issues,
          table('comments')
            .columns({id: string(), issueID: string(), upvotes: number()})
            .primaryKey('issueID', 'id'),
        ],
      }),
    );
    expect(() => hydrate(b, {q2: TOP_COMMENT})).toThrow(
      /must agree on its primary key/,
    );
  });
});
