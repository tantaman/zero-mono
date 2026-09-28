import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import type {PokePartBody} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {type PgTest, test} from '../../test/db.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import type {FakeReplicator} from '../replicator/test-utils.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {SharedSnapshot} from './shared-snapshot.ts';
import {Snapshotter} from './snapshotter.ts';
import {
  ALL_ISSUES_QUERY,
  expectNoPokes,
  ISSUES_QUERY,
  messages,
  nextPoke,
  nextPokeParts,
  permissionsAll,
  restartViewSyncer,
  setup,
  SHARD,
  YIELD_THRESHOLD_MS,
} from './view-syncer-test-util.ts';
import {type SyncContext, TimeSliceTimer} from './view-syncer.ts';

type ClientGroup = ReturnType<typeof restartViewSyncer>;

const SYNC_CONTEXT: SyncContext = {
  clientID: 'foo',
  profileID: 'p0000g00000003203',
  wsID: 'ws1',
  baseCookie: null,
  protocolVersion: PROTOCOL_VERSION,
  httpCookie: undefined,
  origin: undefined,
  userID: 'bar',
  auth: undefined,
};

/** The issues that a poke puts, by id, with their titles. */
function issueTitles(parts: PokePartBody[]) {
  const titles: Record<string, unknown> = {};
  for (const {rowsPatch} of parts) {
    for (const patch of rowsPatch ?? []) {
      if (patch.op === 'put' && patch.tableName === 'issues') {
        titles[patch.value.id as string] = patch.value.title;
      }
    }
  }
  return titles;
}

function pokeParts(poke: Downstream[]): PokePartBody[] {
  return poke.flatMap(msg => (msg[0] === 'pokePart' ? [msg[1]] : []));
}

describe('view-syncer/shared-snapshot', () => {
  let replicator: FakeReplicator;
  let shared: SharedSnapshot;
  let upstream: Subscription<ReplicaState>;
  let newClientGroup: (id: string) => ClientGroup;

  beforeEach<PgTest>(async ({testDBs}) => {
    const {
      vs,
      viewSyncerDone,
      replicaDbFile,
      cvrDB,
      upstreamDb,
      databaseStorage,
      config,
      customQueryTransformer,
      setTimeoutFn,
      replicator: r,
    } = await setup(
      testDBs,
      'view_syncer_shared_snapshot_test',
      permissionsAll,
    );
    replicator = r;
    // Only the client groups created with newClientGroup() are used.
    await vs.stop();
    await viewSyncerDone;

    const lc = createSilentLogContext();
    shared = new SharedSnapshot(
      lc,
      testLogConfig,
      new Snapshotter(lc, replicaDbFile.path, SHARD),
      () => YIELD_THRESHOLD_MS,
      () => new TimeSliceTimer(lc),
    );
    upstream = Subscription.create();
    void shared.relay(upstream);

    const groups: ClientGroup[] = [];
    newClientGroup = (clientGroupID: string) => {
      const group = restartViewSyncer({
        databaseStorage,
        replicaDbFile,
        cvrDB,
        config,
        customQueryTransformer,
        setTimeoutFn,
        clientGroupID,
        sharedSnapshot: shared,
      });
      groups.push(group);
      return group;
    };

    return async () => {
      for (const {vs, viewSyncerDone} of groups) {
        await vs.stop();
        await viewSyncerDone;
      }
      upstream.cancel();
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  /** Signals a replica change, once the shared snapshot has received it. */
  async function notify() {
    await upstream.push({state: 'version-ready'}).result;
  }

  /** Connects a client to the client group and waits for its hydration. */
  async function hydrate(
    group: ClientGroup,
    clientID: string,
    queries: UpQueriesPatch,
  ): Promise<[Queue<Downstream>, PokePartBody[]]> {
    const client = group.connect({...SYNC_CONTEXT, clientID}, queries);
    await nextPoke(client); // desired queries
    return [client, await nextPokeParts(client)];
  }

  function updateIssue1(version: string, title: string) {
    replicator.processTransaction(
      version,
      messages.update('issues', {
        id: '1',
        title,
        owner: 100,
        parent: null,
        big: 9007199254740991n,
      }),
    );
  }

  test('client groups advance together, pushing each change once', async () => {
    const a = newClientGroup('cg-a');
    const b = newClientGroup('cg-b');
    const clientA = a.connect({...SYNC_CONTEXT, clientID: 'a1'}, [
      {op: 'put', hash: 'all-issues', ast: ALL_ISSUES_QUERY},
    ]);
    const clientB = b.connect({...SYNC_CONTEXT, clientID: 'b1'}, [
      {op: 'put', hash: 'issues', ast: ISSUES_QUERY},
    ]);
    await Promise.all([nextPoke(clientA), nextPoke(clientB)]);

    await notify();
    const [hydratedA, hydratedB] = await Promise.all([
      nextPokeParts(clientA),
      nextPokeParts(clientB),
    ]);
    expect(Object.keys(issueTitles(hydratedA))).toEqual([
      '1',
      '2',
      '3',
      '4',
      '5',
    ]);
    expect(Object.keys(issueTitles(hydratedB))).toEqual(['1', '2', '3', '4']);

    updateIssue1('02', 'new title');
    const genPush = vi.spyOn(TableSource.prototype, 'genPush');
    await notify();
    const [pokeA, pokeB] = await Promise.all([
      nextPoke(clientA),
      nextPoke(clientB),
    ]);
    for (const poke of [pokeA, pokeB]) {
      expect(poke.at(-1)).toMatchObject(['pokeEnd', {cookie: '02'}]);
      expect(issueTitles(pokeParts(poke))).toEqual({1: 'new title'});
    }
    // Once for both client groups.
    expect(genPush).toHaveBeenCalledTimes(1);
  });

  test('a round waits for every client group', async () => {
    const a = newClientGroup('cg-a');
    const b = newClientGroup('cg-b');
    await notify();
    const [clientA] = await hydrate(a, 'a1', [
      {op: 'put', hash: 'all-issues', ast: ALL_ISSUES_QUERY},
    ]);
    const [clientB] = await hydrate(b, 'b1', [
      {op: 'put', hash: 'issues', ast: ISSUES_QUERY},
    ]);

    // Holds up the second client group to park for the next round.
    const advanceShared = PipelineDriver.prototype.advanceShared;
    const gate = resolver<void>();
    let parked = 0;
    vi.spyOn(PipelineDriver.prototype, 'advanceShared').mockImplementation(
      async function (this: PipelineDriver) {
        if (++parked === 2) {
          await gate.promise;
        }
        return advanceShared.call(this);
      },
    );

    updateIssue1('02', 'new title');
    await notify();
    await vi.waitFor(() => expect(parked).toBe(2));
    await expectNoPokes(clientA);
    await expectNoPokes(clientB);

    gate.resolve();
    const [pokeA, pokeB] = await Promise.all([
      nextPoke(clientA),
      nextPoke(clientB),
    ]);
    expect(pokeA.at(-1)).toMatchObject(['pokeEnd', {cookie: '02'}]);
    expect(pokeB.at(-1)).toMatchObject(['pokeEnd', {cookie: '02'}]);
  });

  test('a stopped client group no longer holds up rounds', async () => {
    const a = newClientGroup('cg-a');
    const b = newClientGroup('cg-b');
    await notify();
    const [clientA] = await hydrate(a, 'a1', [
      {op: 'put', hash: 'all-issues', ast: ALL_ISSUES_QUERY},
    ]);
    await hydrate(b, 'b1', [{op: 'put', hash: 'issues', ast: ISSUES_QUERY}]);

    await b.vs.stop();
    await b.viewSyncerDone;

    updateIssue1('02', 'new title');
    await notify();
    expect(issueTitles(await nextPokeParts(clientA))).toEqual({
      1: 'new title',
    });
  });

  test('a client group that joins later hydrates at the shared snapshot', async () => {
    const a = newClientGroup('cg-a');
    await notify();
    const [clientA] = await hydrate(a, 'a1', [
      {op: 'put', hash: 'all-issues', ast: ALL_ISSUES_QUERY},
    ]);

    updateIssue1('02', 'new title');
    await notify();
    await nextPoke(clientA);

    // Receives the latest notification on subscribing, so it hydrates without
    // waiting for the next change.
    const c = newClientGroup('cg-c');
    const [clientC, hydratedC] = await hydrate(c, 'c1', [
      {op: 'put', hash: 'issues', ast: ISSUES_QUERY},
    ]);
    expect(issueTitles(hydratedC)).toMatchObject({1: 'new title'});

    updateIssue1('03', 'newer title');
    await notify();
    const [pokeA, pokeC] = await Promise.all([
      nextPokeParts(clientA),
      nextPokeParts(clientC),
    ]);
    expect(issueTitles(pokeA)).toEqual({1: 'newer title'});
    expect(issueTitles(pokeC)).toEqual({1: 'newer title'});
  });
});
