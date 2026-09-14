/**
 * Regression tests for what happens when a custom mutator throws while it is
 * being *rebased* (replayed on top of a new server snapshot) rather than when
 * it first runs optimistically.
 *
 * Rebase happens in three places, all of which call
 * `rebaseMutation` in replicache/src/db/rebase.ts, which simply awaits the
 * mutator implementation and lets anything it throws escape:
 *
 *  - `ReplicacheImpl.maybeEndPull` (poke / pull processing)
 *  - `persist` (moving memdag mutations into the perdag client group)
 *  - `refresh` (pulling another tab's perdag state into this tab's memdag)
 *
 * None of these treat an exception from user code as recoverable, so a single
 * throwing rebase takes the whole client down.
 */
import {afterEach, describe, expect, test, vi} from 'vitest';
import {sleep} from '../../../shared/src/sleep.ts';
import type {InsertValue} from '../../../zql/src/mutate/crud.ts';
import type {Transaction} from '../../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';
import {legacySchema} from '../../../zql/src/query/test/test-schemas.ts';
import {ConnectionStatus} from './connection-status.ts';
import {MockSocket, zeroForTest} from './test-utils.ts';

type Schema = typeof legacySchema;
type MutatorTx = Transaction<Schema>;

afterEach(() => vi.restoreAllMocks());

const zql = createBuilder(legacySchema);

const issue1 = {
  id: '1',
  title: 'foo',
  closed: false,
  description: '',
  ownerId: '',
  createdAt: 1,
} as const;

/**
 * Creates a Zero whose `issue.create` mutator throws when it is rebased and
 * `throwOnRebase` is set, and runs normally otherwise.
 */
function zeroWithFailingRebase() {
  vi.stubGlobal('WebSocket', MockSocket as unknown as typeof WebSocket);
  vi.stubGlobal('fetch', () => Promise.resolve(new Response()));

  const state = {throwOnRebase: false, reasons: [] as string[]};
  const z = zeroForTest({
    schema: legacySchema,
    mutators: {
      issue: {
        create: async (
          tx: MutatorTx,
          args: InsertValue<typeof legacySchema.tables.issue>,
        ) => {
          state.reasons.push(tx.reason);
          if (tx.reason === 'rebase' && state.throwOnRebase) {
            throw new Error('boom during rebase');
          }
          await tx.mutate.issue.insert(args);
        },
      },
    } as const,
  });
  return {z, state};
}

describe('a mutator that throws while being rebased', () => {
  test('kills the connection and stops all syncing and mutating', async () => {
    const {z, state} = zeroWithFailingRebase();
    const q = zql.issue;

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    await z.mutate.issue.create(issue1).client;
    expect((await z.run(q)).map(r => r.id)).toEqual(['1']);

    // From here on the mutator blows up when it is replayed.
    state.throwOnRebase = true;
    state.reasons.length = 0;

    // A poke that advances the cookie but does *not* ack mutation 1 forces
    // mutation 1 to be rebased on top of the new snapshot.
    await z.triggerPoke({
      rowsPatch: [
        {
          op: 'put',
          tableName: 'issues',
          value: {
            id: '2',
            title: 'server-row',
            closed: false,
            description: '',
            ownerId: '',
            createdAt: 2,
          },
        },
      ],
    });
    await sleep(100);

    // The mutator was replayed and threw.
    expect(state.reasons).toContain('rebase');

    // The exception escapes rep.poke(), PokeHandler reports it via
    // onPokeError, and Zero disconnects with ClientErrorKind.Internal, which
    // maps to the *terminal* `error` connection status: the run loop parks
    // until the app calls zero.connection.connect() by hand.
    expect(z.connectionStatus).toEqual(ConnectionStatus.Error);

    // Everything the poke carried is thrown away - the server row never
    // lands, and the local optimistic write is left in place but can never be
    // confirmed.
    expect((await z.run(q)).map(r => r.id)).toEqual(['1']);

    // Worse: because the connection is in a terminal state, MutatorProxy now
    // rejects every new mutation without even running it, so the app can no
    // longer write anything.
    state.reasons.length = 0;
    const after = z.mutate.issue.create({...issue1, id: '3'});
    expect(await after.client).toMatchObject({type: 'error'});
    expect(state.reasons).toEqual([]);
    expect((await z.run(q)).map(r => r.id)).toEqual(['1']);

    await z.close();
  });

  test('reconnecting just hits the same failure again', async () => {
    const {z, state} = zeroWithFailingRebase();

    // Drive the pokes by hand so we control the cookies: the failed poke never
    // lands, so the client's base cookie stays null and the retry has to reuse
    // it.
    const poke = async (pokeID: string, cookie: string) => {
      await z.triggerPokeStart({pokeID, baseCookie: null});
      await z.triggerPokeChunks([{pokeID, gotQueriesPatch: []}]);
      await z.triggerPokeEnd({pokeID, cookie});
      await sleep(100);
    };

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    await z.mutate.issue.create(issue1).client;

    state.throwOnRebase = true;
    state.reasons.length = 0;
    await poke('p1', '0000000001');
    expect(state.reasons).toContain('rebase');
    expect(z.connectionStatus).toEqual(ConnectionStatus.Error);

    // The documented recovery is to reconnect. The mutation is still pending
    // and still unreplayable, so the very next poke fails the same way.
    state.reasons.length = 0;
    await z.connection.connect();
    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);

    await poke('p2', '0000000002');

    expect(state.reasons).toContain('rebase');
    expect(z.connectionStatus).toEqual(ConnectionStatus.Error);

    await z.close();
  });

  test('breaks persist, so the mutation never reaches IndexedDB', async () => {
    const {z, state} = zeroWithFailingRebase();

    await z.triggerConnected();
    await z.waitForConnectionStatus(ConnectionStatus.Connected);
    await z.mutate.issue.create(issue1).client;

    state.throwOnRebase = true;

    // persist() rebases the memdag's local mutations onto the perdag client
    // group, so it throws too. On the scheduled path this rejection is only
    // logged (ReplicacheImpl.#schedule swallows it), which means persistence
    // silently stops making progress for as long as the mutation is pending.
    await expect(z.persist()).rejects.toThrow('boom during rebase');

    // The connection is untouched by this one - the failure is invisible to
    // the app.
    expect(z.connectionStatus).toEqual(ConnectionStatus.Connected);
    expect((await z.run(zql.issue)).map(r => r.id)).toEqual(['1']);

    await z.close();
  });
});
