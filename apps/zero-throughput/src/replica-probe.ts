import {createSilentLogContext} from '../../../packages/shared/src/logging-test-utils.ts';
import {Database} from '../../../packages/zqlite/src/db.ts';

/**
 * One read-only look at a serving replica: how far it has been advanced, and
 * how old the newest row it carries is.
 *
 * This is the measurement seam for a view-syncer running no pipelines. With
 * no client connected there is no query, no IVM pipeline and no poke to
 * observe, so the only thing that moves is the replica file itself -- which
 * is exactly the boundary "end to end from Postgres to a view-syncer" names.
 * The replica is opened read-only on a separate connection; serving replicas
 * are WAL-journaled (`getPragmaConfig('serving')` leaves the journal alone),
 * so this never blocks the replicator.
 */
export type ReplicaSample = {
  readonly atMs: number;
  /** `_zero.replicationState.stateVersion`: the applied commit watermark. */
  readonly stateVersion: string;
  /** When the replicator committed that watermark, on the local clock. */
  readonly writeTimeMs: number;
  /** The highest workload `seq` present in the replica, 0 when empty. */
  readonly maxSeq: number;
  /**
   * `clock_timestamp()` of the newest applied row, i.e. the upstream commit
   * side of the same hop. `now - writtenAtMs` is the end-to-end latency of
   * the newest row the view-syncer holds.
   */
  readonly writtenAtMs: number;
  /** How long this sample's queries took, in ms (the probe's own overhead). */
  readonly probeMs: number;
};

export class ReplicaProbe {
  readonly #db: Database;
  readonly #state;
  readonly #progress;

  constructor(replicaFile: string, table: string) {
    this.#db = new Database(createSilentLogContext(), replicaFile, {
      readonly: true,
    });
    this.#state = this.#db.prepare(
      `SELECT "stateVersion", "writeTimeMs" FROM "_zero.replicationState"`,
    );
    // Both aggregates resolve through the workload's unique index on `seq`
    // (see EXPLAIN: SEARCH ... USING COVERING INDEX), so the probe stays
    // O(log n) as the table grows and does not perturb what it measures.
    this.#progress = this.#db.prepare(
      `SELECT max("seq") AS "maxSeq",
              (SELECT "written_at" FROM ${quote(table)}
                 ORDER BY "seq" DESC LIMIT 1) AS "writtenAtMs"
         FROM ${quote(table)}`,
    );
  }

  sample(): ReplicaSample {
    const start = performance.now();
    const [state] = this.#state.all<{
      stateVersion: string;
      writeTimeMs: number;
    }>();
    const [progress] = this.#progress.all<{
      maxSeq: number | null;
      writtenAtMs: number | null;
    }>();
    return {
      atMs: Date.now(),
      stateVersion: state?.stateVersion ?? '',
      writeTimeMs: state?.writeTimeMs ?? 0,
      maxSeq: Number(progress?.maxSeq ?? 0),
      writtenAtMs: Number(progress?.writtenAtMs ?? 0),
      probeMs: performance.now() - start,
    };
  }

  close(): void {
    this.#db.close();
  }
}

function quote(ident: string): string {
  return `"${ident.replaceAll('"', '""')}"`;
}
