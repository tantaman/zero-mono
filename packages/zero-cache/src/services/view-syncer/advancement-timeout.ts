import {ResetPipelinesSignal} from './snapshotter.ts';

/**
 * No matter how fast hydration is, advancement is given at least this long to
 * complete before doing a pipeline reset.
 */
const MIN_ADVANCEMENT_TIME_LIMIT_MS = 50;
const MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES = 8;
const PROJECTED_ADVANCEMENT_SAMPLE_FRACTION = 0.25;
const MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES = 50;
const MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS = 5;
const MIN_PROJECTED_ADVANCEMENT_CHANGES = 16;
const PROJECTED_ADVANCEMENT_RESET_MULTIPLIER = 1.5;
const LATE_ADVANCEMENT_FINISH_PROGRESS = 0.8;

/** Where an advancement is, for deciding whether to abandon it. */
export type AdvancementProgress = {
  /** The processing time of the advancement so far. */
  readonly elapsedMs: number;
  /** The processing time of the change being pushed, if one is. */
  readonly currentChangeElapsedMs: number | undefined;
  /** The number of changes processed. */
  readonly pos: number;
  /** The number of changes in the advancement. */
  readonly numChanges: number;
  /**
   * The time it took to hydrate the pipelines being advanced, which is what
   * resetting them instead would cost.
   */
  readonly totalHydrationTimeMs: number;
};

/**
 * Returns the signal to abandon an advancement with, if it is taking longer
 * than rehydrating its pipelines would: when the whole batch projects to be
 * more expensive than hydration, or the current source change alone exceeds
 * the hydration budget. The late-finish exception only applies to batch-level
 * checks; a single pathological push always resets.
 */
export function advancementTimeout({
  elapsedMs: elapsed,
  currentChangeElapsedMs,
  pos,
  numChanges,
  totalHydrationTimeMs,
}: AdvancementProgress): ResetPipelinesSignal | undefined {
  if (
    currentChangeElapsedMs !== undefined &&
    shouldResetSlowCurrentChange(currentChangeElapsedMs, totalHydrationTimeMs)
  ) {
    return new ResetPipelinesSignal(
      `Advancement exceeded timeout processing current change at ${pos} of ` +
        `${numChanges} changes after ${currentChangeElapsedMs} ms ` +
        `(${elapsed} ms total). Advancement time limited based on total ` +
        `hydration time of ${totalHydrationTimeMs} ms.`,
      'advancement-timeout',
    );
  }
  const projectedTotalTimeMs = projectedAdvancementTimeMs(
    elapsed,
    pos,
    numChanges,
  );
  const shouldFinish = shouldFinishLateAdvancement(pos, numChanges);
  if (
    !shouldFinish &&
    shouldResetProjectedAdvancement(
      elapsed,
      projectedTotalTimeMs,
      pos,
      numChanges,
      totalHydrationTimeMs,
    )
  ) {
    const projection =
      projectedTotalTimeMs === undefined
        ? ''
        : ` Projected total advancement time is ${projectedTotalTimeMs} ms.`;
    return new ResetPipelinesSignal(
      `Advancement projected to exceed hydration time at ${pos} of ` +
        `${numChanges} changes after ${elapsed} ms.` +
        projection +
        ` Advancement time limited based on total hydration time of ` +
        `${totalHydrationTimeMs} ms.`,
      'advancement-timeout',
    );
  }
  if (
    !shouldFinish &&
    elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
    (elapsed > totalHydrationTimeMs ||
      (elapsed > totalHydrationTimeMs / 2 && pos <= numChanges / 2))
  ) {
    return new ResetPipelinesSignal(
      `Advancement exceeded timeout at ${pos} of ${numChanges} changes ` +
        `after ${elapsed} ms. Advancement time limited based on total ` +
        `hydration time of ${totalHydrationTimeMs} ms.`,
      'advancement-timeout',
    );
  }
  return undefined;
}

function projectedAdvancementTimeMs(
  elapsedMs: number,
  processedChanges: number,
  numChanges: number,
): number | undefined {
  if (processedChanges <= 0 || numChanges <= 0) {
    return undefined;
  }
  return (elapsedMs / processedChanges) * numChanges;
}

function advancementResetTimeLimitMs(totalHydrationTimeMs: number): number {
  return Math.max(totalHydrationTimeMs, 1);
}

function minProjectedAdvancementSampleChanges(numChanges: number): number {
  return Math.max(
    MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES,
    Math.min(
      MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES,
      Math.ceil(numChanges * PROJECTED_ADVANCEMENT_SAMPLE_FRACTION),
    ),
  );
}

function shouldResetProjectedAdvancement(
  elapsedMs: number,
  projectedTotalTimeMs: number | undefined,
  processedChanges: number,
  numChanges: number,
  totalHydrationTimeMs: number,
): boolean {
  if (
    projectedTotalTimeMs === undefined ||
    numChanges < MIN_PROJECTED_ADVANCEMENT_CHANGES ||
    processedChanges < minProjectedAdvancementSampleChanges(numChanges) ||
    elapsedMs < MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS
  ) {
    return false;
  }

  return (
    projectedTotalTimeMs >
    advancementResetTimeLimitMs(totalHydrationTimeMs) *
      PROJECTED_ADVANCEMENT_RESET_MULTIPLIER
  );
}

function shouldFinishLateAdvancement(
  processedChanges: number,
  numChanges: number,
): boolean {
  return (
    numChanges > 0 &&
    processedChanges / numChanges >= LATE_ADVANCEMENT_FINISH_PROGRESS
  );
}

function shouldResetSlowCurrentChange(
  currentChangeElapsedMs: number,
  totalHydrationTimeMs: number,
): boolean {
  return (
    currentChangeElapsedMs > MIN_ADVANCEMENT_TIME_LIMIT_MS &&
    currentChangeElapsedMs > advancementResetTimeLimitMs(totalHydrationTimeMs)
  );
}
