import {getOrCreateCounter} from '../../observability/metrics.ts';

/**
 * Counters for the logical backup archive. Gauges over live writer state
 * (durable cursor lag, buffered bytes) are registered by the server wiring,
 * which observes {@link ../archive/archive-writer.ts ArchiveWriter} state.
 */

export function backupArchiveSegmentsUploaded() {
  return getOrCreateCounter(
    'replica',
    'backup_archive.segments_uploaded',
    'Sealed archive log segments made durable, by result: uploaded, or ' +
      'already present from a previous incarnation (an idempotent retry).',
  );
}

export function backupArchiveUploadErrors() {
  return getOrCreateCounter(
    'replica',
    'backup_archive.upload_errors',
    'Failed archive segment upload attempts. In archive mode the durable ' +
      'cursor stops advancing while these occur, which stalls upstream ACKs.',
  );
}

export function backupArchiveGaps() {
  return getOrCreateCounter(
    'replica',
    'backup_archive.gaps',
    'Discontinuities observed in the archived cursor range: a non-contiguous ' +
      'segment listing at reconcile time, or a stream resume point past the ' +
      'durable archive head. Should be zero; nonzero means the archive ' +
      'cannot serve a restore across the gap.',
  );
}
