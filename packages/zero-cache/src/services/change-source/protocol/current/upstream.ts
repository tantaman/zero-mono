import {jsonValueSchema} from '../../../../../../shared/src/bigint-json.ts';
import * as v from '../../../../../../shared/src/valita.ts';
import {
  backfillIDSchema,
  identifierSchema,
  tableMetadataSchema,
} from './data.ts';
import {upstreamStatusMessageSchema} from './status.ts';

/** At the moment, the only upstream messages are status messages.  */
export const changeSourceUpstreamSchema = upstreamStatusMessageSchema;
export type ChangeSourceUpstream = v.Infer<typeof changeSourceUpstreamSchema>;

/**
 * Contains the information for requesting a backfill of columns in a table.
 * Backfills are automatically started for new tables and columns in a given
 * change stream session; however, if the session is terminated before the
 * backfill completes, it must be restarted with appropriate
 * {@link BackfillRequest}s when creating a new session.
 *
 * The `change-streamer` is responsible for tracking any changes to the table
 * name, column names, or table metadata, and constructing a BackfillRequest
 * based on the current values (which may be different from when the
 * tables/columns were originally added).
 */
export const backfillRequestSchema = v.object({
  table: identifierSchema.extend({
    // The table metadata is set to null if it is never specified by the
    // change-source.
    metadata: tableMetadataSchema.nullable(),
  }),
  columns: v.record(backfillIDSchema),

  /**
   * Resumes an interrupted backfill strictly after the row with this key,
   * with values ordered to match the table's row key columns. Rows are
   * backfilled in row-key order (text-family columns compared bytewise,
   * i.e. `COLLATE "C"`), so the key of the last row durably applied
   * downstream is a valid resumption point. A change-source that cannot
   * resume from the given key values instead restarts the backfill from
   * the beginning, which is always correct since backfills are idempotent.
   */
  resumeAfter: v.array(jsonValueSchema).optional(),
});

export type BackfillRequest = v.Infer<typeof backfillRequestSchema>;
