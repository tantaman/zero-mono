import * as v from '../../../../../shared/src/valita.ts';

/**
 * The publication protocol of a SQLite base: `intent.json` is written first,
 * then the chunks, and `complete.json` strictly last — a base exists if and
 * only if its complete manifest does, so a crash at any earlier boundary
 * leaves debris that is invisible to restore and reclaimable by GC.
 */

export const BASE_FORMAT = 'zero-archive-base';
export const BASE_FORMAT_VERSION = 1;

const baseIntentSchema = v.object({
  format: v.literal(BASE_FORMAT),
  version: v.number(),
  replicaVersion: v.string(),
  /** The watermark embedded in the frozen SQLite file. */
  cursor: v.string(),
  startedAt: v.number(),
});

export type BaseIntent = v.Infer<typeof baseIntentSchema>;

const baseChunkSchema = v.object({
  size: v.number(),
  /** Hex SHA-256 of the chunk. */
  sha256: v.string(),
});

const baseManifestSchema = v.object({
  format: v.literal(BASE_FORMAT),
  version: v.number(),
  replicaVersion: v.string(),
  /** The watermark embedded in the SQLite file the chunks reassemble. */
  cursor: v.string(),
  fileSize: v.number(),
  /** Hex SHA-256 of the whole file. */
  fileSha256: v.string(),
  /** Chunk size; every chunk but the last is exactly this many bytes. */
  chunkBytes: v.number(),
  /** In offset order: chunk `i` occupies `[i * chunkBytes, ...)`. */
  chunks: v.array(baseChunkSchema),
  completedAt: v.number(),
});

export type BaseManifest = v.Infer<typeof baseManifestSchema>;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

export function encodeBaseIntent(intent: BaseIntent): Uint8Array {
  return utf8Encoder.encode(JSON.stringify(intent));
}

export function decodeBaseIntent(data: Uint8Array): BaseIntent {
  return v.parse(JSON.parse(utf8Decoder.decode(data)), baseIntentSchema);
}

export function encodeBaseManifest(manifest: BaseManifest): Uint8Array {
  return utf8Encoder.encode(JSON.stringify(manifest));
}

export function decodeBaseManifest(data: Uint8Array): BaseManifest {
  return v.parse(JSON.parse(utf8Decoder.decode(data)), baseManifestSchema);
}
