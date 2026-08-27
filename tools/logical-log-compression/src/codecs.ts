/**
 * Compression codecs and timing.
 */
import {
  brotliCompressSync,
  brotliDecompressSync,
  constants as Z,
  gzipSync,
  gunzipSync,
  zstdCompressSync,
  zstdDecompressSync,
} from 'node:zlib';

export type Codec = {
  name: string;
  compress: (b: Buffer) => Buffer;
  decompress: (b: Buffer) => Buffer;
};

const zstd = (level: number, windowLog?: number): Codec => ({
  name: windowLog ? `zstd-${level}-win${windowLog}` : `zstd-${level}`,
  compress: b =>
    zstdCompressSync(b, {
      params: {
        [Z.ZSTD_c_compressionLevel]: level,
        ...(windowLog ? {[Z.ZSTD_c_windowLog]: windowLog} : {}),
      },
    }),
  decompress: b =>
    zstdDecompressSync(b, {params: {[Z.ZSTD_d_windowLogMax]: 27}}),
});

const gzip = (level: number): Codec => ({
  name: `gzip-${level}`,
  compress: b => gzipSync(b, {level}),
  decompress: b => gunzipSync(b),
});

const brotli = (quality: number): Codec => ({
  name: `brotli-${quality}`,
  compress: b =>
    brotliCompressSync(b, {
      params: {
        [Z.BROTLI_PARAM_QUALITY]: quality,
        [Z.BROTLI_PARAM_LGWIN]: 24,
        [Z.BROTLI_PARAM_SIZE_HINT]: b.length,
      },
    }),
  decompress: b => brotliDecompressSync(b),
});

/** The codecs worth considering for 16MiB logical-log chunks. */
export const CODECS: Codec[] = [
  zstd(1),
  zstd(3),
  zstd(3, 24),
  zstd(6),
  zstd(9),
  zstd(12),
  zstd(19),
  gzip(1),
  gzip(6),
  gzip(9),
  brotli(4),
  brotli(9),
  brotli(11),
];

export type CodecResult = {
  codec: string;
  compressedBytes: number;
  ratio: number;
  compressMs: number;
  decompressMs: number;
  compressMiBs: number;
  decompressMiBs: number;
};

/**
 * Reports the best (minimum) time over up to `reps` repetitions, but stops
 * early once `budgetMs` is spent so that the slow codecs (zstd-19, brotli-11)
 * do not dominate the run.
 */
export function benchmark(
  codec: Codec,
  raw: Buffer,
  reps: number,
  budgetMs = 8000,
): CodecResult {
  const t0 = performance.now();
  const warm = codec.compress(raw); // also warms up the binding
  const warmMs = performance.now() - t0;
  codec.decompress(warm);

  let cMs = warmMs;
  let out = warm;
  for (let i = 1; i < reps && (i + 1) * cMs < budgetMs; i++) {
    const t = performance.now();
    out = codec.compress(raw);
    cMs = Math.min(cMs, performance.now() - t);
  }
  let dMs = Infinity;
  for (let i = 0; i < reps; i++) {
    const t = performance.now();
    const back = codec.decompress(out);
    dMs = Math.min(dMs, performance.now() - t);
    if (back.length !== raw.length) {
      throw new Error(`${codec.name}: roundtrip length mismatch`);
    }
  }
  const mib = raw.length / (1 << 20);
  return {
    codec: codec.name,
    compressedBytes: out.length,
    ratio: raw.length / out.length,
    compressMs: cMs,
    decompressMs: dMs,
    compressMiBs: mib / (cMs / 1000),
    decompressMiBs: mib / (dMs / 1000),
  };
}
