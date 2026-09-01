import {spawn} from 'node:child_process';
import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {describe, expect, test} from 'vitest';

/**
 * M1's acceptance test and the permanent regression guard for the archive's
 * streaming discipline: the drill subprocess archives, replays, and restores
 * one synthetic transaction far larger than its V8 heap. See
 * `memory-ceiling-drill.ts` for what runs inside.
 */

const DRILL = fileURLToPath(
  new URL('./memory-ceiling-drill.ts', import.meta.url),
);

// A ~1 GiB transaction under a 192 MB old-space cap: any stage that
// regresses to O(transaction) or O(segment) memory dies with an OOM crash
// long before the payload fits. (The payload is compressible by design —
// compressibility affects only speed, never residency.)
const ROWS = 256;
const ROW_BYTES = 4 * 1024 * 1024;
const HEAP_MB = 192;

describe('backup/memory-ceiling', () => {
  test(
    'archives, replays, and restores a transaction far larger than the heap',
    {timeout: 10 * 60 * 1000},
    async () => {
      const workDir = mkdtempSync(join(tmpdir(), 'zero-memory-ceiling-'));
      try {
        const {code, output} = await run(process.execPath, [
          `--max-old-space-size=${HEAP_MB}`,
          DRILL,
          workDir,
          String(ROWS),
          String(ROW_BYTES),
        ]);
        expect(code, output).toBe(0);
        expect(output).toContain('MEMORY-CEILING-DRILL OK');
      } finally {
        rmSync(workDir, {recursive: true, force: true});
      }
    },
  );
});

function run(
  command: string,
  args: string[],
): Promise<{code: number | null; output: string}> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {stdio: ['ignore', 'pipe', 'pipe']});
    let output = '';
    child.stdout.on('data', chunk => (output += chunk));
    child.stderr.on('data', chunk => (output += chunk));
    child.on('error', reject);
    child.on('close', code => resolve({code, output}));
  });
}
