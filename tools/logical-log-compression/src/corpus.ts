import {execFileSync} from 'node:child_process';
import {randomBytes} from 'node:crypto';
/**
 * A corpus of real developer text, used to fill text-heavy columns with
 * non-repeating values.
 *
 * Cycling a small seed dataset (e.g. zbugs' 924 real comments) to fill a 16MiB
 * chunk would make the chunk look far more compressible than production
 * traffic, where each comment body is distinct. This module supplies distinct
 * slices of real text (the repo's own TypeScript and Markdown, plus the real
 * GitHub-derived zbugs bodies) so that measured ratios reflect genuinely novel
 * payloads.
 */
import {readFileSync} from 'node:fs';

const REPO = '/home/user/zero-mono';

function repoFiles(pattern: string, limitBytes: number): string[] {
  const out = execFileSync(
    'bash',
    [
      '-c',
      `cd ${REPO} && find packages apps -name '${pattern}' -not -path '*/node_modules/*' | head -4000`,
    ],
    {encoding: 'utf8', maxBuffer: 1 << 28},
  );
  const texts: string[] = [];
  let total = 0;
  for (const f of out.split('\n')) {
    if (!f) continue;
    try {
      const t = readFileSync(`${REPO}/${f}`, 'utf8');
      texts.push(t);
      total += t.length;
      if (total >= limitBytes) break;
    } catch {
      // ignore unreadable files
    }
  }
  return texts;
}

const PARAGRAPH_SPLIT = /\n\s*\n/;

let paragraphs: string[] | undefined;

/**
 * Paragraph-sized slices of real developer text (prose, markdown and code),
 * which is what issue descriptions and comment bodies actually look like.
 */
export function textCorpus(): string[] {
  if (paragraphs) {
    return paragraphs;
  }
  const raw = [
    ...repoFiles('*.md', 4 << 20),
    ...repoFiles('*.ts', 24 << 20),
  ].join('\n\n');

  // Split on blank lines into paragraph-ish units.
  paragraphs = raw
    .split(PARAGRAPH_SPLIT)
    .map(p => p.trim())
    .filter(p => p.length >= 40);
  return paragraphs;
}

/**
 * Hands out non-repeating text of approximately `targetLen` characters,
 * walking the corpus linearly so no two values share content.
 */
export class TextSource {
  readonly #paras: string[];
  #i = 0;

  constructor(paras = textCorpus()) {
    this.#paras = paras;
  }

  next(targetLen: number): string {
    const parts: string[] = [];
    let len = 0;
    while (len < targetLen) {
      const p = this.#paras[this.#i++ % this.#paras.length];
      parts.push(p);
      len += p.length + 2;
    }
    return parts.join('\n\n').slice(0, targetLen);
  }

  get exhaustedAfter(): number {
    return this.#paras.reduce((n, p) => n + p.length, 0);
  }
}

/**
 * Incompressible text of the requested length, for the pessimistic bound:
 * a payload with no exploitable redundancy at all (e.g. ciphertext, or
 * base64 blobs).
 */
export function randomText(len: number): string {
  return randomBytes(Math.ceil((len * 3) / 4))
    .toString('base64')
    .slice(0, len);
}
