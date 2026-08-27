/**
 * Re-encodes a chunk with `relation` blocks interned.
 *
 * Every data message in the change stream carries a full copy of its table's
 * relation block (schema, name, row key, and the deprecated keyColumns /
 * replicaIdentity fields). Interning replaces each with a small integer
 * reference, with the definitions declared once at the head of the chunk.
 *
 * This answers a practical question: since general-purpose compressors already
 * collapse repeated bytes, is there anything left to gain from fixing the
 * format itself?
 */
export function internRelations(raw: Buffer): Buffer {
  const lines = raw.toString('utf8').split('\n');
  const ids = new Map<string, number>();
  const defs: string[] = [];
  const out: string[] = [];

  for (const line of lines) {
    if (!line) continue;
    const start = line.indexOf('"relation":');
    if (start < 0) {
      out.push(line);
      continue;
    }
    const objStart = line.indexOf('{', start);
    const end = matchBrace(line, objStart);
    if (end < 0) {
      out.push(line);
      continue;
    }
    const rel = line.slice(objStart, end + 1);
    let id = ids.get(rel);
    if (id === undefined) {
      id = ids.size;
      ids.set(rel, id);
      defs.push(`["rel",${id},${rel}]`);
    }
    out.push(`${line.slice(0, start)}"r":${id}${line.slice(end + 1)}`);
  }
  return Buffer.from([...defs, ...out].join('\n') + '\n', 'utf8');
}

function matchBrace(s: string, open: number): number {
  let depth = 0;
  let inStr = false;
  for (let i = open; i < s.length; i++) {
    const c = s[i];
    if (inStr) {
      if (c === '\\') i++;
      else if (c === '"') inStr = false;
    } else if (c === '"') inStr = true;
    else if (c === '{') depth++;
    else if (c === '}' && --depth === 0) return i;
  }
  return -1;
}
