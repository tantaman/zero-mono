import {readFileSync, readdirSync} from 'node:fs';

/**
 * Per-role CPU accounting, read straight from `/proc`.
 *
 * A throughput number on its own does not say what the ceiling *is*: the
 * same 20k rows/s can mean the gateway is saturated, the applier is
 * saturated, or the load generator ran out of connections. Attributing CPU
 * to the replication-manager's process tree (gateway + archive writer +
 * base producer), the view-syncer's (replicator + syncers), Postgres and
 * the harness itself is what turns the measurement into an answer.
 */
const CLOCK_TICKS_PER_SEC = 100;

/** The worker module in a forked child's command line, e.g. `replicator.ts`. */
const WORKER_MODULE = /([\w-]+)\.ts\0?/;

export type ProcSample = {
  readonly atMs: number;
  /**
   * Cumulative bytes each role has sent to storage since the sampler
   * started (`/proc/<pid>/io` write_bytes, which counts what actually
   * reached the block layer). A path that plateaus with every worker below
   * a core is not CPU-bound, and this is the first place to look.
   */
  readonly writeBytes: Readonly<Record<string, number>>;
  /**
   * Whole-device busy time in ms since the sampler started (`io_ticks` from
   * `/proc/diskstats`). Divided by elapsed time this is the classic
   * "%util": if a path plateaus while this stays well under 1.0, the disk
   * is not what it ran out of.
   */
  readonly diskBusyMs: number;
  /** Cumulative CPU seconds per role since the sampler started. */
  readonly cpuSeconds: Readonly<Record<string, number>>;
  /** Resident set size per role, in MiB, at this instant. */
  readonly rssMiB: Readonly<Record<string, number>>;
  /**
   * The same CPU, split per zero-cache worker within each role
   * (`rm/change-streamer`, `rm/base-producer`, `vs-0/replicator`, ...).
   * A role total says the node is busy; this says which worker is, which
   * is the difference between "the gateway is the ceiling" and "the base
   * producer is".
   */
  readonly workerCpuSeconds: Readonly<Record<string, number>>;
};

export type ProcRole = {
  readonly name: string;
  /** The root pid; its descendants are attributed to the same role. */
  readonly pid: number;
};

export class ProcSampler {
  readonly #roles: readonly ProcRole[];
  readonly #baseline = new Map<string, number>();
  readonly #workerBaseline = new Map<string, number>();
  readonly #writeBaseline = new Map<string, number>();
  #diskBaseline = 0;
  /** pid -> worker label; a pid's command line never changes. */
  readonly #workerLabels = new Map<number, string>();

  constructor(roles: readonly ProcRole[]) {
    this.#roles = roles;
    const first = this.#read();
    for (const [role, cpu] of Object.entries(first.cpuSeconds)) {
      this.#baseline.set(role, cpu);
    }
    for (const [worker, cpu] of Object.entries(first.workerCpuSeconds)) {
      this.#workerBaseline.set(worker, cpu);
    }
    for (const [role, bytes] of Object.entries(first.writeBytes)) {
      this.#writeBaseline.set(role, bytes);
    }
    this.#diskBaseline = first.diskBusyMs;
  }

  sample(): ProcSample {
    const raw = this.#read();
    const cpuSeconds: Record<string, number> = {};
    for (const [role, cpu] of Object.entries(raw.cpuSeconds)) {
      cpuSeconds[role] = cpu - (this.#baseline.get(role) ?? 0);
    }
    const workerCpuSeconds: Record<string, number> = {};
    for (const [worker, cpu] of Object.entries(raw.workerCpuSeconds)) {
      workerCpuSeconds[worker] = cpu - (this.#workerBaseline.get(worker) ?? 0);
    }
    const writeBytes: Record<string, number> = {};
    for (const [role, bytes] of Object.entries(raw.writeBytes)) {
      writeBytes[role] = bytes - (this.#writeBaseline.get(role) ?? 0);
    }
    return {
      atMs: raw.atMs,
      cpuSeconds,
      rssMiB: raw.rssMiB,
      workerCpuSeconds,
      writeBytes,
      diskBusyMs: raw.diskBusyMs - this.#diskBaseline,
    };
  }

  /**
   * The zero-cache worker a pid is running, from its command line: workers
   * are `fork()`ed with their module URL, so the module's basename is the
   * worker's name (`change-streamer`, `base-producer`, `replicator`,
   * `syncer`). The role's own root process is `main`.
   */
  #workerLabel(pid: number, rootPid: number): string {
    if (pid === rootPid) {
      return 'main';
    }
    const cached = this.#workerLabels.get(pid);
    if (cached !== undefined) {
      return cached;
    }
    let label = 'other';
    try {
      const cmdline = readFileSync(`/proc/${pid}/cmdline`, 'utf-8');
      const match = WORKER_MODULE.exec(cmdline.replaceAll('\0', ' '));
      label = match?.[1] ?? 'other';
    } catch {
      label = 'other';
    }
    this.#workerLabels.set(pid, label);
    return label;
  }

  #read(): ProcSample {
    const stats = readProcessTable();
    const cpuSeconds: Record<string, number> = {};
    const rssMiB: Record<string, number> = {};
    const workerCpuSeconds: Record<string, number> = {};
    const writeBytes: Record<string, number> = {};

    for (const role of this.#roles) {
      let cpu = 0;
      let rss = 0;
      let written = 0;
      const otherRoots = new Set(
        this.#roles.filter(r => r.pid !== role.pid).map(r => r.pid),
      );
      for (const pid of descendants(stats, role.pid, otherRoots)) {
        const stat = stats.get(pid);
        if (stat) {
          cpu += stat.cpuSeconds;
          rss += stat.rssMiB;
          const key = `${role.name}/${this.#workerLabel(pid, role.pid)}`;
          workerCpuSeconds[key] =
            (workerCpuSeconds[key] ?? 0) + stat.cpuSeconds;
          written += readWriteBytes(pid);
        }
      }
      cpuSeconds[role.name] = cpu;
      rssMiB[role.name] = rss;
      writeBytes[role.name] = written;
    }

    // Postgres is not in any of the roles' process trees (it is started
    // separately), so it is attributed by process name.
    let pgCpu = 0;
    let pgRss = 0;
    let pgWritten = 0;
    for (const stat of stats.values()) {
      if (stat.comm === 'postgres') {
        pgCpu += stat.cpuSeconds;
        pgRss += stat.rssMiB;
        pgWritten += readWriteBytes(stat.pid);
      }
    }
    cpuSeconds.postgres = pgCpu;
    rssMiB.postgres = pgRss;
    writeBytes.postgres = pgWritten;

    return {
      atMs: Date.now(),
      cpuSeconds,
      rssMiB,
      workerCpuSeconds,
      writeBytes,
      diskBusyMs: readDiskBusyMs(),
    };
  }
}

type ProcStat = {
  readonly pid: number;
  readonly ppid: number;
  readonly comm: string;
  readonly cpuSeconds: number;
  readonly rssMiB: number;
};

const WHITESPACE = /\s+/;
const PARTITION_SUFFIX = /\d$/;

/**
 * `io_ticks` (field 10) summed over whole block devices in
 * `/proc/diskstats`: milliseconds the device spent with I/O in flight.
 */
function readDiskBusyMs(): number {
  try {
    let busy = 0;
    for (const line of readFileSync('/proc/diskstats', 'utf-8').split('\n')) {
      const f = line.trim().split(WHITESPACE);
      // Partitions repeat their parent's I/O; count whole devices only.
      if (f.length < 13 || PARTITION_SUFFIX.test(f[2])) {
        continue;
      }
      busy += Number(f[12]);
    }
    return busy;
  } catch {
    return 0;
  }
}

/** `write_bytes` from `/proc/<pid>/io`: bytes that reached the block layer. */
function readWriteBytes(pid: number): number {
  try {
    const raw = readFileSync(`/proc/${pid}/io`, 'utf-8');
    const match = WRITE_BYTES.exec(raw);
    return match ? Number(match[1]) : 0;
  } catch {
    return 0; // exited, or not ours to read
  }
}

const WRITE_BYTES = /^write_bytes:\s+(\d+)$/m;

function readProcessTable(): Map<number, ProcStat> {
  const table = new Map<number, ProcStat>();
  for (const entry of readdirSync('/proc')) {
    const pid = Number(entry);
    if (!Number.isInteger(pid)) {
      continue;
    }
    const stat = readStat(pid);
    if (stat) {
      table.set(pid, stat);
    }
  }
  return table;
}

const PAGE_SIZE_MIB = 4096 / (1024 * 1024);

function readStat(pid: number): ProcStat | undefined {
  let raw: string;
  try {
    raw = readFileSync(`/proc/${pid}/stat`, 'utf-8');
  } catch {
    return undefined; // exited between readdir and read
  }
  // `comm` is parenthesized and may itself contain spaces, so split on the
  // last ')' rather than on whitespace.
  const close = raw.lastIndexOf(')');
  const comm = raw.slice(raw.indexOf('(') + 1, close);
  const fields = raw.slice(close + 2).split(' ');
  // Fields are 1-indexed from `state` at 3 in proc(5); after the slice,
  // index 0 is `state`, so ppid is 1, utime 11, stime 12, rss 21.
  const ppid = Number(fields[1]);
  const utime = Number(fields[11]);
  const stime = Number(fields[12]);
  const rssPages = Number(fields[21]);
  return {
    pid,
    ppid,
    comm,
    cpuSeconds: (utime + stime) / CLOCK_TICKS_PER_SEC,
    rssMiB: rssPages * PAGE_SIZE_MIB,
  };
}

/**
 * The pids under `root`, stopping at any pid in `stopAt`. The harness
 * spawns the zero-cache processes, so without the stop set every role's
 * CPU would also be counted as the harness's.
 */
function descendants(
  stats: Map<number, ProcStat>,
  root: number,
  stopAt: ReadonlySet<number> = new Set(),
): number[] {
  const children = new Map<number, number[]>();
  for (const stat of stats.values()) {
    const list = children.get(stat.ppid);
    if (list) {
      list.push(stat.pid);
    } else {
      children.set(stat.ppid, [stat.pid]);
    }
  }
  const out: number[] = [];
  const queue = [root];
  while (queue.length > 0) {
    const pid = queue.pop() as number;
    out.push(pid);
    for (const child of children.get(pid) ?? []) {
      if (!stopAt.has(child)) {
        queue.push(child);
      }
    }
  }
  return out;
}
