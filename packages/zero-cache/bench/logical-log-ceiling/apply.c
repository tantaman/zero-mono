/*
 * Absolute ceiling for applying a logical log to a SQLite replica.
 *
 * Everything that could be removed, has been:
 *   - the log is mmap'd and walked by pointer arithmetic over fixed-stride
 *     records. There is no parsing, no tokenizing, no allocation per change.
 *   - strings are bound with SQLITE_STATIC directly out of the mapping, so no
 *     bytes are copied on the way into SQLite.
 *   - every statement is prepared once at startup and only reset + rebound.
 *
 * What is left is SQLite's own work plus the bind/step calls. That is the
 * number this program exists to produce.
 *
 * Build:
 *   cc -O3 -o apply apply.c sqlite3.c -lpthread -lm -ldl <same defines as zero-sqlite3>
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include "sqlite3.h"

#define STRIDE 96
#define OP_INSERT 1
#define OP_UPDATE 2
#define OP_DELETE 3
#define OP_BEGIN  4
#define OP_COMMIT 5

/* Record layout, matching export.ts exactly.
 *   0  u8  op
 *   1  u8  table          0 = issue, 1 = comment
 *   2  u16 null bitmap    bit i set => string slot i is NULL
 *   4  u32 pad
 *   8  {u32 off, u32 len} x nStr      offsets into the string arena
 *   8 + nStr*8  i64 x nInt
 */
typedef struct { uint32_t off, len; } Str;

static const uint8_t *g_arena;

static inline void bind_str(sqlite3_stmt *s, int idx, const uint8_t *rec,
                            int slot, uint16_t nulls) {
  if (nulls & (1u << slot)) { sqlite3_bind_null(s, idx); return; }
  Str v;
  memcpy(&v, rec + 8 + slot * 8, sizeof v);
  /* SQLITE_STATIC: the mapping outlives every statement, so SQLite may keep
   * the pointer instead of copying the bytes. */
  sqlite3_bind_text(s, idx, (const char *)(g_arena + v.off), (int)v.len,
                    SQLITE_STATIC);
}

static inline int64_t get_int(const uint8_t *rec, int base, int slot) {
  int64_t v;
  memcpy(&v, rec + base + slot * 8, sizeof v);
  return v;
}

static void die(sqlite3 *db, const char *what) {
  fprintf(stderr, "%s: %s\n", what, db ? sqlite3_errmsg(db) : "");
  exit(1);
}

static sqlite3_stmt *prep(sqlite3 *db, const char *sql) {
  sqlite3_stmt *s;
  if (sqlite3_prepare_v2(db, sql, -1, &s, NULL) != SQLITE_OK) die(db, sql);
  return s;
}

static void run_once(sqlite3 *db, const char *sql) {
  char *err = NULL;
  if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
    fprintf(stderr, "%s: %s\n", sql, err ? err : "");
    exit(1);
  }
}

/* Actual bytes this process moved to and from the filesystem, so page-level
 * write amplification is visible rather than inferred. */
static void proc_io(long long *rd, long long *wr) {
  *rd = *wr = -1;
  FILE *f = fopen("/proc/self/io", "r");
  if (!f) return;
  char k[64]; long long v;
  while (fscanf(f, "%63[^:]: %lld\n", k, &v) == 2) {
    if (!strcmp(k, "read_bytes")) *rd = v;
    else if (!strcmp(k, "write_bytes")) *wr = v;
  }
  fclose(f);
}

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

/* Variants, to decompose where the remaining time goes. Each removes work the
 * apply path does today but arguably need not:
 *   ZLOG_NO_KEY_IN_UPDATE=1  omit the key column from UPDATE ... SET. Setting
 *                            it forces unique-index maintenance on every update
 *                            even though the value never changes.
 *   ZLOG_PLAIN_INSERT=1      plain INSERT instead of INSERT OR REPLACE, which
 *                            has to probe every unique index before inserting.
 *   ZLOG_DROP_SECONDARY=1    drop the non-key indexes. Not a real option -- the
 *                            replica carries upstream's indexes -- but it
 *                            bounds what index maintenance is costing.
 */
static int envflag(const char *name) {
  const char *v = getenv(name);
  return v && *v && strcmp(v, "0") != 0;
}

int main(int argc, char **argv) {
  if (argc < 3) { fprintf(stderr, "usage: apply <db> <log.bin>\n"); return 2; }
  int no_key_in_update = envflag("ZLOG_NO_KEY_IN_UPDATE");
  int plain_insert = envflag("ZLOG_PLAIN_INSERT");
  int drop_secondary = envflag("ZLOG_DROP_SECONDARY");
  int defer_indexes = envflag("ZLOG_DEFER_INDEXES");
  const char *mmap_mb = getenv("ZLOG_MMAP_MB");
  const char *cache_mb = getenv("ZLOG_CACHE_MB");

  int fd = open(argv[2], O_RDONLY);
  if (fd < 0) { perror("open log"); return 1; }
  struct stat st;
  if (fstat(fd, &st) != 0) { perror("fstat"); return 1; }
  const uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (map == MAP_FAILED) { perror("mmap"); return 1; }
  madvise((void *)map, st.st_size, MADV_SEQUENTIAL | MADV_WILLNEED);

  if (memcmp(map, "ZLOG0001", 8) != 0) { fprintf(stderr, "bad magic\n"); return 1; }
  uint32_t stride, nrec, arena_off;
  memcpy(&stride, map + 8, 4);
  memcpy(&nrec, map + 12, 4);
  memcpy(&arena_off, map + 16, 4);
  if (stride != STRIDE) { fprintf(stderr, "stride mismatch\n"); return 1; }
  const uint8_t *recs = map + 64;
  g_arena = map + arena_off;

  sqlite3 *db;
  if (sqlite3_open(argv[1], &db) != SQLITE_OK) die(db, "open db");
  run_once(db, "PRAGMA locking_mode = EXCLUSIVE");
  run_once(db, "PRAGMA journal_mode = OFF");
  run_once(db, "PRAGMA synchronous = OFF");
  run_once(db, "PRAGMA busy_timeout = 30000");
  char pg[128];
  if (mmap_mb && *mmap_mb) {
    snprintf(pg, sizeof pg, "PRAGMA mmap_size = %lld",
             atoll(mmap_mb) * 1048576LL);
    run_once(db, pg);
  }
  /* Defer index maintenance: drop every index the apply path does not itself
   * need, replay, then rebuild. The apply path only ever looks a row up by its
   * key, so the key index stays; everything else is dead weight during replay
   * and can be rebuilt in one sorted pass at the end.
   *
   * Legitimate because a replica being caught up serves no queries. A
   * restoring ViewSyncer would rebuild before serving; the replication
   * manager's replica needs them only once something restores from it. */
  char *deferred_sql[32]; char *deferred_name[32]; int n_deferred = 0;
  double drop_ms = 0, rebuild_ms = 0;
  if (defer_indexes) {
    sqlite3_stmt *q = prep(db,
      "SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL");
    while (sqlite3_step(q) == SQLITE_ROW && n_deferred < 32) {
      const char *nm = (const char *)sqlite3_column_text(q, 0);
      const char *sq = (const char *)sqlite3_column_text(q, 1);
      char isql[256]; sqlite3_stmt *ii;
      snprintf(isql, sizeof isql, "PRAGMA index_info(\"%s\")", nm);
      if (sqlite3_prepare_v2(db, isql, -1, &ii, NULL) != SQLITE_OK) continue;
      int ncols = 0, is_id = 0;
      while (sqlite3_step(ii) == SQLITE_ROW) {
        ncols++;
        const char *c = (const char *)sqlite3_column_text(ii, 2);
        if (c && strcmp(c, "id") == 0) is_id = 1;
      }
      sqlite3_finalize(ii);
      if (ncols == 1 && is_id) continue;   /* the key index has to stay */
      deferred_name[n_deferred] = strdup(nm);
      deferred_sql[n_deferred] = strdup(sq);
      n_deferred++;
    }
    sqlite3_finalize(q);
    double d0 = now_ms();
    for (int i = 0; i < n_deferred; i++) {
      char st[256];
      snprintf(st, sizeof st, "DROP INDEX \"%s\"", deferred_name[i]);
      run_once(db, st);
    }
    drop_ms = now_ms() - d0;
  }

  if (drop_secondary) {
    /* Bounds what non-key index maintenance costs. Not a real option -- the
     * replica carries upstream's indexes. */
    run_once(db, "DROP INDEX IF EXISTS issue_modified_idx");
    run_once(db, "DROP INDEX IF EXISTS comment_issue_idx");
  }
  if (cache_mb && *cache_mb) {
    snprintf(pg, sizeof pg, "PRAGMA cache_size = -%lld", atoll(cache_mb) * 1024LL);
    run_once(db, pg);
  }

  /* Column order matches what the JS appliers bind, so the resulting replica
   * is byte-for-byte comparable. */
  sqlite3_stmt *ins_i = prep(db,
    "INSERT OR REPLACE INTO \"issue\" (\"id\",\"shortID\",\"title\",\"description\","
    "\"open\",\"creatorID\",\"assigneeID\",\"created\",\"modified\",\"visibility\","
    "\"_0_version\") VALUES (?,?,?,?,?,?,?,?,?,?,?)");
  sqlite3_stmt *upd_i = prep(db,
    "UPDATE \"issue\" SET \"id\"=?,\"shortID\"=?,\"title\"=?,\"description\"=?,"
    "\"open\"=?,\"creatorID\"=?,\"assigneeID\"=?,\"created\"=?,\"modified\"=?,"
    "\"visibility\"=?,\"_0_version\"=? WHERE \"id\"=?");
  sqlite3_stmt *del_i = prep(db, "DELETE FROM \"issue\" WHERE \"id\"=?");
  sqlite3_stmt *ins_c = prep(db, plain_insert ?
    "INSERT INTO \"comment\" (\"id\",\"issueID\",\"creatorID\",\"created\","
    "\"body\",\"_0_version\") VALUES (?,?,?,?,?,?)" :
    "INSERT OR REPLACE INTO \"comment\" (\"id\",\"issueID\",\"creatorID\",\"created\","
    "\"body\",\"_0_version\") VALUES (?,?,?,?,?,?)");
  sqlite3_stmt *upd_c = prep(db, no_key_in_update ?
    "UPDATE \"comment\" SET \"issueID\"=?,\"creatorID\"=?,\"created\"=?,"
    "\"body\"=?,\"_0_version\"=? WHERE \"id\"=?" :
    "UPDATE \"comment\" SET \"id\"=?,\"issueID\"=?,\"creatorID\"=?,\"created\"=?,"
    "\"body\"=?,\"_0_version\"=? WHERE \"id\"=?");
  sqlite3_stmt *del_c = prep(db, "DELETE FROM \"comment\" WHERE \"id\"=?");
  sqlite3_stmt *setv = prep(db,
    "UPDATE \"_zero.replicationState\" SET stateVersion = ?");
  sqlite3_stmt *begin = prep(db, "BEGIN IMMEDIATE");
  sqlite3_stmt *commit = prep(db, "COMMIT");

  const char *ver = NULL; int ver_len = 0;
  long applied = 0;
  /* Per-op accounting: a mixed log hides which operation actually costs. */
  double op_ms[6] = {0}; long op_n[6] = {0};
  double commit_ms = 0;

  long long rd0, wr0, rd1, wr1;
  proc_io(&rd0, &wr0);
  double t0 = now_ms();
  for (uint32_t i = 0; i < nrec; i++) {
    const uint8_t *r = recs + (size_t)i * STRIDE;
    uint8_t op = r[0], table = r[1];
    uint16_t nulls;
    memcpy(&nulls, r + 2, 2);

    if (op == OP_BEGIN) {
      Str w; memcpy(&w, r + 8, sizeof w);
      ver = (const char *)(g_arena + w.off); ver_len = (int)w.len;
      sqlite3_reset(begin);
      if (sqlite3_step(begin) != SQLITE_DONE) die(db, "begin");
      continue;
    }
    if (op == OP_COMMIT) {
      double c0 = now_ms();
      Str w; memcpy(&w, r + 8, sizeof w);
      sqlite3_reset(setv);
      sqlite3_bind_text(setv, 1, (const char *)(g_arena + w.off), (int)w.len,
                        SQLITE_STATIC);
      if (sqlite3_step(setv) != SQLITE_DONE) die(db, "setv");
      sqlite3_reset(commit);
      if (sqlite3_step(commit) != SQLITE_DONE) die(db, "commit");
      commit_ms += now_ms() - c0;
      continue;
    }
    double o0 = now_ms();

    if (table == 0) {                       /* issue: 6 strings, 4 ints @56 */
      if (op == OP_DELETE) {
        sqlite3_reset(del_i);
        bind_str(del_i, 1, r, 0, nulls);
        if (sqlite3_step(del_i) != SQLITE_DONE) die(db, "del issue");
        applied++; op_ms[op] += now_ms() - o0; op_n[op]++;
        continue;
      }
      sqlite3_stmt *s = (op == OP_INSERT) ? ins_i : upd_i;
      int d = (op == OP_UPDATE && no_key_in_update) ? 1 : 0;
      sqlite3_reset(s);
      if (!d) bind_str(s, 1, r, 0, nulls);             /* id */
      sqlite3_bind_int64(s, 2 - d, get_int(r, 56, 0)); /* shortID */
      bind_str(s, 3 - d, r, 1, nulls);                 /* title */
      bind_str(s, 4 - d, r, 2, nulls);                 /* description */
      sqlite3_bind_int64(s, 5 - d, get_int(r, 56, 1)); /* open */
      bind_str(s, 6 - d, r, 3, nulls);                 /* creatorID */
      bind_str(s, 7 - d, r, 4, nulls);                 /* assigneeID */
      sqlite3_bind_int64(s, 8 - d, get_int(r, 56, 2)); /* created */
      sqlite3_bind_int64(s, 9 - d, get_int(r, 56, 3)); /* modified */
      bind_str(s, 10 - d, r, 5, nulls);                /* visibility */
      sqlite3_bind_text(s, 11 - d, ver, ver_len, SQLITE_STATIC);
      if (op == OP_UPDATE) bind_str(s, 12 - d, r, 0, nulls);
      if (sqlite3_step(s) != SQLITE_DONE) die(db, "issue");
      if (op == OP_UPDATE && sqlite3_changes(db) == 0) {
        sqlite3_reset(ins_i);
        bind_str(ins_i, 1, r, 0, nulls);
        sqlite3_bind_int64(ins_i, 2, get_int(r, 56, 0));
        bind_str(ins_i, 3, r, 1, nulls);
        bind_str(ins_i, 4, r, 2, nulls);
        sqlite3_bind_int64(ins_i, 5, get_int(r, 56, 1));
        bind_str(ins_i, 6, r, 3, nulls);
        bind_str(ins_i, 7, r, 4, nulls);
        sqlite3_bind_int64(ins_i, 8, get_int(r, 56, 2));
        sqlite3_bind_int64(ins_i, 9, get_int(r, 56, 3));
        bind_str(ins_i, 10, r, 5, nulls);
        sqlite3_bind_text(ins_i, 11, ver, ver_len, SQLITE_STATIC);
        if (sqlite3_step(ins_i) != SQLITE_DONE) die(db, "issue upsert");
      }
    } else {                                /* comment: 4 strings, 1 int @40 */
      if (op == OP_DELETE) {
        sqlite3_reset(del_c);
        bind_str(del_c, 1, r, 0, nulls);
        if (sqlite3_step(del_c) != SQLITE_DONE) die(db, "del comment");
        applied++; op_ms[op] += now_ms() - o0; op_n[op]++;
        continue;
      }
      sqlite3_stmt *s = (op == OP_INSERT) ? ins_c : upd_c;
      int d = (op == OP_UPDATE && no_key_in_update) ? 1 : 0;
      sqlite3_reset(s);
      if (!d) bind_str(s, 1, r, 0, nulls);             /* id */
      bind_str(s, 2 - d, r, 1, nulls);                 /* issueID */
      bind_str(s, 3 - d, r, 2, nulls);                 /* creatorID */
      sqlite3_bind_int64(s, 4 - d, get_int(r, 40, 0)); /* created */
      bind_str(s, 5 - d, r, 3, nulls);                 /* body */
      sqlite3_bind_text(s, 6 - d, ver, ver_len, SQLITE_STATIC);
      if (op == OP_UPDATE) bind_str(s, 7 - d, r, 0, nulls);
      if (sqlite3_step(s) != SQLITE_DONE) die(db, "comment");
      if (op == OP_UPDATE && sqlite3_changes(db) == 0) {
        sqlite3_reset(ins_c);
        bind_str(ins_c, 1, r, 0, nulls);
        bind_str(ins_c, 2, r, 1, nulls);
        bind_str(ins_c, 3, r, 2, nulls);
        sqlite3_bind_int64(ins_c, 4, get_int(r, 40, 0));
        bind_str(ins_c, 5, r, 3, nulls);
        sqlite3_bind_text(ins_c, 6, ver, ver_len, SQLITE_STATIC);
        if (sqlite3_step(ins_c) != SQLITE_DONE) die(db, "comment upsert");
      }
    }
    applied++;
    op_ms[op] += now_ms() - o0;
    op_n[op]++;
  }
  double ms = now_ms() - t0;
  proc_io(&rd1, &wr1);

  if (defer_indexes) {
    double r0 = now_ms();
    for (int i = 0; i < n_deferred; i++) run_once(db, deferred_sql[i]);
    rebuild_ms = now_ms() - r0;
  }

  /* Same fingerprint the JS benchmark computes, so the two can be compared. */
  char buf[512]; int n = 0;
  const char *tables[2] = {"issue", "comment"};
  for (int t = 0; t < 2; t++) {
    char sql[512];
    snprintf(sql, sizeof sql,
      "SELECT COUNT(*), SUM(LENGTH(COALESCE(\"id\",''))), "
      "SUM(LENGTH(COALESCE(\"created\",0))), COUNT(DISTINCT \"_0_version\") "
      "FROM \"%s\"", tables[t]);
    sqlite3_stmt *s = prep(db, sql);
    if (sqlite3_step(s) != SQLITE_ROW) die(db, "checksum");
    n += snprintf(buf + n, sizeof buf - n, "%s%s:%lld/%lld/%lld/%lld",
                  t ? " " : "", tables[t],
                  (long long)sqlite3_column_int64(s, 0),
                  (long long)sqlite3_column_int64(s, 1),
                  (long long)sqlite3_column_int64(s, 2),
                  (long long)sqlite3_column_int64(s, 3));
    sqlite3_finalize(s);
  }
  sqlite3_stmt *s = prep(db, "SELECT stateVersion FROM \"_zero.replicationState\"");
  if (sqlite3_step(s) != SQLITE_ROW) die(db, "stateVersion");
  n += snprintf(buf + n, sizeof buf - n, " v:%s", sqlite3_column_text(s, 0));
  sqlite3_finalize(s);

  printf("applied=%ld  %.1f ms  %.2f us/change", applied, ms,
         ms * 1000.0 / (double)applied);
  if (wr1 >= 0 && wr0 >= 0) {
    printf("  io: read %.0f MB write %.0f MB", (rd1 - rd0) / 1048576.0,
           (wr1 - wr0) / 1048576.0);
  }
  printf("\n");
  static const struct {int op; const char *name;} OPS[] = {
    {OP_INSERT, "insert"}, {OP_UPDATE, "update"}, {OP_DELETE, "delete"}};
  for (unsigned k = 0; k < sizeof OPS / sizeof OPS[0]; k++) {
    int o = OPS[k].op;
    if (op_n[o])
      printf("    %-7s n=%-7ld %7.1f ms  %6.2f us each\n", OPS[k].name, op_n[o],
             op_ms[o], op_ms[o] * 1000.0 / (double)op_n[o]);
  }
  printf("    %-7s          %7.1f ms\n", "commit", commit_ms);
  if (defer_indexes) {
    printf("    deferred %d index(es): drop %.1f + rebuild %.1f ms"
           " => total %.1f ms (%.2f us/change)\n",
           n_deferred, drop_ms, rebuild_ms, ms + drop_ms + rebuild_ms,
           (ms + drop_ms + rebuild_ms) * 1000.0 / (double)applied);
  }
  printf("    checksum=%s\n", buf);
  sqlite3_close(db);
  munmap((void *)map, st.st_size);
  close(fd);
  return 0;
}
