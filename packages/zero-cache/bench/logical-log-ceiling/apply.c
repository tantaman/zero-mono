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

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

int main(int argc, char **argv) {
  if (argc < 3) { fprintf(stderr, "usage: apply <db> <log.bin>\n"); return 2; }

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
  sqlite3_stmt *ins_c = prep(db,
    "INSERT OR REPLACE INTO \"comment\" (\"id\",\"issueID\",\"creatorID\",\"created\","
    "\"body\",\"_0_version\") VALUES (?,?,?,?,?,?)");
  sqlite3_stmt *upd_c = prep(db,
    "UPDATE \"comment\" SET \"id\"=?,\"issueID\"=?,\"creatorID\"=?,\"created\"=?,"
    "\"body\"=?,\"_0_version\"=? WHERE \"id\"=?");
  sqlite3_stmt *del_c = prep(db, "DELETE FROM \"comment\" WHERE \"id\"=?");
  sqlite3_stmt *setv = prep(db,
    "UPDATE \"_zero.replicationState\" SET stateVersion = ?");
  sqlite3_stmt *begin = prep(db, "BEGIN IMMEDIATE");
  sqlite3_stmt *commit = prep(db, "COMMIT");

  const char *ver = NULL; int ver_len = 0;
  long applied = 0;

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
      Str w; memcpy(&w, r + 8, sizeof w);
      sqlite3_reset(setv);
      sqlite3_bind_text(setv, 1, (const char *)(g_arena + w.off), (int)w.len,
                        SQLITE_STATIC);
      if (sqlite3_step(setv) != SQLITE_DONE) die(db, "setv");
      sqlite3_reset(commit);
      if (sqlite3_step(commit) != SQLITE_DONE) die(db, "commit");
      continue;
    }

    if (table == 0) {                       /* issue: 6 strings, 4 ints @56 */
      if (op == OP_DELETE) {
        sqlite3_reset(del_i);
        bind_str(del_i, 1, r, 0, nulls);
        if (sqlite3_step(del_i) != SQLITE_DONE) die(db, "del issue");
        applied++;
        continue;
      }
      sqlite3_stmt *s = (op == OP_INSERT) ? ins_i : upd_i;
      sqlite3_reset(s);
      bind_str(s, 1, r, 0, nulls);                     /* id */
      sqlite3_bind_int64(s, 2, get_int(r, 56, 0));     /* shortID */
      bind_str(s, 3, r, 1, nulls);                     /* title */
      bind_str(s, 4, r, 2, nulls);                     /* description */
      sqlite3_bind_int64(s, 5, get_int(r, 56, 1));     /* open */
      bind_str(s, 6, r, 3, nulls);                     /* creatorID */
      bind_str(s, 7, r, 4, nulls);                     /* assigneeID */
      sqlite3_bind_int64(s, 8, get_int(r, 56, 2));     /* created */
      sqlite3_bind_int64(s, 9, get_int(r, 56, 3));     /* modified */
      bind_str(s, 10, r, 5, nulls);                    /* visibility */
      sqlite3_bind_text(s, 11, ver, ver_len, SQLITE_STATIC);
      if (op == OP_UPDATE) bind_str(s, 12, r, 0, nulls);
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
        applied++;
        continue;
      }
      sqlite3_stmt *s = (op == OP_INSERT) ? ins_c : upd_c;
      sqlite3_reset(s);
      bind_str(s, 1, r, 0, nulls);                     /* id */
      bind_str(s, 2, r, 1, nulls);                     /* issueID */
      bind_str(s, 3, r, 2, nulls);                     /* creatorID */
      sqlite3_bind_int64(s, 4, get_int(r, 40, 0));     /* created */
      bind_str(s, 5, r, 3, nulls);                     /* body */
      sqlite3_bind_text(s, 6, ver, ver_len, SQLITE_STATIC);
      if (op == OP_UPDATE) bind_str(s, 7, r, 0, nulls);
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
  }
  double ms = now_ms() - t0;

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

  printf("applied=%ld  %.1f ms  %.2f us/change  checksum=%s\n",
         applied, ms, ms * 1000.0 / (double)applied, buf);
  sqlite3_close(db);
  munmap((void *)map, st.st_size);
  close(fd);
  return 0;
}
