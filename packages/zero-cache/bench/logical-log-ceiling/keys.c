/*
 * Why does replay slow down so much as the replica grows?
 *
 * Two candidate explanations, both about how rows are keyed rather than about
 * how much data there is:
 *
 *   1. The replica has no real primary key. `create-table` produces a plain
 *      rowid table and the key arrives later as a separate UNIQUE index, so a
 *      lookup by id is two B-tree descents (index -> rowid -> row) and an
 *      insert writes two trees. A WITHOUT ROWID table stores the row *in* the
 *      id-ordered tree: one descent, one tree.
 *
 *   2. The keys are random. Zero apps key rows with nanoid/uuid4-style random
 *      strings, so every insert lands at an unpredictable point in the index
 *      and dirties a fresh page. A time-ordered key (uuidv7) appends to the
 *      right edge instead.
 *
 * This measures both, crossed, at several table sizes. Same row payload
 * throughout so the only variables are key scheme and table structure.
 *
 * Build: make keys
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include "sqlite3.h"

static uint64_t rng_state = 0x9e3779b97f4a7c15ULL;
static inline uint64_t rnd(void) {
  rng_state ^= rng_state << 13;
  rng_state ^= rng_state >> 7;
  rng_state ^= rng_state << 17;
  return rng_state;
}
static const char HEX[] = "0123456789abcdef";

typedef enum { KEY_RANDOM, KEY_V7, KEY_SEQNUM } KeyScheme;

/* 32 hex chars. RANDOM is uuid4-shaped. V7 puts a monotonic 48-bit counter
 * first, so keys sort in insertion order the way uuidv7 does. SEQNUM is what
 * the rest of these benchmarks use: a decimal suffix, which sorts
 * lexicographically scrambled ("id-10" < "id-9"). */
static void make_key(char *out, KeyScheme scheme, uint64_t i) {
  switch (scheme) {
    case KEY_RANDOM: {
      for (int k = 0; k < 32; k++) out[k] = HEX[rnd() & 15];
      out[32] = 0;
      break;
    }
    case KEY_V7: {
      uint64_t ts = 1700000000000ULL + i;      /* monotonic, ms-like */
      for (int k = 11; k >= 0; k--) { out[k] = HEX[ts & 15]; ts >>= 4; }
      for (int k = 12; k < 32; k++) out[k] = HEX[rnd() & 15];
      out[32] = 0;
      break;
    }
    case KEY_SEQNUM:
      snprintf(out, 33, "issue-b%llu", (unsigned long long)i);
      break;
  }
}

static double now_ms(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

static void ex(sqlite3 *db, const char *sql) {
  char *err = NULL;
  if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
    fprintf(stderr, "%s: %s\n", sql, err ? err : "");
    exit(1);
  }
}

static char TITLE[64], DESC[512];

int main(int argc, char **argv) {
  uint64_t N = argc > 1 ? strtoull(argv[1], NULL, 10) : 2000000;
  uint64_t M = argc > 2 ? strtoull(argv[2], NULL, 10) : 50000;

  memset(TITLE, 'a', sizeof TITLE - 1);
  memset(DESC, 'b', sizeof DESC - 1);

  const char *scheme_name[] = {"random(uuid4)", "time-ordered(v7)", "seqnum"};
  const char *struct_name[] = {"rowid+uniq idx", "WITHOUT ROWID"};

  printf("%-17s %-16s %10s %14s %14s\n",
         "keys", "table", "db MB", "insert rows/s", "update rows/s");
  printf("--------------------------------------------------------------------------\n");

  char **keys = malloc(sizeof(char *) * N);
  char *keybuf = malloc(33 * N);
  for (uint64_t i = 0; i < N; i++) keys[i] = keybuf + 33 * i;

  for (int without_rowid = 0; without_rowid < 2; without_rowid++) {
    for (int sc = 0; sc < 3; sc++) {
      char path[256];
      snprintf(path, sizeof path, "/tmp/keys-%d-%d.db", without_rowid, sc);
      remove(path);

      sqlite3 *db;
      if (sqlite3_open(path, &db) != SQLITE_OK) { fprintf(stderr, "open\n"); return 1; }
      ex(db, "PRAGMA locking_mode = EXCLUSIVE");
      ex(db, "PRAGMA journal_mode = OFF");
      ex(db, "PRAGMA synchronous = OFF");

      if (without_rowid) {
        ex(db, "CREATE TABLE issue (id TEXT NOT NULL, \"shortID\" INTEGER,"
               " title TEXT, description TEXT, \"creatorID\" TEXT,"
               " created INTEGER, modified INTEGER, \"_0_version\" TEXT,"
               " PRIMARY KEY(id)) WITHOUT ROWID");
      } else {
        /* Exactly what the replica ends up with today: a plain rowid table
         * plus a separate UNIQUE index carrying the key. */
        ex(db, "CREATE TABLE issue (id TEXT NOT NULL, \"shortID\" INTEGER,"
               " title TEXT, description TEXT, \"creatorID\" TEXT,"
               " created INTEGER, modified INTEGER, \"_0_version\" TEXT)");
        ex(db, "CREATE UNIQUE INDEX issue_pkey ON issue (id)");
      }

      rng_state = 0x9e3779b97f4a7c15ULL;
      for (uint64_t i = 0; i < N; i++) make_key(keys[i], (KeyScheme)sc, i);

      sqlite3_stmt *ins;
      sqlite3_prepare_v2(db,
        "INSERT OR REPLACE INTO issue VALUES (?,?,?,?,?,?,?,?)", -1, &ins, NULL);

      double t0 = now_ms();
      ex(db, "BEGIN");
      for (uint64_t i = 0; i < N; i++) {
        sqlite3_reset(ins);
        sqlite3_bind_text(ins, 1, keys[i], -1, SQLITE_STATIC);
        sqlite3_bind_int64(ins, 2, (int64_t)i);
        sqlite3_bind_text(ins, 3, TITLE, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 4, DESC, -1, SQLITE_STATIC);
        sqlite3_bind_text(ins, 5, "user-1", -1, SQLITE_STATIC);
        sqlite3_bind_int64(ins, 6, 1700000000000LL + (int64_t)i);
        sqlite3_bind_int64(ins, 7, 1700000000000LL + (int64_t)i);
        sqlite3_bind_text(ins, 8, "0a", -1, SQLITE_STATIC);
        if (sqlite3_step(ins) != SQLITE_DONE) { fprintf(stderr, "insert\n"); return 1; }
        if ((i % 100000) == 99999) { ex(db, "COMMIT"); ex(db, "BEGIN"); }
      }
      ex(db, "COMMIT");
      double ins_ms = now_ms() - t0;

      sqlite3_stmt *pc, *ps;
      sqlite3_prepare_v2(db, "PRAGMA page_count", -1, &pc, NULL);
      sqlite3_prepare_v2(db, "PRAGMA page_size", -1, &ps, NULL);
      sqlite3_step(pc); sqlite3_step(ps);
      double mb = (double)sqlite3_column_int64(pc, 0) *
                  (double)sqlite3_column_int64(ps, 0) / 1048576.0;
      sqlite3_finalize(pc); sqlite3_finalize(ps);

      /* Random point updates by key -- the shape replay actually has. */
      sqlite3_stmt *upd;
      sqlite3_prepare_v2(db,
        "UPDATE issue SET title=?, description=?, modified=?, \"_0_version\"=?"
        " WHERE id=?", -1, &upd, NULL);
      t0 = now_ms();
      ex(db, "BEGIN");
      for (uint64_t j = 0; j < M; j++) {
        const char *k = keys[rnd() % N];
        sqlite3_reset(upd);
        sqlite3_bind_text(upd, 1, TITLE, -1, SQLITE_STATIC);
        sqlite3_bind_text(upd, 2, DESC, -1, SQLITE_STATIC);
        sqlite3_bind_int64(upd, 3, 1700000009999LL);
        sqlite3_bind_text(upd, 4, "0b", -1, SQLITE_STATIC);
        sqlite3_bind_text(upd, 5, k, -1, SQLITE_STATIC);
        if (sqlite3_step(upd) != SQLITE_DONE) { fprintf(stderr, "update\n"); return 1; }
        if ((j % 20000) == 19999) { ex(db, "COMMIT"); ex(db, "BEGIN"); }
      }
      ex(db, "COMMIT");
      double upd_ms = now_ms() - t0;

      printf("%-17s %-16s %10.0f %14.0f %14.0f\n",
             scheme_name[sc], struct_name[without_rowid], mb,
             N / (ins_ms / 1000.0), M / (upd_ms / 1000.0));
      fflush(stdout);

      sqlite3_finalize(ins); sqlite3_finalize(upd);
      sqlite3_close(db);
      remove(path);
    }
  }
  return 0;
}
