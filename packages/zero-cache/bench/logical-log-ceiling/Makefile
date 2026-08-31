# Builds the ceiling harnesses against the *same* SQLite amalgamation and the
# same compile-time defines zero-sqlite3 uses, so the numbers are comparable to
# what the TypeScript benchmarks measure.

SQLITE_DIR := $(shell node -e "console.log(require.resolve('@rocicorp/zero-sqlite3/package.json').replace(/package.json$$/,'deps/sqlite3'))" 2>/dev/null)
DEFINES_GYPI := $(SQLITE_DIR)/../defines.gypi
DEFS := $(shell grep -o "'SQLITE_[A-Z0-9_]*\(=[0-9x]*\)\?'" $(DEFINES_GYPI) | tr -d "'" | sort -u | sed 's/^/-D/')

# macOS has no libdl and doesn't need -lpthread; Linux wants both.
UNAME := $(shell uname -s)
ifeq ($(UNAME),Darwin)
  LIBS := -lm
else
  LIBS := -lpthread -lm -ldl
endif

CFLAGS := -O3 -DNDEBUG $(DEFS) -I$(SQLITE_DIR)

all: apply keys

check:
	@test -n "$(SQLITE_DIR)" || { echo "could not locate @rocicorp/zero-sqlite3 -- run pnpm install from the repo root"; exit 1; }
	@test -f "$(SQLITE_DIR)/sqlite3.c" || { echo "missing $(SQLITE_DIR)/sqlite3.c"; exit 1; }
	@echo "sqlite amalgamation: $(SQLITE_DIR)"

apply: apply.c
	cc $(CFLAGS) -o $@ apply.c $(SQLITE_DIR)/sqlite3.c $(LIBS)

keys: keys.c
	cc $(CFLAGS) -o $@ keys.c $(SQLITE_DIR)/sqlite3.c $(LIBS)

clean:
	rm -f apply keys
.PHONY: all check clean
