# Builds the ceiling harness against the *same* SQLite amalgamation and the same
# compile-time defines zero-sqlite3 uses, so the number it produces is
# comparable to what the JS benchmarks measure.
SQLITE_DIR := $(shell node -e "console.log(require.resolve('@rocicorp/zero-sqlite3/package.json').replace(/package.json$$/,'deps/sqlite3'))" 2>/dev/null)
DEFINES_GYPI := $(SQLITE_DIR)/../defines.gypi
DEFS := $(shell grep -o "'SQLITE_[A-Z0-9_]*\(=[0-9x]*\)\?'" $(DEFINES_GYPI) | tr -d "'" | sort -u | sed 's/^/-D/')

all: apply keys

apply: apply.c
	cc -O3 -DNDEBUG $(DEFS) -I$(SQLITE_DIR) -o $@ apply.c $(SQLITE_DIR)/sqlite3.c -lpthread -lm -ldl

keys: keys.c
	cc -O3 -DNDEBUG $(DEFS) -I$(SQLITE_DIR) -o $@ keys.c $(SQLITE_DIR)/sqlite3.c -lpthread -lm -ldl

clean:
	rm -f apply keys
.PHONY: all clean
