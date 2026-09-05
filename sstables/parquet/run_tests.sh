#!/bin/bash
# Full test suite for sstables/parquet (layer 1 format codec + layer 2 mapping).
# Standalone by design: no Seastar, no Scylla headers, no libthrift.
set -u
cd "$(dirname "$0")" || exit 1
# Test fixtures: real Parquet files from public datasets plus generated
# conformance cases. Point PARQUET_TEST_DATA at a directory containing
# nyc_taxi.parquet, hits_0.parquet and conf/*.parquet (see
# docs/dev/parquet-storage-format.md section 9.3 for where they come from).
# The default is the lab location; CI overrides it with the env var.
DATA=${1:-${PARQUET_TEST_DATA:-$HOME/pq-lab/data}}
FAIL=0
S=format

# Fixture preflight.
#
# This used to be a bare `[ -d "$DATA" ] || exit 2`, which was a false-pass
# generator in two distinct ways, and a "green baseline" was in fact once
# reported off it:
#   * it printed no PARQUET_SUITE terminator line, so the usual way of reading
#     this log -- grep for PASS/FAILURES -- found neither, and "no failures"
#     got read as "passed". The run had executed zero test cases.
#   * `-d` is true for an *empty* directory, so pointing PARQUET_TEST_DATA at a
#     real-but-unpopulated path skipped the guard entirely and fell through into
#     the suites, where some fail confusingly and the nested ones (15, 16)
#     silently print "(no nested fixture)" and pass.
# So: check for the actual files, report every one that is missing rather than
# just the first, and exit through the same failure terminator every other
# failure in this script uses.
MISSING=""
require_file() { [ -f "$1" ] || MISSING="$MISSING$1"$'\n'; }
require_glob() {
  # shellcheck disable=SC2086
  set -- $1
  [ -f "$1" ] || MISSING="$MISSING$1 (no match)"$'\n'
}
require_file "$DATA/nyc_taxi.parquet"          # suites 2, 4
require_file "$DATA/hits_0.parquet"            # suite 2
require_glob "$DATA/conf/*.parquet"            # suite 2
require_glob "$DATA/conf/v2page_*.parquet"     # suites 3, 12, 14
if [ -n "$MISSING" ]; then
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "!! PARQUET SUITE ABORTED: required test fixtures are missing."
  echo "!! NO TESTS WERE RUN. This is a FAILURE, not a skip."
  echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
  echo "fixture directory: $DATA"
  if [ ! -d "$DATA" ]; then
    echo "  (that directory does not exist)"
  fi
  echo "missing:"
  # Quoted / line-oriented: an unquoted $MISSING word-splits the "(no match)"
  # annotations across separate output lines.
  printf '%s' "$MISSING" | while IFS= read -r m; do echo "  $m"; done
  echo
  echo "Point PARQUET_TEST_DATA at a populated fixture directory, or pass the"
  echo "path as \$1. See docs/dev/parquet-storage-format.md section 9.3 for how"
  echo "these files are obtained."
  echo
  echo "==================================="
  echo "PARQUET SUITE: FAILURES"
  exit 1
fi

# Optional fixtures. Absent ones downgrade a suite to a skip, which is
# legitimate -- but a skip must be *counted*, so that a run which quietly did
# less than the full suite cannot be read as a clean full-suite pass.
SKIPPED=0

echo "### build ###"
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/pq_meta_t $S/parquet_metadata.cc $S/test_parquet_metadata.cc || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined \
    -o /tmp/pq_lvl_t  $S/parquet_metadata.cc $S/page_header.cc $S/test_levels.cc || FAIL=1
g++ -std=c++20 -O2 -Wall -Wextra -Wpedantic \
    -o /tmp/pq_write_t $S/parquet_writer.cc $S/parquet_metadata.cc $S/encryption.cc \
       $S/test_writer.cc -lzstd -llz4 -lcrypto || FAIL=1
g++ -std=c++20 -O2 -Wall -Wextra -Wpedantic -I. \
    -o /tmp/pq_nested_write_t $S/test_nested_write.cc $S/parquet_writer.cc \
       $S/parquet_metadata.cc $S/encryption.cc -lzstd -llz4 -lcrypto || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. \
    -o /tmp/pq_nested_read_t $S/test_nested_read.cc $S/parquet_reader.cc \
       $S/parquet_metadata.cc $S/page_header.cc $S/encryption.cc -lzstd -lsnappy -llz4 -lcrypto || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. \
    -o /tmp/pq_levels_tree_t $S/test_levels_tree.cc $S/parquet_metadata.cc || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. \
    -o /tmp/pq_rowrange_t $S/test_row_range.cc $S/parquet_reader.cc $S/parquet_metadata.cc \
       $S/page_header.cc $S/encryption.cc -lzstd -lsnappy -llz4 -lcrypto || FAIL=1
# The shred/reassemble matrix now lives at test/unit/parquet_shred_test.cc, so that
# CI runs it too (configure.py target test/unit/parquet_shred_test, one case per
# subcommand in test/unit/test_config.yaml). It is still built and run from here --
# there is one copy of the matrix, not two -- but its includes are now repo-root
# relative like the rest of the tree, hence -I../.. alongside the -I. that the
# format headers' sibling-relative includes need.
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. -I../.. \
    -o /tmp/pq_shred_t schema_mapping.cc $S/parquet_writer.cc $S/parquet_metadata.cc \
       $S/page_header.cc $S/parquet_reader.cc $S/encryption.cc \
       ../../test/unit/parquet_shred_test.cc \
       -lzstd -lsnappy -llz4 -lcrypto || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I../.. \
    -o /tmp/pq_tier_t tiering_policy.cc test_tiering.cc -lfmt || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. -I../.. \
    -o /tmp/pq_oi_t $S/test_offset_index.cc $S/parquet_reader.cc $S/parquet_metadata.cc \
       $S/page_header.cc $S/encryption.cc -lzstd -lsnappy -llz4 -lcrypto || FAIL=1
# Every target that links parquet_reader.cc also needs encryption.cc: reading an encrypted footer
# is part of the reader now (c690d33683), so the four targets above would otherwise fail to link
# and take the whole suite's build down with them.
# Modular encryption. Two directions, and both are needed: the conformance test decrypts files
# written by parquet-cpp, the writer test produces files for pyarrow to open. Either one alone
# would only prove we agree with ourselves.
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. -I../.. \
    -o /tmp/pq_enc_t $S/encryption.cc $S/parquet_metadata.cc $S/test_encryption.cc \
       -lcrypto || FAIL=1
g++ -std=c++20 -O2 -Wall -Wextra -I. -I../.. \
    -o /tmp/pq_encw_t $S/parquet_writer.cc $S/parquet_metadata.cc $S/encryption.cc \
       $S/page_header.cc $S/parquet_reader.cc $S/test_encrypt_write.cc \
       -lzstd -lsnappy -llz4 -lcrypto || FAIL=1
g++ -std=c++20 -O1 -Wall -Wextra -Wpedantic -fsanitize=address,undefined -I. -I../.. \
    -o /tmp/pq_xread_t $S/test_crossread.cc $S/parquet_reader.cc $S/parquet_metadata.cc \
       $S/page_header.cc $S/encryption.cc -lzstd -lsnappy -llz4 -lcrypto || FAIL=1
for f in $S/parquet_metadata.cc $S/page_header.cc $S/parquet_writer.cc $S/parquet_reader.cc schema_mapping.cc tiering_policy.cc; do
  # -Werror + -Wunused-private-field mirrors the in-tree Scylla build, which is
  # stricter than the gcc invocations above.
  clang++ -std=c++20 -O2 -Wall -Wextra -Wpedantic -Werror -Wunused-private-field \
          -I. -I../.. -c $f -o /tmp/clang_chk.o || FAIL=1
done

echo; echo "### 1. RLE/bit-packed round-trip ###";            /tmp/pq_lvl_t roundtrip || FAIL=1
echo; echo "### 2. footer conformance vs pyarrow ###"
python3 $S/conformance.py /tmp/pq_meta_t $DATA/nyc_taxi.parquet $DATA/hits_0.parquet $DATA/conf/*.parquet || FAIL=1
echo; echo "### 3. real V2 page level decode ###"
for f in $DATA/conf/v2page_*.parquet; do /tmp/pq_lvl_t levels "$f" || FAIL=1; done
echo; echo "### 4. footer fuzz / corruption ###";              /tmp/pq_meta_t fuzz $DATA/nyc_taxi.parquet | tail -11 || FAIL=1
echo; echo "### 5. writer -> pyarrow interop ###"
mkdir -p $DATA/wout && /tmp/pq_write_t emit $DATA/wout >/dev/null && \
  python3 $S/writer_interop.py $DATA/wout || FAIL=1
echo; echo "### 6. folding round-trip (losslessness) ###";     /tmp/pq_shred_t roundtrip || FAIL=1
echo; echo "### 7. divergence cost curve ###";                 /tmp/pq_shred_t cost || FAIL=1
echo; echo "### 8. hybrid tiering policy (C1, C5, C6) ###";         /tmp/pq_tier_t || FAIL=1
echo; echo "### 19. modular encryption: decrypt parquet-cpp's files ###"
# $DATA, not a hardcoded $HOME: otherwise pointing PARQUET_TEST_DATA at a CI
# path still silently read the lab's home directory, and this suite claimed to
# have tested fixtures that were not the ones under test.
if [ -d "$DATA/enc_ref" ]; then
  /tmp/pq_enc_t "$DATA/enc_ref" || FAIL=1
else
  echo "SKIP -- no reference files; generate with \$PARQUET_TEST_DATA/enc_ref/gen.py"
  SKIPPED=$((SKIPPED + 1))
fi
echo; echo "### 20. modular encryption: pyarrow reads what we write ###"
ENCOUT=$(mktemp -d)
if /tmp/pq_encw_t "$ENCOUT" >/dev/null; then
  python3 "$S/test_encrypt_interop.py" "$ENCOUT" || FAIL=1
else
  echo "encrypted writer FAILED"; FAIL=1
fi
rm -rf "$ENCOUT"
echo; echo "### 9. file round-trip: rows -> parquet -> rows ###"; /tmp/pq_shred_t filetrip || FAIL=1
echo; echo "### 10. OffsetIndex: row -> page lookup ###"
/tmp/pq_oi_t $DATA/wout/*.parquet || FAIL=1
echo; echo "### 11. L3 logical export (lossy, export-only) ###"; /tmp/pq_shred_t logical || FAIL=1
echo; echo "### 13. schema recovery from the file alone ###"; /tmp/pq_shred_t recovery || FAIL=1
echo; echo "### 13a. pre-fold files still read (per-column __ldt) ###"; /tmp/pq_shred_t legacy || FAIL=1
echo; echo "### 18. non-frozen collections, in memory and through a file ###"
/tmp/pq_shred_t collections || FAIL=1
echo; echo "### 15. Dremel levels from the schema tree, vs pyarrow ###"
if [ -f "$DATA"/nested/nested.parquet ]; then
  /tmp/pq_levels_tree_t "$DATA"/nested/nested.parquet "$DATA"/nested/nested.levels.json || FAIL=1
  echo; echo "### 16. read a nested list column, vs pyarrow ###"
  /tmp/pq_nested_read_t "$DATA"/nested/nested.parquet "$DATA"/nested/nested.tags.txt \
      tags.list.element || FAIL=1
else
  echo "SKIP -- no nested fixture; generate with $S/gen_nested.py"
  SKIPPED=$((SKIPPED + 2))   # suites 15 and 16
fi

echo; echo "### 17. write a nested list column, vs pyarrow ###"
mkdir -p "$DATA"/wnest
if /tmp/pq_nested_write_t "$DATA"/wnest; then
  python3 $S/writer_nested_interop.py "$DATA"/wnest || FAIL=1
  # and our own reader must agree with what pyarrow saw
  /tmp/pq_nested_read_t "$DATA"/wnest/w_nested.parquet "$DATA"/wnest/w_nested.tags.txt \
      tags.list.element || FAIL=1
else
  FAIL=1
fi
echo; echo "### 14. read_row_range == read_row_group, sliced ###"
/tmp/pq_rowrange_t "$DATA"/wout/*.parquet "$DATA"/conf/v2page_*.parquet || FAIL=1
echo; echo "### 12. cross-read: parquet-cpp files, values vs pyarrow ###"
python3 $S/crossread.py /tmp/pq_xread_t $DATA/conf/v2page_*.parquet || FAIL=1

echo; echo "==================================="
echo "fixture directory: $DATA"
if [ $FAIL -ne 0 ]; then
  echo "PARQUET SUITE: FAILURES"
elif [ $SKIPPED -ne 0 ]; then
  # Deliberately not the "ALL PASS" string: a run that skipped suites must not
  # match a grep for the clean-run marker.
  echo "PARQUET SUITE: PASS WITH $SKIPPED SKIPPED SUITE(S) -- not a full run"
else
  echo "PARQUET SUITE: ALL PASS"
fi
exit $FAIL
