#!/bin/bash -e

set -o pipefail

orig="$1"
stripped="$orig.stripped"
debuginfo="$orig.debug"

# generate stripped binary and debuginfo
cp -a "$orig" "$stripped"
gdb-add-index "$stripped"
objcopy --merge-notes "$stripped"
eu-strip --remove-comment -f "$debuginfo" "$stripped"

# generate minidebug (minisymtab)
dynsyms="$orig.dynsyms"
funcsyms="$orig.funcsyms"
keep_symbols="$orig.keep_symbols"
mini_debuginfo="$orig.minidebug"
remove_sections=`readelf -W -S "$debuginfo" | awk '{ if (index($2,".debug_") != 1 && ($3 == "PROGBITS" || $3 == "NOTE" || $3 == "NOBITS") && index($8,"A") == 0) printf "--remove-section "$2" " }'`
nm -D "$stripped" --format=posix --defined-only | awk '{ print $1 }' | sort > "$dynsyms"
nm "$debuginfo" --format=posix --defined-only | awk '{ if ($2 == "T" || $2 == "t" || $2 == "D") print $1 }' | sort > "$funcsyms"
comm -13 "$dynsyms" "$funcsyms" > "$keep_symbols"
objcopy -S $remove_sections --keep-symbols="$keep_symbols" "$debuginfo" "$mini_debuginfo"
xz -f --threads=0 "$mini_debuginfo"
objcopy --add-section .gnu_debugdata="$mini_debuginfo".xz "$stripped"
