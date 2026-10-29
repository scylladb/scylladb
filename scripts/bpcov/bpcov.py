#!/usr/bin/env python3
#
# Copyright (C) 2026-present ScyllaDB
#

#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
#

"""
Breakpoint coverage: which lines of Scylla's source code did a run execute?

This works on an ordinary optimized build, without coverage instrumentation.
It only needs the line-number table, which dev mode has when configured with
"./configure.py --line-tables" (release and debug builds have it unless
configured with --debuginfo 0).
The program runs at full speed, except for one trap the first time each line
is reached; see tracer.c.

Typical use with test/alternator/run:

    SCYLLA=scripts/bpcov/scylla test/alternator/run
    scripts/bpcov/bpcov.py less alternator/executor.cc

All commands work on build/dev/scylla, unless given another binary with --binary.
See README.md in this directory for more.

"prepare" reads the line table of the binary, and picks a breakpoint address
for every place where the code of a line of a Scylla source file (not of
Seastar, abseil, fmt, generated code or system headers) begins, and also at the
start of the code of every function inlined into such a line (see prepare.c). Its
results are kept in the bpcov/ directory next to the binary (e.g.,
build/dev/bpcov/), whose "info" file says which binary and build they are for,
so it only does the work once per build. Like the build directory, bpcov/ only
keeps the latest build: preparing a new build replaces the data of the old one.
The scripts/bpcov/scylla wrapper prepares when needed, so running "prepare"
yourself is optional - it only moves the work, a few seconds, out of the first
boot of a new build.

The scripts/bpcov/scylla wrapper runs the binary under the tracer, which records
the breakpoints hit in the hits/ subdirectory. Several runs add up, until
"report --reset" or a rebuild. "cat FILE" prints a source file with "* " before
each line whose code never ran, and "less FILE" shows the same in less. "report"
prints a summary and writes an lcov .info file and, with --html, an HTML report.

The results are approximate in the ways the optimizer makes line tables
approximate: a line counts as executed if any copy of its code (it may be
inlined in many places) was executed, and a line without code of its own in
the binary - for example, code the compiler removed or merged with another
line's - does not appear in the report at all.
"""

import argparse
import collections
import fcntl
import os
import shutil
import signal
import struct
import subprocess
import sys

import numpy as np

SOURCE_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# Increase when "prepare" changes what it computes, or the format of the data
# files, so that data prepared by an older version is prepared again.
DATA_VERSION = 5

# The records of rows.bin (see prepare.c).
ROW_DTYPE = np.dtype([('addr', '<u8'), ('file', '<u4'), ('line', '<u4')])
# The values in a hits file (see tracer.c).
HIT, HIT_IN_FORK = 1, 3


def build_id(binary: str) -> str:
    """
    Return the GNU build-id of a 64-bit little-endian ELF executable: a hash of
    its contents, which the linker writes into a note in a PT_NOTE segment.
    """
    with open(binary, 'rb') as f:
        elf = f.read(64)
        if elf[:4] != b'\x7fELF' or elf[4] != 2 or elf[5] != 1:
            sys.exit(f'{binary} is not a 64-bit little-endian ELF file')
        phoff, = struct.unpack_from('<Q', elf, 32)
        phentsize, phnum = struct.unpack_from('<HH', elf, 54)
        f.seek(phoff)
        phdrs = f.read(phentsize * phnum)
        for i in range(phnum):
            p_type, _, p_offset, _, _, p_filesz = struct.unpack_from('<IIQQQQ', phdrs, i * phentsize)
            if p_type != 4:  # PT_NOTE
                continue
            f.seek(p_offset)
            notes = f.read(p_filesz)
            pos = 0
            while pos + 12 <= len(notes):
                namesz, descsz, n_type = struct.unpack_from('<III', notes, pos)
                name_start = pos + 12
                desc_start = name_start + (namesz + 3) // 4 * 4
                if n_type == 3 and notes[name_start:name_start + namesz] == b'GNU\0':  # NT_GNU_BUILD_ID
                    return notes[desc_start:desc_start + descsz].hex()
                pos = desc_start + (descsz + 3) // 4 * 4
    sys.exit(f'{binary} has no build-id')


def data_dir(binary: str) -> str:
    """Where the data of the binary is kept: the bpcov/ directory next to it."""
    return os.path.join(os.path.dirname(os.path.realpath(binary)), 'bpcov')


def expected_info(binary: str) -> dict[str, str]:
    """What the "info" file in the data directory says when the data is of this binary."""
    return {'binary': os.path.realpath(binary), 'build-id': build_id(binary), 'version': str(DATA_VERSION)}


def read_info(d: str) -> dict[str, str]:
    try:
        with open(os.path.join(d, 'info')) as f:
            return dict(line.rstrip('\n').split(' ', 1) for line in f if ' ' in line)
    except FileNotFoundError:
        return {}


def prepare(binary: str) -> tuple[str, bool]:
    """
    Compute the breakpoints for the binary, unless the data directory already
    has them. Returns the data directory, and whether it prepared anything.
    Prints nothing, unless something fails: when run by the wrapper, our output
    is the binary's output, which tests may read (e.g., expecting it to start
    with Scylla's first log line).
    """
    d = data_dir(binary)
    expected = expected_info(binary)
    if read_info(d) == expected:
        return d, False
    os.makedirs(d, exist_ok=True)
    # Several runs of a new build may start at the same time, but only one
    # should prepare. The others wait for it, and then use its data.
    with open(os.path.join(d, 'lock'), 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if read_info(d) == expected:
            return d, False
        # Whatever is here is of another build (or binary), or of an older
        # version of bpcov - including its runs, which are useless now.
        for name in os.listdir(d):
            if name != 'lock':
                path = os.path.join(d, name)
                shutil.rmtree(path) if os.path.isdir(path) and not os.path.islink(path) else os.unlink(path)
        os.makedirs(os.path.join(d, 'hits'))
        subprocess.check_call([tool_path('prepare'), binary, SOURCE_DIR, d])
        # Written last, as it says that the preparation is complete.
        with open(os.path.join(d, 'info.tmp'), 'w') as f:
            f.writelines(f'{k} {v}\n' for k, v in expected.items())
        os.rename(os.path.join(d, 'info.tmp'), os.path.join(d, 'info'))
    return d, True


def tool_path(name: str) -> str:
    """Build the tool scripts/bpcov/<name>.c if it is missing or older than its source, and return its path."""
    src = os.path.join(SOURCE_DIR, 'scripts', 'bpcov', f'{name}.c')
    exe = os.path.join(SOURCE_DIR, 'build', f'bpcov-{name}')
    if not os.path.exists(exe) or os.path.getmtime(exe) < os.path.getmtime(src):
        os.makedirs(os.path.dirname(exe), exist_ok=True)
        # Build under a temporary name, as other runs may be using or building it.
        tmp = f'{exe}.{os.getpid()}'
        # prepare.c decompresses compressed debug sections (-gz), with the
        # zstd and zlib libraries which Scylla's build needs anyway.
        libs = ['-lzstd', '-lz'] if name == 'prepare' else []
        subprocess.check_call(['cc', '-O2', '-Wall', '-o', tmp, src] + libs)
        os.rename(tmp, exe)
    return exe


def run(binary: str, args: list[str]):
    """Replace this process with the tracer running the binary with these arguments."""
    binary = os.path.realpath(binary)
    d, _ = prepare(binary)
    tracer = tool_path('tracer')
    hits = os.path.join(d, 'hits', f'{os.getpid()}.bin')
    os.execv(tracer, [tracer, os.path.join(d, 'breakpoints.bin'), hits, binary] + args)


def load_coverage(binary: str):
    """
    Combine the hits of all runs of this build so far. Returns the data directory,
    the list of hit files, the list of source files, and three arrays with one
    element per source line that has code: its file number (index into the list
    of source files), its line number, and whether it was executed. The arrays are
    sorted by file and line.
    """
    d = data_dir(binary)
    if read_info(d) != expected_info(binary):
        sys.exit(f'No coverage data for this build of {binary}: run it with scripts/bpcov/scylla first')
    rows = np.fromfile(os.path.join(d, 'rows.bin'), dtype=ROW_DTYPE)
    breakpoints = np.fromfile(os.path.join(d, 'breakpoints.bin'), dtype='<u8')
    with open(os.path.join(d, 'files.txt')) as f:
        files = f.read().splitlines()
    hit_files = [os.path.join(d, 'hits', name) for name in os.listdir(os.path.join(d, 'hits')) if name.endswith('.bin')]
    # Which breakpoints any of the runs hit.
    hit = np.zeros(len(breakpoints), dtype=bool)
    for h in hit_files:
        status = np.fromfile(h, dtype=np.uint8)[:len(breakpoints)]
        hit[:len(status)] |= (status == HIT) | (status == HIT_IN_FORK)

    # A line was executed if any of its breakpoints was hit.
    keys = (rows['file'].astype(np.uint64) << 32) | rows['line']
    covered = hit[np.searchsorted(breakpoints, rows['addr'])]
    keys, inverse = np.unique(keys, return_inverse=True)
    line_covered = np.zeros(len(keys), dtype=bool)
    np.logical_or.at(line_covered, inverse, covered)
    key_files = (keys >> 32).astype(np.int64)
    key_lines = (keys & 0xffffffff).astype(np.int64)
    return d, hit_files, files, key_files, key_lines, line_covered


def cat(binary: str, path: str, gcov: bool):
    """
    Print a source file, with '* ' before each line whose code never ran. With gcov=True,
    use gcov's format instead: '#####' marks lines whose code never ran, '1' lines that
    ran, and '-' lines without code.
    """
    d, hit_files, files, key_files, key_lines, line_covered = load_coverage(binary)
    rel = os.path.relpath(os.path.realpath(path), SOURCE_DIR) if os.path.exists(path) else os.path.normpath(path)
    matches = [i for i, f in enumerate(files) if f == rel] or [i for i, f in enumerate(files) if f.endswith('/' + rel)]
    if len(matches) != 1:
        sys.exit(f'{path} matches {len(matches)} measured source files' +
                 (': ' + ' '.join(files[i] for i in matches[:10]) if matches else ''))
    f = matches[0]
    sel = key_files == f
    status = dict(zip(key_lines[sel].tolist(), line_covered[sel].tolist()))
    color = sys.stdout.isatty()
    with open(os.path.join(SOURCE_DIR, files[f]), errors='replace') as src:
        if not gcov:
            for n, text in enumerate(src, 1):
                print(('* ' if status.get(n) is False else '  ') + text, end='')
            return
        print(f'{"-":>9}:{0:>5}:Source:{files[f]}')
        print(f'{"-":>9}:{0:>5}:Runs:{len(hit_files)}')
        for n, text in enumerate(src, 1):
            text = text.rstrip('\n')
            if n not in status:
                print(f'{"-":>9}:{n:>5}:{text}')
            elif status[n]:
                print(f'{"1":>9}:{n:>5}:{text}')
            elif color:
                print(f'\033[31m{"#####":>9}:{n:>5}:{text}\033[0m')
            else:
                print(f'{"#####":>9}:{n:>5}:{text}')


def less(binary: str, path: str):
    """Show cat()'s output in less, starting at the first line whose code never ran."""
    r, w = os.pipe()
    pager = subprocess.Popen(['less', '+/^\\* '], stdin=r)
    os.close(r)
    with os.fdopen(w, 'w') as out:
        sys.stdout = out
        try:
            cat(binary, path, False)
        except BrokenPipeError:
            pass
        finally:
            sys.stdout = sys.__stdout__
    pager.wait()


def report(binary: str, prefixes: list[str], html: str | None, reset: bool):
    d, hit_files, files, key_files, key_lines, line_covered = load_coverage(binary)
    print(f'{len(hit_files)} runs')

    if prefixes:
        keep = np.array([any(files[f].startswith(p) for p in prefixes) for f in range(len(files))])
        sel = keep[key_files]
        key_files, key_lines, line_covered = key_files[sel], key_lines[sel], line_covered[sel]

    by_dir = collections.defaultdict(lambda: [0, 0])
    total = [0, 0]
    info = os.path.join(d, 'coverage.info')
    with open(info, 'w') as out:
        # keys were sorted by file then line, so each file's lines are contiguous.
        boundaries = np.flatnonzero(np.diff(key_files)) + 1
        for start, end in zip(np.r_[0, boundaries], np.r_[boundaries, len(key_files)]):
            if start == end:
                continue
            path = files[key_files[start]]
            n_hit = int(line_covered[start:end].sum())
            n = int(end - start)
            top = path.split('/')[0] if '/' in path else '.'
            by_dir[top][0] += n_hit
            by_dir[top][1] += n
            total[0] += n_hit
            total[1] += n
            out.write(f'SF:{os.path.join(SOURCE_DIR, path)}\n')
            for line, c in zip(key_lines[start:end], line_covered[start:end]):
                out.write(f'DA:{line},{int(c)}\n')
            out.write(f'LH:{n_hit}\nLF:{n}\nend_of_record\n')

    width = max(len(k) for k in by_dir)
    for top, (n_hit, n) in sorted(by_dir.items()):
        print(f'{top:<{width}} {n_hit:>8} / {n:<8} {100 * n_hit / n:5.1f}%')
    print(f'{"total":<{width}} {total[0]:>8} / {total[1]:<8} {100 * total[0] / max(total[1], 1):5.1f}%')
    print(f'lcov data: {info}')
    if html:
        shutil.rmtree(html, ignore_errors=True)
        subprocess.check_call(['genhtml', '--quiet', '--ignore-errors', 'source', '--prefix', SOURCE_DIR,
                               '--output-directory', html, info])
        print(f'HTML report: {os.path.join(html, "index.html")}')
    if reset:
        for h in hit_files:
            os.unlink(h)
            if os.path.exists(h + '.log'):
                os.unlink(h + '.log')


def main():
    # Python ignores SIGPIPE, which would make "bpcov.py cat ... | head" fail
    # with a BrokenPipeError, and the binary run under the tracer inherit the
    # ignored SIGPIPE. Restore the default.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    # Not through argparse, which could mistake the binary's options for ours.
    if len(sys.argv) >= 3 and sys.argv[1] == 'run':
        run(sys.argv[2], sys.argv[3:])
    parser = argparse.ArgumentParser(description='Breakpoint coverage of a Scylla binary')
    sub = parser.add_subparsers(dest='command', required=True)
    default_binary = os.path.join(SOURCE_DIR, 'build', 'dev', 'scylla')
    p = sub.add_parser('prepare', help='compute the breakpoints for a binary (done once per build)')
    p.add_argument('--binary', default=default_binary, help='default: build/dev/scylla')
    p = sub.add_parser('run', help='run the binary under the tracer: run BINARY [ARGS...]')
    p = sub.add_parser('cat', help="print a source file, marking with '* ' the lines whose code never ran")
    p.add_argument('--binary', default=default_binary, help='default: build/dev/scylla')
    p.add_argument('--gcov', action='store_true', help="gcov's format: '#####' never ran, '1' ran, '-' no code")
    p.add_argument('file', help='source file, e.g. alternator/executor.cc')
    p = sub.add_parser('less', help="like cat, but in less, starting at the first line whose code never ran ('n' for the next)")
    p.add_argument('--binary', default=default_binary, help='default: build/dev/scylla')
    p.add_argument('file', help='source file, e.g. alternator/executor.cc')
    p = sub.add_parser('report', help='summarize the coverage of all runs so far')
    p.add_argument('--binary', default=default_binary, help='default: build/dev/scylla')
    p.add_argument('--prefix', action='append', default=[],
                   help='only report source files starting with this prefix, e.g. alternator/ (can be repeated)')
    p.add_argument('--html', metavar='DIR', help='also write an HTML report to this directory')
    p.add_argument('--reset', action='store_true', help='forget the runs afterwards')
    args = parser.parse_args()
    if args.command == 'prepare':
        d, prepared = prepare(args.binary)
        print(f'Prepared {d}' if prepared else f'{d} was already prepared for this build')
    elif args.command == 'cat':
        cat(args.binary, args.file, args.gcov)
    elif args.command == 'less':
        less(args.binary, args.file)
    elif args.command == 'report':
        report(args.binary, args.prefix, args.html, args.reset)


if __name__ == '__main__':
    main()
