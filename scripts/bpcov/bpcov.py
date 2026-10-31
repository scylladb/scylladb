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
is reached; see preload.c (and tracer.c for the alternative ptrace tracer).

Typical use with test/alternator/run, which runs one Scylla executable:

    SCYLLA=scripts/bpcov/scylla test/alternator/run
    scripts/bpcov/bpcov.py less alternator/executor.cc

and with test.py, which runs many test executables, many times each:

    scripts/bpcov/bpcov.py exec ./test.py --mode=dev test/boost
    scripts/bpcov/bpcov.py report

The commands that show coverage combine all the executables of build/dev that
were measured, unless given another directory with --dir, or one executable
with --binary. See README.md in this directory for more.

"prepare" reads the line table of an executable, and picks a breakpoint address
for every place where the code of a line of a Scylla source file (not of
Seastar, abseil, fmt, generated code or system headers) begins, and also at the
start of the code of every function inlined into such a line (see prepare.c). Its
results are kept in build/dev/bpcov/, in a directory for each executable (e.g.,
build/dev/bpcov/scylla/ for build/dev/scylla), whose "info" file says which
build they are for, so it only does the work once per build. Like the build
directory, bpcov/ only keeps the latest build: preparing a new build replaces
the data of the old one. Executables are prepared when they first run under
bpcov, so running "prepare" yourself is optional.

The scripts/bpcov/scylla wrapper, and "exec", run executables under the tracer,
which records the breakpoints hit in the executable's "hits" file. All the runs
of an executable share this file, and add up, until "report --reset" or a
rebuild; a run traps only on code that no earlier run reached. "cat FILE"
prints a source file with "* " before each line whose code never ran, and "less
FILE" shows the same in less. "report" prints a summary and writes an lcov .info
file and, with --html, an HTML report.

The results are approximate in the ways the optimizer makes line tables
approximate: a line counts as executed if any copy of its code (it may be
inlined in many places) was executed, and a line without code of its own in
the binary - for example, code the compiler removed or merged with another
line's - does not appear in the report at all.
"""

import argparse
import array
import bisect
import collections
import fcntl
import itertools
import os
import shutil
import signal
import struct
import subprocess
import sys
import time

SOURCE_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..'))
# Increase when "prepare" changes what it computes, or the format of the data
# files, so that data prepared by an older version is prepared again.
DATA_VERSION = 7

# The values in a hits file (see tracer.c).
HIT = 1
DEFAULT_DIR = os.path.join(SOURCE_DIR, 'build', 'dev')


def build_id(binary: str) -> str:
    """
    Return the GNU build-id of a 64-bit little-endian ELF executable: a hash of
    its contents, which the linker writes into a note in a PT_NOTE segment.
    """
    with open(binary, 'rb') as f:
        elf = f.read(64)
        if elf[:4] != b'\x7fELF' or elf[4] != 2 or elf[5] != 1:
            raise ValueError(f'{binary} is not a 64-bit little-endian ELF file')
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
    raise ValueError(f'{binary} has no build-id')


def default_dir(binary: str) -> str:
    """
    The directory whose executables are measured together, and which keeps their
    data in its bpcov/ subdirectory: build/MODE for an executable under it (e.g.,
    build/dev/scylla or build/dev/test/boost/combined_tests), otherwise the
    executable's own directory.
    """
    binary = os.path.realpath(binary)
    rel = os.path.relpath(binary, os.path.realpath(os.path.join(SOURCE_DIR, 'build')))
    if not rel.startswith('..') and os.sep in rel:
        return os.path.realpath(os.path.join(SOURCE_DIR, 'build', rel.split(os.sep)[0]))
    return os.path.dirname(binary)


def data_dir(binary: str, directory: str) -> str:
    """Where the data of the binary is kept: DIRECTORY/bpcov/PATH, for the binary DIRECTORY/PATH."""
    return os.path.join(directory, 'bpcov', os.path.relpath(os.path.realpath(binary), directory))


def expected_info(binary: str) -> dict[str, str]:
    """What the "info" file in the data directory says when the data is of this binary."""
    return {'binary': os.path.realpath(binary), 'build-id': build_id(binary), 'version': str(DATA_VERSION)}


def info_matches(info: dict[str, str], expected: dict[str, str]) -> bool:
    """Whether the data is of this binary - prepared, or found to be impossible to prepare (an "error")."""
    return all(info.get(k) == v for k, v in expected.items())


def read_info(d: str) -> dict[str, str]:
    try:
        with open(os.path.join(d, 'info')) as f:
            return dict(line.rstrip('\n').split(' ', 1) for line in f if ' ' in line)
    except FileNotFoundError:
        return {}


def prepare(binary: str, directory: str, quiet: bool = False) -> tuple[str, bool]:
    """
    Compute the breakpoints for the binary, unless the data directory already
    has them. Returns the data directory, and whether it prepared anything.
    Prints nothing, unless something fails: when run by the wrapper, our output
    is the binary's output, which tests may read (e.g., expecting it to start
    with Scylla's first log line).

    If the binary can't be measured - e.g., it has no line tables - the "info"
    file says so in an "error" line, so that its runs don't try again. Then we
    exit with the error, unless quiet: bpcov-preload.so runs us for every
    executable under the measured directory, and those that can't be measured
    should just run unmeasured; and "ninja dev-bpcov-prepare" prepares all the
    executables, and shouldn't fail because of one.
    """
    d = data_dir(binary, directory)
    expected = expected_info(binary)
    if not info_matches(read_info(d), expected):
        os.makedirs(d, exist_ok=True)
        # Several runs of a new build may start at the same time, but only one
        # should prepare. The others wait for it, and then use its data.
        with open(os.path.join(d, 'lock'), 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if not info_matches(read_info(d), expected):
                prepare_locked(binary, d, expected)
                prepared = True
            else:
                prepared = False
    else:
        prepared = False
    error = read_info(d).get('error')
    if error and not quiet:
        sys.exit(f'bpcov cannot measure {binary}: {error}')
    return d, prepared


def prepare_locked(binary: str, d: str, expected: dict[str, str]):
    start = time.monotonic()
    # Whatever is here is of another build (or binary), or of an older
    # version of bpcov - including its hits, which are useless now.
    for name in os.listdir(d):
        if name not in ('lock', 'prepare.log'):
            path = os.path.join(d, name)
            shutil.rmtree(path) if os.path.isdir(path) and not os.path.islink(path) else os.unlink(path)
    info = dict(expected)
    r = subprocess.run([tool_path('prepare'), binary, SOURCE_DIR, d], stderr=subprocess.PIPE, text=True)
    if r.returncode != 0:
        info['error'] = ' '.join(r.stderr.split()) or f'bpcov-prepare failed with status {r.returncode}'
    else:
        # The runs' hits file, which they extend if it is missing, but then
        # several runs starting together would race to create it.
        with open(os.path.join(d, 'hits'), 'wb') as f:
            f.truncate(os.path.getsize(os.path.join(d, 'breakpoints.bin')) // 4)
    # Written last, as it says that the preparation is complete.
    with open(os.path.join(d, 'info.tmp'), 'w') as f:
        f.writelines(f'{k} {v}\n' for k, v in info.items())
    os.rename(os.path.join(d, 'info.tmp'), os.path.join(d, 'info'))
    with open(os.path.join(d, 'prepare.log'), 'a') as f:
        f.write(f'{time.strftime("%Y-%m-%d %H:%M:%S")}: prepared build-id {info["build-id"]} '
                f'in {time.monotonic() - start:.2f} seconds' + (' (cannot be measured)' if 'error' in info else '') + '\n')


def tool_path(name: str) -> str:
    """Build the tool scripts/bpcov/<name>.c if it is missing or older than its source, and return its path."""
    src = os.path.join(SOURCE_DIR, 'scripts', 'bpcov', f'{name}.c')
    # preload.c is a library for LD_PRELOAD, the others are programs.
    library = name == 'preload'
    exe = os.path.join(SOURCE_DIR, 'build', f'bpcov-{name}' + ('.so' if library else ''))

    def outdated():
        return not os.path.exists(exe) or os.path.getmtime(exe) < os.path.getmtime(src)

    if outdated():
        os.makedirs(os.path.dirname(exe), exist_ok=True)
        # Many runs may start at the same time (e.g., the preparations of
        # "ninja dev-bpcov-prepare"): one builds, the others wait for it.
        with open(f'{exe}.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            if outdated():
                # Build under a temporary name, as other runs may be using it.
                tmp = f'{exe}.{os.getpid()}'
                # prepare.c decompresses compressed debug sections (-gz), with the
                # zstd and zlib libraries which Scylla's build needs anyway.
                libs = ['-lzstd', '-lz'] if name == 'prepare' else []
                shared = ['-shared', '-fPIC'] if library else []
                subprocess.check_call(['cc', '-O2', '-Wall'] + shared + ['-o', tmp, src] + libs)
                os.rename(tmp, exe)
    return exe


def preload_env(directory: str) -> dict[str, str]:
    """The environment that makes bpcov-preload.so measure the executables under the directory."""
    env = dict(os.environ)
    preload = tool_path('preload')
    others = [p for p in env.get('LD_PRELOAD', '').split() if p != preload]
    env['LD_PRELOAD'] = ' '.join([preload] + others)
    env['BPCOV_DIR'] = os.path.realpath(directory)
    # The library runs "prepare" with this interpreter, which is known to work -
    # the command may have another python3 first in its PATH (test.py does).
    env['BPCOV_PYTHON'] = sys.executable
    env['BPCOV_PY'] = os.path.realpath(__file__)
    env['BPCOV_VERSION'] = str(DATA_VERSION)
    return env


def tracer_name() -> str:
    tracer = os.environ.get('BPCOV_TRACER') or 'preload'
    if tracer not in ('preload', 'ptrace'):
        sys.exit(f'BPCOV_TRACER should be "preload" (the default) or "ptrace", not "{tracer}"')
    return tracer


def run(binary: str, args: list[str]):
    """Replace this process with the binary, run with these arguments under a tracer.

    The tracer is the library bpcov-preload.so, which handles the breakpoints
    inside the program, or with BPCOV_TRACER=ptrace, bpcov-tracer, a separate
    process that handles them with ptrace (see README.md for when to use it).
    With bpcov-preload.so, the other executables of the same build directory
    that the binary runs are measured too.
    """
    tracer = tracer_name()
    binary = os.path.realpath(binary)
    directory = default_dir(binary)
    d, _ = prepare(binary, directory)
    if tracer == 'preload':
        os.execve(binary, [binary] + args, preload_env(directory))
    exe = tool_path('tracer')
    os.execv(exe, [exe, os.path.join(d, 'breakpoints.bin'), os.path.join(d, 'hits'), binary] + args)


def exec_command(directory: str, command: list[str]):
    """Replace this process with the command, with every executable under the directory that it runs measured."""
    if tracer_name() != 'preload':
        sys.exit('"exec" needs the LD_PRELOAD tracer: the ptrace tracer only measures one executable')
    if not os.path.isdir(directory):
        sys.exit(f'{directory} is not a directory')
    try:
        os.execvpe(command[0], command, preload_env(directory))
    except OSError as e:
        sys.exit(f'{command[0]}: {e}')


def measured(directory: str, binary: str | None) -> tuple[list[tuple[str, str]], list[str]]:
    """
    The executables to report on: binary, or all those under the directory that
    were prepared. Returns a list of (executable, data directory) pairs, and a
    list of messages about the executables that are left out.
    """
    if binary:
        return [(os.path.realpath(binary), data_dir(binary, directory))], []
    result, left_out = [], []
    root = os.path.join(directory, 'bpcov')
    for d, _, names in os.walk(root):
        if 'info' not in names:
            continue
        info = read_info(d)
        exe = info.get('binary', '')
        rel = os.path.relpath(exe, directory)
        if info.get('version') != str(DATA_VERSION) or os.path.join(root, rel) != d:
            continue  # data of an older version of bpcov
        try:
            current = build_id(exe) == info.get('build-id')
        except (OSError, ValueError):
            current = False
        if not current:
            left_out.append(f'{rel}: its data is of an older build')
        elif 'error' in info:
            left_out.append(f'{rel}: {info["error"]}')
        else:
            result.append((exe, d))
    return sorted(result), sorted(left_out)


def lines_run(d: str) -> tuple[array.array, array.array, list[bool]]:
    """
    For the data directory of one executable: the file number and line number of
    each source line with code, sorted by file and line (see prepare.c), and
    whether the line ran - whether any of its breakpoints was hit.
    """
    lines = array.array('I')
    with open(os.path.join(d, 'lines.bin'), 'rb') as f:
        lines.frombytes(f.read())
    line_breakpoints = array.array('I')
    with open(os.path.join(d, 'line_breakpoints.bin'), 'rb') as f:
        line_breakpoints.frombytes(f.read())
    n_bp = os.path.getsize(os.path.join(d, 'breakpoints.bin')) // 4
    try:
        with open(os.path.join(d, 'hits'), 'rb') as f:
            hits = f.read(n_bp).ljust(n_bp, b'\0')
    except FileNotFoundError:
        hits = bytes(n_bp)
    starts = lines[2::3]
    try:
        import numpy as np
    except ImportError:
        np = None
    if np:
        hit = np.frombuffer(hits, dtype=np.uint8) == HIT
        covered = np.logical_or.reduceat(hit[np.frombuffer(line_breakpoints, dtype=np.uint32)], starts).tolist()
    else:
        # The same, about 4 times slower: the hits of all the lines'
        # breakpoints, in the order of line_breakpoints, then any() of each
        # line's part.
        flags = bytes(map(hits.__getitem__, line_breakpoints))
        ends = itertools.chain(starts[1:], [len(flags)])
        covered = [HIT in flags[start:end] for start, end in zip(starts, ends)]
    return lines[0::3], lines[1::3], covered


def load_coverage(directory: str, binary: str | None):
    """
    Combine the hits of all the runs of the measured executables (see measured()).
    Returns the list of data directories, the number of runs, and a dict from each
    source file with code (its path relative to SOURCE_DIR) to two sets: its lines
    with code, and those of them that ran.
    """
    executables, left_out = measured(directory, binary)
    for message in left_out:
        print(f'Not measured: {message}', file=sys.stderr)
    errors = os.path.join(directory, 'bpcov', 'errors.log')
    if not binary and os.path.exists(errors) and os.path.getsize(errors):
        print(f'Some executables could not be prepared while they ran, see {errors}', file=sys.stderr)
    data_dirs, runs, coverage = [], 0, {}
    for exe, d in executables:
        if not info_matches(read_info(d), expected_info(exe)) or 'error' in read_info(d):
            sys.exit(f'No coverage data for this build of {exe}: run it under bpcov first')
        data_dirs.append(d)
        try:
            with open(os.path.join(d, 'hits.log')) as f:
                runs += sum(' inserted ' in line for line in f)
        except FileNotFoundError:
            pass
        with open(os.path.join(d, 'files.txt')) as f:
            exe_files = f.read().splitlines()
        files, lines, covered = lines_run(d)
        # A line ran if it ran in any of the executables.
        for i, path in enumerate(exe_files):
            # The lines of each file are together, as they are sorted by file.
            start = bisect.bisect_left(files, i)
            end = bisect.bisect_left(files, i + 1, start)
            if start == end:
                continue
            has_code, ran = coverage.setdefault(path, (set(), set()))
            has_code.update(lines[start:end])
            ran.update(itertools.compress(lines[start:end], covered[start:end]))
    if not data_dirs:
        sys.exit(f'No coverage data in {os.path.join(directory, "bpcov")}: run something under bpcov first')
    return data_dirs, runs, coverage


def cat(directory: str, binary: str | None, path: str, gcov: bool):
    """
    Print a source file, with '* ' before each line whose code never ran. With gcov=True,
    use gcov's format instead: '#####' marks lines whose code never ran, '1' lines that
    ran, and '-' lines without code.
    """
    _, runs, coverage = load_coverage(directory, binary)
    rel = os.path.relpath(os.path.realpath(path), SOURCE_DIR) if os.path.exists(path) else os.path.normpath(path)
    matches = [f for f in coverage if f == rel] or [f for f in coverage if f.endswith('/' + rel)]
    if len(matches) != 1:
        sys.exit(f'{path} matches {len(matches)} measured source files' +
                 (': ' + ' '.join(sorted(matches)[:10]) if matches else ''))
    source = matches[0]
    has_code, ran = coverage[source]
    color = sys.stdout.isatty()
    with open(os.path.join(SOURCE_DIR, source), errors='replace') as src:
        if not gcov:
            for n, text in enumerate(src, 1):
                print(('* ' if n in has_code and n not in ran else '  ') + text, end='')
            return
        print(f'{"-":>9}:{0:>5}:Source:{source}')
        print(f'{"-":>9}:{0:>5}:Runs:{runs}')
        for n, text in enumerate(src, 1):
            text = text.rstrip('\n')
            if n not in has_code:
                print(f'{"-":>9}:{n:>5}:{text}')
            elif n in ran:
                print(f'{"1":>9}:{n:>5}:{text}')
            elif color:
                print(f'\033[31m{"#####":>9}:{n:>5}:{text}\033[0m')
            else:
                print(f'{"#####":>9}:{n:>5}:{text}')


def less(directory: str, binary: str | None, path: str):
    """Show cat()'s output in less, starting at the first line whose code never ran."""
    r, w = os.pipe()
    pager = subprocess.Popen(['less', '+/^\\* '], stdin=r)
    os.close(r)
    with os.fdopen(w, 'w') as out:
        sys.stdout = out
        try:
            cat(directory, binary, path, False)
        except BrokenPipeError:
            pass
        finally:
            sys.stdout = sys.__stdout__
    pager.wait()


def report(directory: str, binary: str | None, prefixes: list[str], html: str | None, reset: bool):
    data_dirs, runs, coverage = load_coverage(directory, binary)
    print(f'{runs} runs of {len(data_dirs)} executables')

    by_dir = collections.defaultdict(lambda: [0, 0])
    total = [0, 0]
    info = os.path.join(directory, 'bpcov', 'coverage.info')
    with open(info, 'w') as out:
        for path, (has_code, ran) in sorted(coverage.items()):
            if prefixes and not any(path.startswith(p) for p in prefixes):
                continue
            n_hit = len(ran)
            n = len(has_code)
            top = path.split('/')[0] if '/' in path else '.'
            by_dir[top][0] += n_hit
            by_dir[top][1] += n
            total[0] += n_hit
            total[1] += n
            out.write(f'SF:{os.path.join(SOURCE_DIR, path)}\n')
            for line in sorted(has_code):
                out.write(f'DA:{line},{int(line in ran)}\n')
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
        # Runs that are still running keep recording into the deleted files.
        for d in data_dirs:
            for name in ('hits', 'hits.log'):
                if os.path.exists(os.path.join(d, name)):
                    os.unlink(os.path.join(d, name))


def main():
    # Python ignores SIGPIPE, which would make "bpcov.py cat ... | head" fail
    # with a BrokenPipeError, and the binary run under the tracer inherit the
    # ignored SIGPIPE. Restore the default.
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    # Not through argparse, which could mistake the binary's or command's options for ours.
    if len(sys.argv) >= 3 and sys.argv[1] == 'run':
        run(sys.argv[2], sys.argv[3:])
    if len(sys.argv) >= 2 and sys.argv[1] == 'exec':
        args = sys.argv[2:]
        directory = DEFAULT_DIR
        if len(args) >= 2 and args[0] == '--dir':
            directory, args = args[1], args[2:]
        if not args:
            sys.exit('Usage: bpcov.py exec [--dir DIR] COMMAND [ARGS...]')
        exec_command(directory, args)
    parser = argparse.ArgumentParser(description='Breakpoint coverage of Scylla executables')
    sub = parser.add_subparsers(dest='command', required=True)
    default_binary = os.path.join(SOURCE_DIR, 'build', 'dev', 'scylla')
    p = sub.add_parser('prepare', help='compute the breakpoints for an executable (done once per build)')
    p.add_argument('--binary', default=default_binary, help='default: build/dev/scylla')
    p.add_argument('--dir', help='the measured directory, whose bpcov/ keeps the data (default: build/MODE)')
    p.add_argument('--quiet', action='store_true',
                   help="print nothing, and don't fail if the executable can't be measured (its data says why)")
    p = sub.add_parser('run', help='run an executable under the tracer: run BINARY [ARGS...]')
    p = sub.add_parser('exec', help='run a command, measuring every executable under build/dev that it runs: '
                                    'exec [--dir DIR] COMMAND [ARGS...]')
    for name, text in (('cat', "print a source file, marking with '* ' the lines whose code never ran"),
                       ('less', "like cat, but in less, starting at the first line whose code never ran ('n' for the next)"),
                       ('report', 'summarize the coverage of all runs so far')):
        p = sub.add_parser(name, help=text)
        p.add_argument('--dir', default=DEFAULT_DIR,
                       help='combine the executables measured under this directory (default: build/dev)')
        p.add_argument('--binary', help='only this executable')
        if name == 'cat':
            p.add_argument('--gcov', action='store_true', help="gcov's format: '#####' never ran, '1' ran, '-' no code")
        if name in ('cat', 'less'):
            p.add_argument('file', help='source file, e.g. alternator/executor.cc')
        if name == 'report':
            p.add_argument('--prefix', action='append', default=[],
                           help='only report source files starting with this prefix, e.g. alternator/ (can be repeated)')
            p.add_argument('--html', metavar='DIR', help='also write an HTML report to this directory')
            p.add_argument('--reset', action='store_true', help='forget the runs afterwards')
    args = parser.parse_args()
    if args.command == 'prepare':
        directory = args.dir or default_dir(args.binary)
        d, prepared = prepare(args.binary, directory, args.quiet)
        if not args.quiet:
            print(f'Prepared {d}' if prepared else f'{d} was already prepared for this build')
        return
    # Unless told otherwise, look for an executable's data where its runs put it.
    directory = default_dir(args.binary) if args.binary and args.dir == DEFAULT_DIR else os.path.realpath(args.dir)
    if args.command == 'cat':
        cat(directory, args.binary, args.file, args.gcov)
    elif args.command == 'less':
        less(directory, args.binary, args.file)
    elif args.command == 'report':
        report(directory, args.binary, args.prefix, args.html, args.reset)

if __name__ == '__main__':
    main()
