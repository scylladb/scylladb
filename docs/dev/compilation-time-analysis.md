ScyllaDB's long build time has been a problem almost since its inception -
our issue #1 - https://github.com/scylladb/scylladb/issues/1 - is about it.
To reduce Scylla's compilation time, it's important to understand what is
causing it.

In the old days of of C compilers, the compilation time was mostly
proportional to the amount of source code. The only way to reduce compilation
time without deleting source code was to reduce the amount of unnecessary
header-file inclusion. However, in modern template-heavy C++ code, such
as Seastar and ScyllaDB, not all header files slow down the build by
the same amount. Template instantiation and code generation takes orders
of magnitude more time than just parsing - and it's possible that one
header file adds a millisecond to the build time, while another header
file of the same length adds a full second.

Clang has a compile-time option, "-ftime-trace", for tracing the
compilation time of a single source file, to find which parts of the
compilation took most time. This document is about how to use this
option together with a tool `ClangBuildAnalyzer` - which aggregates the
`-ftime-trace` logs from all the source files to decide which header files
and templates are slowing down build the most.

# Installing ClangBuildAnalyzer

Get ClangBuildAnalyzer:
```
git clone https://github.com/aras-p/ClangBuildAnalyzer
```
Build it with `make -f projects/make/Makefile`, and install the resulting
`build/ClangBuildAnalyzer`.

# Getting started with ClangBuildAnalyzer
In your Scylla working directory, start with a fresh "build" directory
to avoid measurement of already-deleted source files or irrelevant build
modes:
```
    rm -r build
```
Next, rebuild build.ninja to add `-ftime-trace` to the compilation commands.
Run your favorite `configure.py` command line, adding `--cflags=-ftime-trace`.
For example:

```
    ./configure.py --disable-dpdk --cflags=-ftime-trace
```

Now build the main `scylla` executable, in one build mode. For example,
```
    ninja  build/dev/scylla
```

This build will create in `build/dev`, for each source file compiled, a
**separate** JSON file with the compile-time analysis of it. For example,
for `cdc/generation.cc`, a file `build/dev/cdc/generation.json` will be
created.

These JSON files use Chrome's trace format, which can be viewed individually
in various tools including Google's https://ui.perfetto.dev/.
The ClangBuildAnalyzer tool which we installed above allows us to combine
the information from the separate source files to find the worst **overall**
offenders. Run it as:

```
    ClangBuildAnalyzer --all build/dev build/dev/clba.bin
    ClangBuildAnalyzer --analyze build/dev/clba.bin | less
```

The `-all` command merges all the ".json" files in build/dev into one
merged binary file `build/dev/clba.bin`. The "--analyze" step generates
a human-visible output.  The next section is about understanding this output,
but before that, some comments on how to iteratively use ClangBuildAnalyzer
after changing the code to see how compilation performance changed:

* Because adding "-ftime-trace" changes the command line, the first "ninja"
  build described above will need to rebuild everything, even if ccache 
  was used to already build this executable. Afterwards, ccache unfortunately
  avoids any caching while the "-ftime-trace" option is in use (this is
  documented in https://ccache.dev/releasenotes.html, only stating that
  it is "too hard").
* After changing the source code, you only need to rerun the "ninja" and
  "ClangBuildAnalyzer" steps above. The "ninja" will only recompile the
  source files that need recompiling, these source files' ".json" output
  files will change, and the "ClangBuildAnalyzer" step will use the new
  json files - in addition to those that didn't change - to build the new
  up-to-date statistics.
* If a source file is removed from the project, its json file will stay
  behind in build/dev/ and add wrong information to ClangBuildAnalyzer.
  So remove that json file manually, or clean everything up by removing
  the entire "build" directory and compiling again.
* If you no longer want "ninja" to measure build time and write the json
  file, re-run configure.py without the `--cflags=-ftime-trace`.

After making a change to the source code, it is sometimes hard to measure
the small improvement to build time because build time measurement isn't
very accurate. We noticed in the past that the build time is highly
correlated with the total produced object size:
```
    du -bc build/dev/**/*.o
```
I.e., the C++ compiler is usually slow because it generates a huge amount
of code. So this `du` output can be used as a good estimate of how much
build time we saved by a change to the code.

# Using ClangBuildAnalyzer output

This section is about how to make use of the output of
```
    ClangBuildAnalyzer --analyze build/dev/clba.bin
```

The first two sections of the output - "Files that took longest to parse"
and "Files that took longest to codegen", list the source files (`*.cc`)
which took the longest to build. As of this writing, the top offender
is `messaging_service.cc` taking 84 seconds to parse and 135 second to
compile:
```
**** Files that took longest to parse (compiler frontend):
 84164 ms: build/dev/message/messaging_service.o
...
**** Files that took longest to codegen (compiler backend):
135417 ms: build/dev/message/messaging_service.o
```

There is little to be done with this sort of information - it doesn't
teach us _why_ this one source file, only 1,376 lines of code, takes
almost 4 minutes of CPU to compile. It might have some use, for finding
the slowest-to-build files to build them first (to avoid stragglers in
a parallel build) or to split them to even smaller files.

The next section, is more interesting:
```
**** Templates that took longest to instantiate:
385738 ms: fmt::detail::vformat_to<char> (552 times, avg 698 ms)
197016 ms: boost::basic_regex<char>::assign (329 times, avg 598 ms)
195522 ms: ser::deserialize<seastar::simple_memory_input_stream> (1617 times, av
g 120 ms)
...
```

The top templates in this list have the unfortunate distinction of being
used in many source files (often by appearing in a header file used in
many places), and taking a very long time (as much as 0.7 seconds in the
above example) to compile each time it is used. We can save a lot of
compilation time if we reduce the number of times each is used (perhaps
by avoiding it in a popular header file, or by using this header file
less), change the template to use fewer templates inside it, or use
"extern template" to only instantiate it once.

The next section,
```
*** Template sets that took longest to instantiate:
640682 ms: std::unique_ptr<$> (31372 times, avg 20 ms)
581171 ms: std::__and_<$> (351772 times, avg 1 ms)

```
Is probably less useful directly. We can perhaps hope that other changes
will reduce the number of time that `std::unique_ptr<>` needs to be
instantiated, but can't do anything directly to improve it.

In the next section, we see the slowest to compile functions:
```
**** Functions that took longest to compile:
  2899 ms: cql3_parser::CqlParser::cqlStatement() (build/dev/gen/cql3/CqlParser.cpp)
  2144 ms: replica::database::setup_metrics() (replica/database.cc)
```
Maybe we can take a look at some of these functions to figure out why they
are so slow to compile - e.g., a short function like alternator::stats::stats() 
taking a full second to compile because of inefficient Boost options tricks.
But in general, looking at this section is probably not worth the effort,
because each of these functions is only compiled **once**, not hundreds of
times as in the template instantiations above, so even if we improve some of
them, the saving will be small.

The next section is similar, but for template functions (so can be compiled
multiple times):

```
*** Function sets that took longest to compile / optimize:
 57092 ms: fmt::v9::appender fmt::v9::detail::write_int_noinline<$>(fmt::v9::appender, fmt::v9::detail::write_int_arg<$>, fmt::v9::basic_format_specs<$> const&, fmt::v9::detail::locale_ref) (1071 times, avg 53 ms)
 44361 ms: fmt::v9::detail::format_dragon(fmt::v9::detail::basic_fp<$>, unsigned int, int, fmt::v9::detail::buffer<$>&, int&) (357 times, avg 124 ms)
```

The next section is one of the most interesting ones, about the most
expensive header files:

```
1086240 ms: replica/database.hh (included 139 times, avg 7814 ms), included via:
  47x: query_processor.hh wasm.hh 
  46x: <direct include>
  14x: schema_registry.hh 
  6x: user_function.hh wasm.hh 
  5x: cql_test_env.hh 
  4x: column_family.hh 
  3x: wasm.hh 
  3x: storage_service.hh tablet_allocator.hh 
  3x: server.hh query_processor.hh wasm.hh 
  2x: storage_service.hh data_listeners.hh schema_registry.hh 
  1x: test_services.hh 
  ...
```

Here we see that a whopping 1086 seconds - that's about 6% of the total
build time - was spent compiling replica/database.hh 139 times - almost
8 seconds each. We should make an effort to include database.hh less,
split it, make it do less. Note also how out of the 139 source files
that included database.hh, only 46 included it directly - others included
it through `wasm.hh` and `query_processor.hh` - are those indirect
includes necessary?

It's worth noting that the the cost of 1086 seconds is an **overestimation**.
In practice, deleting this header file will not reduce 1086 seconds of the
compilation time. The reason is that database.hh probably includes other
headers and instantiates templates which get "billed" to it because this
header happened, by chance, to be the first to use them in this compilation
unit; But other headers use those headers and templates too, so they would
have been instantiated anyway even if database.hh got deleted. And it's
hard to know this without trying.

# ClangBuildAnalyzer.ini
As show above, ClangBuildAnalyzer makes lists of various types (slowest
source files, slowest templates, etc.) and each of these show a fixed
number of top items. This number, and other things, can be configured in a
file ClangBuildAnalyzer.ini. See an example of how to configure it in
https://github.com/aras-p/ClangBuildAnalyzer/blob/main/ClangBuildAnalyzer.ini

Here is another example - the configuration file that I use:
```
[counts]

# files that took most time to parse
fileParse = 20
# files that took most time to generate code for
fileCodegen = 20
# functions that took most time to generate code for
function = 30
# header files that were most expensive to include
header = 1000
# for each expensive header, this many include paths to it are shown
headerChain = 10
# templates that took longest to instantiate
template = 50

# Minimum times (in ms) for things to be recorded into trace
[minTimes]
# parse/codegen for a file
file = 1

[misc]
# Maximum length of symbol names printed; longer names will get truncated
maxNameLength = 270
# Only print "root" headers in expensive header report, i.e.
# only headers that are directly included by at least one source file
onlyRootHeaders = false
```
