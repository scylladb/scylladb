# Code Coverage Support

Scylla supports several code coverage workflows in order to analyze code coverage in testing
and during runs this relies on llvm toolchain for instrumentation and conversion of the resulting
profiles.
The support for code coverage is provided through the following components:
1. `configure.py` - build Scylla with code coverage instrumentation
2. `test.py` - unit test runs with code coverage processing and reporting.
3. `test/pylib/coverage_utils.py` - provides library utilities and cli for common code coverage processing operations.
4. `test/pylib/lcov_utils.py` - provides library utilities for manipulation of lcov trace files.
5. a rest api endpoint: `/system/dump_llvm_profile` for profile dumping during runtime or in cases where graceful shutdown of Scylla is not desired or possible.

## General Workflow With Coverage
1. Build Scylla with coverage instrumentation
2. Run Scylla with some use case (or run test)
3. Trigger coverage profile dump
4. Process the profile into an lcov trace file
5. (optional) post process the lcov trace
6. Generate a textual or an HTML report to view the coverage report

## Why Does The Profile Data Converted Into Lcov Format?
One can just use the coverage profiles produced by llvm with the native llvm tools, the method for converting the profile data into lcov format was chosen for the following reasons:
1. Being source oriented, Lcov format allows for merging coverage data from different binaries easily even if they originated from different source trees (even though it is not very useful...). However combining different binaries from the same source file is useful since you can combine unit test runs with tests that used the actual Scylla binary.
2. Lcov is easy to parse and manipulate (For example: we can transform it into a patch coverage report)
3. The HTML reporting is nicer (genhtml) than the llvm one.

## Building Scylla With Coverage Instrumentation
Building scylla with coverage instrumentation is done by adding the `--coverage` option to the
`configure.py` command line. Example:
`./configure.py --mode dev --coverage` - will build Scylla in dev mode with coverage instrumentation.

NOTE: when adding the `--coverage` all build modes that are configured are going to be built with coverage instrumentation.

NOTE: Coverage instrumentation incurs some performance penalty but it is hard to determine exactly
how much.

## Running Unit Tests With Coverage Processing And Reporting
In order to get an lcov trace files for unit test runs, simply add `--coverage` or `--coverage-mode` to the `test.py` command line.
Examples:
1. `./test.py --coverage` - will run all tests in all built modes and will produce a coverage lcov traces for all of them.
2. `./test.py --coverage-mode dev` - will run all tests in all built modes and will produce a coverage lcov traces only for the dev mode runs.
3. `./test.py --mode dev --mode release --coverage-mode dev` - will run all tests in the dev and release mode but will only produce an lcov traces for the dev mode tests.

   ### Where To Find The Lcov Traces After The `test.py` Run?
   `test.py` will produce a hierarchy of of lcov trace, all rooted in the `<tmpdir>` (default:``./testlog``).
   1. `<suite_name>.info` - will be produced for every suite under `<tmpdir>/<mode>/coverage/<suite_name>/`
   2. `<mode>_coverage.info` - will be produced for ever coverage enabled mode under `<tmpdir>/<mode>/coverage/` and contains the aggregated coverage information from all suites.
   3. `test_coverage.info` - will be created under `<tmpdir>` and contains the aggregated coverage information for all coverage enabled modes.
   4. `test_coverage_report.txt` - will be produced under `<tmpdir>` a textual report of the coverage information in `test_coverage.info`.

## Advanced And Manual Workflows
Once built with code coverage instrumentation, all test executables and scylla executable are going
to dump a a profile upon graceful shutdown / clean exit. However, in order to produce a coverage report some extra processing is needed.
For reference of how to process the profile dumps using the llvm toolchain, refer to: https://clang.llvm.org/docs/SourceBasedCodeCoverage.html

HINT: It is recommended to run with LLVM_PROFILE_FILE set in the environment so the profiles will be dumped to a known location (https://clang.llvm.org/docs/SourceBasedCodeCoverage.html#running-the-instrumented-program)

   ### Working with the cli
   The cli `test/pylib/coverage_utils.py` contains some commands to help with the processing of profile dumps. **It is recommended to run the tool from the main source directory**

   For extensive cli help run: `test/pylib/coverage_utils.py` or give the `-h` or `--help` to one of the subcommands.

   Given a `profiles/someprofile.profraw` (or several files) the most common operations are:

   NOTE: In most of the commands a list of files can be given in order to batch the operation on multiple profiles. But we concentrate on single file here for simplicity.

   #### 1. Converting Raw Profiles Into Indexed Profile:
   Run: `test/pylib/coverage_utils.py llvm-profiles merge profiles/someprofile.profraw profiles`- This will create an indexed profile in the `./profiles` directory. This file is named after the build ID of the executable that created this file. For example: `85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.profdata`
   On this file you can use the llvm tools to produce reports from. However files that originated in different binaries are hard to merge and will yield incomplete or incorrect results.


   #### 2. Converting Indexed Profiles Into Lcov files:
   Run: `test/pylib/coverage_utils.py llvm-profiles to-lcov --binary-search-path build --excludes-file coverage_excludes.txt profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.profdata` - This will create `profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.info` which can be manipulated and be used to produce a report.

   #### 3. Producing A Report From Lcov trace files:
   For textual reports:
   1. `lcov --summary profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.info` for a summary of the the coverage rates:
   ```
   Reading tracefile profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.info
   Summary coverage rate:
   lines......: 14.5% (11 of 76 lines)
   functions..: 11.8% (2 of 17 functions)
   branches...: no data found

   ```
   2. `lcov --list profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.info` for a per file report:
   ```
   Reading tracefile profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.info
                        |Lines       |Functions  |Branches
   Filename             |Rate     Num|Rate    Num|Rate     Num
   ===========================================================
   [utils/]
   abi/eh_ia64.hh       | 100%      3| 100%     1|    -      0
   exceptions.cc        |15.1%     53|16.7%     6|    -      0
   exceptions.hh        | 0.0%     20| 0.0%    10|    -      0
   ===========================================================
                  Total:|14.5%     76|11.8%    17|    -      0

   ```
   For an HTML report:
   `test/pylib/coverage_utils.py lcov-tools genhtml --output-dir profiles/html profiles/85e5e08c67bd9bd74c2caeb98aca2a45360cf25a.info` - This will create an HTML report rooted at `profiles/html` in order to view it, open `profiles/html/index.html` in your browser.

#### Other Advanced Operations With Lcov Files:
Once you have a coverage data you can manipulate it in the following ways:
1. "Set Like" operations -
```
union               Merges several (or single) lcov file into another trace file. If testname is given, the resulting lcov file will be tagged with this name, else if will just merge the files similarly to 'lcov -a...' command.
                        Files can also be filtered (see 'man lcovrc')
    diff                computes the diff between two or more coverage files (lines that are covered by first but not others)
    intersection        computes the intersection between two or more coverage files (lines that are covered by all trace files)
    symmetric-dff       computes the symmetric difference between two traces (line covered by either trace but not both)
```
2. Produce a "patch coverage" report which transform the report into a patch centric report of the coverage of lines introduced by some git history fragment (i.e `HEAD~10..HEAD`) there is also an option for patch coverage report for a merge commit in which case only the patches on the merge will be considered.

For further information you can refer to the cli help:
`test/pylib/coverage_utils.py help` or view the tools code: `test/pylib/coverage_utils.py` and `test/pylib/lcov_utils.py`