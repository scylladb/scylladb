# Single-node functional tests for ScyllaDB's Alternator features

These tests use AWS's Python library ("boto3") and the pytest framework
to test Alternator's user-visible functionality ("black-box testing").
By using an actual AWS library for the tests, the same tests can be run
against *any* implementation of the DynamoDB API - both ScyllaDB Alternator
and original DynamoDB. Most tests - except in rare cases - should pass on
both, to ensure that Alternator is compatible with DynamoDB in most features.

Both pytest and boto3 are easily available on Linux distributions, or via
"pip install".

To run all tests against an already-running installation of Alternator
listening on http://localhost:8000, just run `pytest` in this directory.
The `--url` option can be used to specify a different address where Alternator
is listening. Use the `--aws` option to run tests against AWS DynamoDB.

More conveniently, instead of starting ScyllaDB on your own, we have a
script `test/alternator/run` which does all the work necessary to start
ScyllaDB with Alternator, and then runs `pytest` against it. The ScyllaDB
process is run in a temporary directory which is automatically deleted when
the test ends. This is recommended way because it configures scylla to start
much faster.

`run` automatically picks the most-recently compiled version of Scylla in
`build/*/scylla` - but this choice of Scylla executable can be overridden with
the `SCYLLA` environment variable.

By default, `pytest` or `test/alternator/run` run all Alternator tests.
You can pass different options to control which tests run:

* To run all tests in a single file, do `pytest` (or `run`) `test_gsi.py`.
* To run a single specific test, do `pytest test_gsi.py::test_gsi_strong_consistency`.

Additional useful pytest options, especially useful for debugging tests:

* -v: show the names of each individual test running instead of just dots.
* -s: show the full output of running tests (by default, pytest captures the test's output and only displays it if a test fails)

When `pytest` or `run` get the `--aws` option and run the test against AWS
instead of a ScyllaDB installation, you need to have AWS credentials for
using DynamoDB, and configure the following files:

In `~/.aws/credentials` you should put your AWS key:
```
[default]
aws_access_key_id = XXXXXXXXXXXXXXXXXXXX
aws_secret_access_key = xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

and `~/.aws/config`, put the default region to use in the test:
```
[default]
region = us-east-1
```

You don't need these files to run the tests against a local installation
of ScyllaDB.

## HTTPS support

In order to run tests over HTTPS instead of the default HTTP, run
`pytest` or `run` with the `--https` parameter. `run` will automatically
run ScyllaDB with HTTPS, but if you are running ScyllaDB manually you will
need to configure it appropriately before running `pytest --https`:

(you don't need to do the following if you use the `run` convenience script)

You'll need to configure ScyllaDB with the alternator\_https\_port
configuration option in order to initialize a HTTPS server. Moreover, running
an instance of a HTTPS server requires a certificate. Here's how to easily
generate a key and a self-signed certificate, which is sufficient to run
`--https` tests:

```
openssl genrsa 2048 > scylla.key
openssl req -new -x509 -nodes -sha256 -days 365 -key scylla.key -out scylla.crt
```

If this pair is put into `conf/` directory, it will be enough
to allow the alternator HTTPS server to think it's been authorized and properly certified.
Still, boto3 library issues warnings that the certificate used for communication is self-signed,
and thus should not be trusted. For the sake of running local tests this warning is explicitly ignored.


## Authorization

(you don't need to do the following if you use the `run` convenience script
which configure ScyllaDB for you automatically)

By default, boto3 prepares a properly signed Authorization header with every request.
In order to confirm the authorization, the server recomputes the signature by using
user credentials (user-provided username + a secret key known by the server),
and then checks if it matches the signature from the header.
Early alternator code did not verify signatures at all, which is also allowed by the protocol.
A partial implementation of the authorization verification can be allowed by providing a Scylla
configuration parameter:
```yaml
  alternator_enforce_authorization: true
```
The test implementation is currently coupled with Scylla's default account
"cassandra" with password "cassandra".

Most tests expect the authorization to succeed, so they will pass even with `alternator_enforce_authorization`
turned off. However, test cases from `test_authorization.py` may require this option to be turned on,
so it's advised.

# Developing new Alternator tests

The Alternator test framework is designed to encourage Alternator developers
to quickly write _extensive_ functional tests for the Alternator features which
they develop. This is why test/alternator is included in the main Scylla
repository (and not some external repository), and why the test framework
focuses on the ease of writing new tests, the ease of understanding test
failures, and the speed to run and re-run tests especially during development
of the test and/or the tested feature. Moreover, the ability to run the
same tests against DynamoDB is meant to make it easier to write good tests
even before developing a feature (so-called "test-driven development").

To maintain these benefits, we recommend that the following principles
and practices be followed when writing new tests:

1. **Keep each test fast**: Ideally each test function should
   take a fraction of a second. At the time of this writing, the entire
   Alternator test suite of over 700 test functions takes around 28
   seconds to run with `test/alternator/run` - on average 0.04 second per
   test. Always think if your test really requires inserting a million items
   or sleeping 5 seconds - usually it does NOT.
   Short tests make it easy and fun to run and rerun a single test during
   development, and also allow developers to run the entire Alternator
   test suite during development instead of trying to guess which test might
   break.

2. **Keep each test small**: Don't write one big test function for many
   aspects of some feature. Instead, write in the same test file many small
   test functions, each for a different aspect of the feature. This makes it
   easier to understand what each small test checks. And when a test fails,
   it makes it easier to understand exactly which part of the feature broke.
   It also makes the test code easier to read and understand.

3. **Use fixtures to reduce test time**: When testing a feature with many
   small test functions (as recommended above), often all of these small
   test functions need some common setup, such as a table with a certain
   schema or with certain data in it. Instead of each small test function
   re-creating the same table (which takes time), use pytest _fixtures_.
   Fixtures allow several test functions to use the same temporary test table.
   Different tests can safely share a test table by using unique keys
   instead of hard-coded keys that can break if another test accidentally
   uses the same key. Because the DynamoDB data model is mostly schema-less,
   we can often reuse the same table in many - even hundreds - of different
   tests. test/alternator/conftest.py contains a few common fixtures like
   that - for example `test_table_ss` is a table with a string hash key
   and string sort key which many tests reuse.
   The heavy use of fixtures - instead of creating a new table in each
   tests - is one of the reasons why Alternator tests are so fast.
   New tests should strive to keep it this way - and create as few as
   possible new tables.

4. **Write comments**: No, a self-explanatory test name is NOT enough.
   It may be self-explanatory to you, but when the test fails a year
   from now, nobody will remember what you were testing, or *why* you
   decided to check these specific conditions, or if it's a forgotten
   backport of a fix to an issue, which issue was that.
   Before each test function, please explain why it exists - what feature
   it intends to verify, and why it is tested in this specific way.
   If the test is meant to reproduce a specific issue, please give the
   issue number. All of this will be very helpful for later developers
   which need to understand why your test suddenly failed after their
   refactoring, or on a different Scylla branch.

5. **Run your test against DynamoDB**: It is not enough to run your
   test against Alternator and see that it passes. Run it against DynamoDB as
   well, using `test/alternator/run --aws`. If the feature being tested is
   Scylla-only, the test can be skipped on DynamoDB by using the `scylla_only`
   fixture. But most of Scylla's Alternator features are identical to
   DynamoDB's and therefore most of our Alternator tests should pass on
   DynamoDB. If a test does not pass on DynamoDB, the test itself is likely
   wrong, and should be fixed. In rare cases, the test fails on DynamoDB
   because of a known bug in DynamoDB or a deliberate difference
   between Scylla and DynamoDB; In these rare cases, the test can be
   skipped by using the `scylla_only` fixture. However, make sure that
   you explain in a comment why you did this.

6. **Think about risky cases, don't just randomize**: One of the benefits
   of a developer also developing the tests during development of a feature
   (instead of someone else doing it later) is that specific edge cases
   can be considered and tested. For example, consider some operation taking
   a string. If a developer knows that an empty string or a very long
   string required special code and are at risk of being mis-handled or
   are at risk to break during some future refactoring, the developer should
   write separate tests for these cases.
   If, instead, we write a test that loops 1000 times testing a random
   string of length 10 - the result will be slow **and** will also miss
   the interesting cases - neither the empty or very long strings will
   come up as random draws of strings of length 10.
   Another danger of randomized tests is that they tend to obscure what
   is actually being tested: For example, a reviewer of the test may think
   the empty string is included in the test, while actually it isn't.
   Randomized "fuzz" testing has its benefits, but it is almost always the
   wrong thing to do in the context of the test/alternator framework.
   We should probably have a separate framework (or at least separate files)
   for these tests.

7. **Write tests, not a test library**: Developers are often told that
   long functions are evil, and are tempted to take maybe-useful sections
   of code from their test function and split them out to utility functions.
   Having a small number of these utility functions is indeed convenient,
   and we have some in `util.py`. However please resist the urge to add
   more and more of these utility functions.
   Utility functions are bad for several reasons. First, they make it
   harder to read tests - any reader will understand boto3 requests (and
   can find many online references for that library), but not be familiar
   with dozens of obscure utility functions.  Second, when a utility function
   is written for the benefit of a single test, it is often much less general
   than its author thought, and when it comes time to reuse it in a second
   test, it turns out it needs to be duplicated or changed, and the result
   is many confusingly-similar utility functions, or utility functions with
   many confusing parameters. We've seen this happening in other frameworks
   such as dtest.
   If you believe something _should_ be a utility function, start by
   putting it inside the single test file that needs it - and only move
   it to util.py if several test files can benefit from it.
   At the time of this writing, test/alternator has over 17,000 lines of test
   code, and around of 500 lines of library code. Please keep this ratio.
   We're writing a collection of tests - not a library.

8. **Do not over-design**: Continuing the "we're writing tests, not a
   library" theme, please focus on making individual tests easy and fast
   to write as well as later read. Do not over-design the test suite to use
   cool Python features like classes, strong typing, and other features that
   are useful for big projects but only make writing small tests more
   difficult. Putting tests inside Classes, in particular (as we do in dtest),
   just make it more cumbersome to run an individual test - that now needs
   to specify not just the file and test names, but also the class name.
   To share functions between different tests, a test file is good enough -
   we don't need a class inside the file.

9. **Put tests in the right file**: Try to keep related tests functions -
   tests which check the same feature, have a similar theme, or perhaps use
   some shared fixture or set of convenience functions, in the same test file.
   There is no overhead involved with having many small test files
   (unlike C++ tests where compiling each file has a large fixed overhead),
   but when there are too many small test files there is a cognitive
   burden for developers trying to find tests or trying to decide where
   to place new tests. So when writing a new test please try to consider
   whether it fits the theme of an already-existing test file, and if not try
   to create a new test file that you can explain, in a comment, which
   additional tests might belong there in the future.

10. **Test user-visible DynamoDB API features**: Usually (but not always),
   we should strive of testing API features that a user might access through
   a AWS SDK. We do have test that check traces, for example, but these should
   be the minority. The majority of the tests should not check log messages
   which aren't visible to a DynamoDB application.
   Tests that do check for error conditions _should_ check the error message
   but should usually focus on the _type_ of the error and important
   substrings, not entire error messages. We don't want dozens of tests to
   break every time we change a trivial detail in an error message.
