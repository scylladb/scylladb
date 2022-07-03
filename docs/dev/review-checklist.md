# Review Checklist

## How to use this document

This document describes general guidelines for reviewing patches to
Scylla. The guidelines are not absolute; with good reason they can be
overridden at the discretion of the maintainers. Nevertheless, patches
should try to conform to these guidelines instead of finding reasons for
seeking an exception.

## Code style

Code should conform to the project's coding style, see
[Seastar coding style](https://github.com/scylladb/seastar/blob/master/coding-style.md).


## Patch organization

Changes should be organized into one or more patches, depending on
how big the change is, according to the following
guidelines:
- Patches should be small.
- Patches should all individually compile and be correct
  (all tests should pass). You are not required to verify
  this but do look for any signs in the code that
  suggest this might be violated. For example a patch
  introducing a regression test being ordered before the
  fix. Or a method being used before the patch that
  introduces it.
- Patches should be self-contained and do one thing.
- Each patch should have a descriptive commit log.
- The patch description should not assume the reviewer is an
  expert in the matter. It should include enough context
  that a non-expert can understand, on their way to becoming
  an expert.
- The patch description should be self-contained and not
  refer to out-of-band discussions ("as agreed on the daily
  call").
- Each patch should have a motivation for the change.
  Patch descriptions that simply say "make X do Y" without
  explaining why that's a good thing are considered bad.
  A good litmus test is whether the inverse patch ("make X
  not do Y") is just as compelling as the original patch. If it
  is, why prefer one over the other?
- Each pull request should include a motivation for the
  change(s). For multi-patch series, this should be in
  the cover-letter, for single patch ones in the patch
  description.

## Naming

Names should avoid abbreviations (industry-standard abbreviations
are acceptable) as they make the code opaque to newcomers. Within
a restricted scope such as a function or a class, local or private
variables can be given abbreviated names.

## Micro-performance

The following should be avoided:
 - computational loops without preemption checks and yields
 - allocation of large contiguous memory blocks in non-initialization
   code
 - quadratic (or worse) behavior on user-controlled inputs

## Concurrency

Code that generates concurrent behavior should include means to control
the amount of concurrency, placing an upper bound on the amount of memory
in use by concurrent operations.

If a component performs work in the background, it would include a mechanism
(`stop()` or `close()`) to wait for background work to complete. If background
work is unbounded in time, the code should be able to abort in-progress work.

## Unit testing

Patches should be accompanied by unit test when feasible.
 - patches that fix a bug should be accompanied by a test that fails
   before the patch is applied and passes after it is applied
 - patches that introduce generic code (such as a container) should
   test all aspects of that code, not just those that are used by
   the patchset.

## Templates

In non performance sensitive code, type erasure should be preferred
to templates in order to reduce code and compile time bloat. In performance
sensitive code, template bloat can be justified.

Template parameters should be constrained by C++ concepts, both as
code documentation, and to enable early error checking by the compiler.

Templates should not be used as an ad-hoc method to reduce code duplication;
rather they should be used when a common concept can be captured in
template code and reused.

## Singletons

The singleton pattern should be avoided when it can introduce
confusion about initialization order or make unit testing difficult. Instead,
dependencies should be passed via constructor parameters (the "dependency
injection" pattern).

## Invariant checking in scylla

Non obvious invariants should be checked. There are three ways of
doing it, asserts, throwing exceptions and logging failures.

If an invariant is critical for the stability of the system, use an
assert. For example, a broken invariant in the memory allocator can
lead to data corruption and there is no way to work around it, so
assert.

If an invariant is needed only by some features and the system can
shutdown cleanly without it, then a throw is appropriate. If the issue
was found while serving a user request the exception is propagated
back to the user. It if was a scylla internal operation, it will still
bring down the node.

When the operation can just be ignored, logging the failure is the
best option. The canonical example is when an element is "known" to be
in a container, but while trying to remove it we find out that it was
not there.
