/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "sstables/parquet/tiering_policy.hh"

// Per-criterion tests for the hybrid tiering policy, in CI.
//
// `sstables/parquet/test_tiering.cc` has asserted the same properties since the policy was
// written, but it is built and run only by the hand-run `sstables/parquet/run_tests.sh`, so
// nothing in the project's own test wiring executed it: a change that quietly stopped
// enforcing a criterion would not have failed any CI job. This file is the CI-enforced copy.
//
// It is a `pure_boost_tests` entry because that is what the policy is: `tiering_policy.hh`
// has no Scylla dependencies at all and `evaluate_tiering()` is a pure function over a plain
// struct, so there is no reactor to start and nothing to fixture. `tiering_policy.cc` is
// already part of `scylla_core` in `configure.py`, so no extra source had to be added.
//
// The shape of the tests follows from the policy being a *conjunction*: assert that an output
// satisfying every criterion is accepted, then relax exactly one criterion at a time and
// assert both that it is rejected and that the reason names the criterion that did it.
// Asserting only the accept would let a criterion stop being enforced silently; asserting a
// rejection without its reason would let the wrong criterion take the blame, which is what
// makes the operator-facing log useless.

using namespace sstables::parquet;

namespace {

// The canonical accept: a large, clean, measured bottom-tier output.
tiering_inputs good() {
    tiering_inputs in;
    in.bottom_tier = true;
    in.schema_eligible = true;
    in.column_count = 105;             // ClickBench's width: admitted, and saves 40 %
    // Comfortably over the 0.40 default and comfortably under the 0.80 that
    // test_tiering_honours_custom_thresholds sets, so neither test sits on a boundary
    // it did not mean to test. Not a measured figure -- a fixture.
    in.predicted_gain = 0.60;
    return in;
}

// A rejection has to carry the reason as well as the verdict -- see the header comment.
void require_reject(const tiering_inputs& in, const std::string& reason_contains) {
    auto d = evaluate_tiering(in);
    BOOST_REQUIRE(!d.parquet());
    BOOST_REQUIRE_MESSAGE(d.reason.find(reason_contains) != std::string::npos,
                          "rejected for the wrong reason: " + d.reason);
}

} // namespace

BOOST_AUTO_TEST_CASE(test_tiering_accepts_a_fully_satisfied_output) {
    auto d = evaluate_tiering(good());
    BOOST_REQUIRE(d.parquet());
    // An acceptance explains itself too, not just a rejection.
    BOOST_REQUIRE(d.reason.find("predicted gain") != std::string::npos);
}

// C1 -- position. Parquet is for data nothing is going to rewrite again.
BOOST_AUTO_TEST_CASE(test_tiering_c1_rejects_non_bottom_tier) {
    auto in = good();
    in.bottom_tier = false;
    require_reject(in, "bottom-tier");
}

// C5 -- schema. Two independent halves: eligibility, and the width ceiling.
BOOST_AUTO_TEST_CASE(test_tiering_c5_rejects_ineligible_schema) {
    auto in = good();
    in.schema_eligible = false;
    require_reject(in, "not eligible");
}

BOOST_AUTO_TEST_CASE(test_tiering_c5_rejects_too_many_columns) {
    // 197 columns is Backblaze's width, which saves 4 % and point-reads at 134x native
    // (design doc 10.4e). That is the shape the default ceiling of 128 exists to refuse, so
    // it is the right number to pin the boundary with -- a value no CQL table could reach
    // would only prove the comparison ran.
    auto in = good();
    in.column_count = 197;
    require_reject(in, "columns");
}

// C6 -- the load-bearing criterion, and the one that has to fail closed.
BOOST_AUTO_TEST_CASE(test_tiering_c6_unmeasured_is_a_rejection_not_a_guess) {
    auto in = good();
    in.predicted_gain.reset();
    require_reject(in, "no measured gain");
}

BOOST_AUTO_TEST_CASE(test_tiering_c6_rejects_gain_below_the_floor) {
    auto in = good();
    in.predicted_gain = 0.05;
    require_reject(in, "predicted gain");
}

BOOST_AUTO_TEST_CASE(test_tiering_c6_refuses_a_gain_the_old_gate_allowed) {
    // 0.30 was accepted while min_gain_ratio was 0.15 and must be refused at 0.40. This is the
    // only case that would notice the default being reverted -- the boundary test below moves with
    // the default, so it cannot catch that on its own.
    auto in = good();
    in.predicted_gain = 0.30;
    require_reject(in, "predicted gain");
}

BOOST_AUTO_TEST_CASE(test_tiering_c6_accepts_exactly_at_the_threshold) {
    // The boundary itself, so that a `>` / `>=` slip is caught rather than being absorbed by
    // a comfortable margin on either side.
    auto in = good();
    in.predicted_gain = 0.40;
    BOOST_REQUIRE(evaluate_tiering(in).parquet());
}

// The thresholds are per-table knobs (design doc 8.3), so passing them has to actually
// change the answer -- in both directions, or a caller-supplied threshold that was silently
// ignored would still look like it worked on the defaults.
BOOST_AUTO_TEST_CASE(test_tiering_honours_custom_thresholds) {
    tiering_thresholds th;
    th.min_gain_ratio = 0.80;

    auto in = good();                  // gain 0.42: fine by default, not by this threshold
    BOOST_REQUIRE(!evaluate_tiering(in, th).parquet());

    in.predicted_gain = 0.9;
    BOOST_REQUIRE(evaluate_tiering(in, th).parquet());
}

// Whatever the criterion, the decision must be explicable. This is what the compaction log
// prints, and an empty string there is indistinguishable from "no decision was made".
BOOST_AUTO_TEST_CASE(test_tiering_rejections_are_never_silent) {
    auto in = good();
    in.bottom_tier = false;
    BOOST_REQUIRE(!evaluate_tiering(in).reason.empty());
}
