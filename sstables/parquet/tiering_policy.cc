/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "sstables/parquet/tiering_policy.hh"

#include <fmt/format.h>

namespace sstables::parquet {

namespace {

tiering_decision decline(std::string why) {
    return {tiering_verdict::use_native, std::move(why)};
}

} // namespace

tiering_decision evaluate_tiering(const tiering_inputs& in,
                                  const tiering_thresholds& th) {
    // Three criteria, a conjunction, cheapest first so the common "this is not a bottom-tier
    // output" case costs one comparison. What is *not* here is as deliberate as what is --
    // see the header for why C2, C4 and C7 went.

    // C1 -- position in the LSM.
    if (!in.bottom_tier) {
        return decline("not a bottom-tier output; it would be re-compacted and "
                       "re-encoded again");
    }

    // C5 -- schema, and width. The width bound stands in for C7: a point read decodes a page
    // in every column chunk it projects, so its cost is linear in column count (~90 us each),
    // and past the ceiling a table is too slow to point-read as Parquet however well it
    // compresses.
    if (!in.schema_eligible) {
        return decline("schema is not eligible (counters or unsupported types)");
    }
    if (in.column_count > th.max_columns) {
        return decline(fmt::format("{} columns exceeds the {} limit",
                                   in.column_count, th.max_columns));
    }

    // C6 -- the load-bearing one, and the only one that reads data. An unmeasured table is
    // not a table we convert: guessing wrong means rewriting terabytes for nothing. It also
    // absorbs what C2 used to do, because a file too small to pay measures as a loss.
    if (!in.predicted_gain) {
        return decline("no measured gain available; run the estimator first");
    }
    if (*in.predicted_gain < th.min_gain_ratio) {
        return decline(fmt::format("predicted gain {:.3f} is below the {:.3f} minimum",
                                   *in.predicted_gain, th.min_gain_ratio));
    }

    return {tiering_verdict::use_parquet,
            fmt::format("bottom tier, {} columns, predicted gain {:.3f}",
                        in.column_count, *in.predicted_gain)};
}

} // namespace sstables::parquet
