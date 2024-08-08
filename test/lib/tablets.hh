/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/format.h>
#include <fmt/color.h>
#include <optional>
#include <chrono>

#include "locator/tablets.hh"

namespace load_balancer_test {

struct params {
    int iterations;
    int nodes;
    std::optional<int> tablets1;
    std::optional<int> tablets2;
    int rf1;
    int rf2;
    int shards;
    int scale1 = 1;
    int scale2 = 1;
};

struct table_balance {
    double shard_overcommit;
    double best_shard_overcommit;
    double node_overcommit;
};

constexpr auto nr_tables = 2;

struct cluster_balance {
    table_balance tables[nr_tables];
};

using seconds_double = std::chrono::duration<double>;

struct rebalance_stats {
    seconds_double elapsed_time = seconds_double(0);
    seconds_double max_rebalance_time = seconds_double(0);
    uint64_t rebalance_count = 0;

    rebalance_stats& operator+=(const rebalance_stats& other) {
        elapsed_time += other.elapsed_time;
        max_rebalance_time = std::max(max_rebalance_time, other.max_rebalance_time);
        rebalance_count += other.rebalance_count;
        return *this;
    }
};

struct results {
    cluster_balance init;
    cluster_balance worst;
    cluster_balance last;
    rebalance_stats stats;
};

// Wrapper around a value with a formatter which colors it based on value thresholds.
struct pretty_value {
    double val;

    const std::array<double, 5>& thresholds;

    static constexpr std::array<fmt::terminal_color, 5> colors = {
            fmt::terminal_color::bright_white,
            fmt::terminal_color::bright_yellow,
            fmt::terminal_color::yellow,
            fmt::terminal_color::bright_red,
            fmt::terminal_color::red,
    };
};

struct overcommit_value : public pretty_value {
    static constexpr std::array<double, 5> thresholds = {1.02, 1.1, 1.3, 1.7};
    overcommit_value(double value) : pretty_value(value, thresholds) {}
};

future<results> test_load_balancing_with_many_tables(params, bool table_aware);

} // namespace load_balancer_test

template<>
struct fmt::formatter<load_balancer_test::pretty_value> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const load_balancer_test::pretty_value& v, FormatContext& ctx) const {
        auto i = std::upper_bound(v.thresholds.begin(), v.thresholds.end(), std::max(v.val, v.thresholds[0]));
        auto col = v.colors[std::distance(v.thresholds.begin(), i) - 1];
        return fmt::format_to(ctx.out(), fg(col), "{:.3f}", v.val);
    }
};

template<> struct fmt::formatter<load_balancer_test::overcommit_value> : fmt::formatter<load_balancer_test::pretty_value> {};

template<>
struct fmt::formatter<load_balancer_test::table_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const load_balancer_test::table_balance& b, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{shard={} (best={:.2f}), node={}}}",
                              load_balancer_test::overcommit_value(b.shard_overcommit),
                              b.best_shard_overcommit,
                              load_balancer_test::overcommit_value(b.node_overcommit));
    }
};

template<>
struct fmt::formatter<load_balancer_test::cluster_balance> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const load_balancer_test::cluster_balance& r, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{table1={}, table2={}}}", r.tables[0], r.tables[1]);
    }
};

template<>
struct fmt::formatter<load_balancer_test::params> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const load_balancer_test::params& p, FormatContext& ctx) const {
        auto tablets1_per_shard = double(p.tablets1.value_or(0)) * p.rf1 / (p.nodes * p.shards);
        auto tablets2_per_shard = double(p.tablets2.value_or(0)) * p.rf2 / (p.nodes * p.shards);
        return fmt::format_to(ctx.out(), "{{iterations={}, nodes={}, tablets1={} ({:0.1f}/sh), tablets2={} ({:0.1f}/sh), rf1={}, rf2={}, shards={}}}",
                              p.iterations, p.nodes,
                              p.tablets1.value_or(0), tablets1_per_shard,
                              p.tablets2.value_or(0), tablets2_per_shard,
                              p.rf1, p.rf2, p.shards);
    }
};
