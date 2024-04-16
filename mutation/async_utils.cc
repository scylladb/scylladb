/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "mutation/async_utils.hh"
#include "mutation/mutation_partition_view.hh"
#include "mutation/partition_version.hh"
#include "partition_builder.hh"
#include "mutation/canonical_mutation.hh"
#include "converting_mutation_partition_applier.hh"
#include "mutation/mutation_partition_serializer.hh"
#include "idl/mutation.dist.impl.hh"

future<> apply_gently(mutation_partition& target, const schema& s, mutation_partition_view p,
        const schema& p_schema, mutation_application_stats& app_stats) {
    mutation_partition p2(target, mutation_partition::copy_comparators_only{});
    partition_builder b(p_schema, p2);
    co_await p.accept_gently(p_schema, b);
    if (s.version() != p_schema.version()) {
        p2.upgrade(p_schema, s);
    }
    apply_resume res;
    while (target.apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::yes, res) == stop_iteration::no) {
        co_await yield();
    }
}

future<> apply_gently(mutation_partition& target, const schema& s, const mutation_partition& p,
        const schema& p_schema, mutation_application_stats& app_stats) {
    mutation_partition p2(p_schema, p);
    if (s.version() != p_schema.version()) {
        p2.upgrade(p_schema, s);
    }
    apply_resume res;
    while (target.apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::yes, res) == stop_iteration::no) {
        co_await yield();
    }
}

future<> apply_gently(mutation_partition& target, const schema& s, mutation_partition&& p, mutation_application_stats& app_stats) {
    apply_resume res;
    while (target.apply_monotonically(s, std::move(p), no_cache_tracker, app_stats, is_preemptible::yes, res) == stop_iteration::no) {
        co_await yield();
    }
}

future<> apply_gently(mutation& target, mutation&& m) {
    mutation_application_stats app_stats;
    co_await apply_gently(target.partition(), *target.schema(), std::move(m.partition()), *m.schema(), app_stats);
}

future<> apply_gently(mutation& target, const mutation& m) {
    auto m2 = m;
    mutation_application_stats app_stats;
    co_await apply_gently(target.partition(), *target.schema(), std::move(m2.partition()), *m.schema(), app_stats);
}
