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
#include "schema/schema_registry.hh"

future<> apply_gently(mutation_partition& target, const schema& s, mutation_partition_view p,
        const schema& p_schema, mutation_application_stats& app_stats) {
    mutation_partition p2(target, mutation_partition::copy_comparators_only{});
    partition_builder b(p_schema, p2);
    co_await p.accept_gently(p_schema, b);
    if (s.version() != p_schema.version()) {
        p2.upgrade(p_schema, s);
    }
    apply_resume res;
    // we only move from p2.static_row() in the first iteration when target.static_row() is empty
    // NOLINTNEXTLINE(bugprone-use-after-move)
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
    // we only move from p2.static_row() in the first iteration when target.static_row() is empty
    // NOLINTNEXTLINE(bugprone-use-after-move)
    while (target.apply_monotonically(s, std::move(p2), no_cache_tracker, app_stats, is_preemptible::yes, res) == stop_iteration::no) {
        co_await yield();
    }
}

future<> apply_gently(mutation_partition& target, const schema& s, mutation_partition&& p, mutation_application_stats& app_stats) {
    apply_resume res;
    // we only move from p2.static_row() in the first iteration when target.static_row() is empty
    // NOLINTNEXTLINE(bugprone-use-after-move)
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

future<mutation> to_mutation_gently(const canonical_mutation& cm, schema_ptr s) {
    auto in = ser::as_input_stream(cm.representation());
    auto mv = ser::deserialize(in, boost::type<ser::canonical_mutation_view>());

    auto cf_id = mv.table_id();
    if (s->id() != cf_id) {
        throw std::runtime_error(format("Attempted to deserialize canonical_mutation of table {} with schema of table {} ({}.{})",
                                        cf_id, s->id(), s->ks_name(), s->cf_name()));
    }

    auto version = mv.schema_version();
    auto pk = mv.key();

    mutation m(std::move(s), std::move(pk));

    if (version == m.schema()->version()) {
        auto partition_view = mutation_partition_view::from_view(mv.partition());
        mutation_application_stats app_stats;
        co_await apply_gently(m.partition(), *m.schema(), partition_view, *m.schema(), app_stats);
    } else {
        column_mapping cm = mv.mapping();
        converting_mutation_partition_applier v(cm, *m.schema(), m.partition());
        auto partition_view = mutation_partition_view::from_view(mv.partition());
        co_await partition_view.accept_gently(cm, v);
    }
    co_return m;
}

future<canonical_mutation> make_canonical_mutation_gently(const mutation& m)
{
    mutation_partition_serializer part_ser(*m.schema(), m.partition());

    canonical_mutation res;
    ser::writer_of_canonical_mutation<bytes_ostream> wr(res.representation());
    auto w = co_await std::move(wr).write_table_id(m.schema()->id())
                 .write_schema_version(m.schema()->version())
                 .write_key(m.key())
                 .write_mapping(m.schema()->get_column_mapping())
                 .partition_gently([&] (auto wr) {
                     return part_ser.write_gently(std::move(wr));
                 });
    w.end_canonical_mutation();
    co_return res;
}

future<frozen_mutation>
freeze_gently(const mutation& m) {
    auto fm = frozen_mutation(m.key());
    mutation_partition_serializer part_ser(*m.schema(), m.partition());

    ser::writer_of_mutation<bytes_ostream> wom(fm.representation());
    auto wr = co_await std::move(wom).write_table_id(m.schema()->id())
                .write_schema_version(m.schema()->version())
                .write_key(m.key())
                .partition_gently([&] (auto wr) {
                    return part_ser.write_gently(std::move(wr));
                });
    wr.end_mutation();
    fm.representation().reduce_chunk_count();
    co_return fm;
}

future<mutation>
unfreeze_gently(const frozen_mutation& fm, schema_ptr schema) {
    check_schema_version(fm.schema_version(), *schema);
    mutation m(schema, fm.key());
    partition_builder b(*schema, m.partition());
    try {
        co_await fm.partition().accept_gently(*schema, b);
    } catch (...) {
        std::throw_with_nested(std::runtime_error(format(
                "frozen_mutation::unfreeze_gently(): failed unfreezing mutation {} of {}.{}", fm.key(), schema->ks_name(), schema->cf_name())));
    }
    co_return m;
}

future<std::vector<mutation>> unfreeze_gently(std::span<frozen_mutation> muts) {
    std::vector<mutation> result;
    result.reserve(muts.size());
    for (auto& fm : muts) {
        result.push_back(co_await unfreeze_gently(fm, local_schema_registry().get(fm.schema_version())));
    }
    co_return result;
}
