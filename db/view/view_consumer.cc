/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "db/view/view_consumer.hh"
#include "db/view/view_builder.hh"
#include "readers/from_fragments.hh"
#include "utils/exponential_backoff_retry.hh"

using namespace std::chrono_literals;

static logging::logger vc_logger("view_consumer");

static inline void inject_failure(std::string_view operation) {
    utils::get_local_injector().inject(operation,
            [operation] { throw std::runtime_error(std::string(operation)); });
}

namespace db {

namespace view {

view_consumer::view_consumer(shared_ptr<view_update_generator> gen, gc_clock::time_point now, abort_source& as)
    : _gen(std::move(gen))
    , _now(now)
    , _as(as) {}

void view_consumer::add_fragment(auto&& fragment) {
    _fragments_memory_usage += fragment.memory_usage(*reader().schema());
    _fragments.emplace_back(*reader().schema(), permit(), std::move(fragment));
    if (_fragments_memory_usage > view_builder::batch_memory_max) {
        // Although we have not yet completed the batch of base rows that
        // compact_for_query<> planned for us (view_builder::batchsize),
        // we've still collected enough rows to reach sizeable memory use,
        // so let's flush these rows now.
        flush_fragments();
    }
}

void view_consumer::flush_fragments() {
    inject_failure("view_builder_flush_fragments");
    _as.check();
    if (!_fragments.empty()) {
        _fragments.emplace_front(*reader().schema(), permit(), partition_start(get_current_key(), tombstone()));
        auto base_schema = base()->schema();
        auto fragments_reader = make_mutation_reader_from_fragments(reader().schema(), permit(), std::move(_fragments));
        auto close_reader = defer([&fragments_reader] { fragments_reader.close().get(); });
        fragments_reader.upgrade_schema(base_schema);
        _gen->populate_views(
                *base(),
                _views_to_build,
                get_current_key().token(),
                std::move(fragments_reader),
                _now).get();
        close_reader.cancel();
        _fragments.clear();
        _fragments_memory_usage = 0;
    } 
}

stop_iteration view_consumer::consume_new_partition(const dht::decorated_key& dk) {
    inject_failure("view_builder_consume_new_partition");
    if (dk.key().is_empty()) {
        on_internal_error(vc_logger, format("Trying to consume empty partition key {}", dk));
    }
    set_current_key(std::move(dk));
    check_for_built_views();
    _views_to_build.clear();
    load_views_to_build();
    return stop_iteration(_views_to_build.empty());
}

stop_iteration view_consumer::consume(tombstone) {
    inject_failure("view_builder_consume_tombstone");
    return stop_iteration::no;
}

stop_iteration view_consumer::consume(static_row&& sr, tombstone, bool) {
    inject_failure("view_builder_consume_static_row");
    if (_views_to_build.empty() || _as.abort_requested()) {
        return stop_iteration::yes;
    }

    add_fragment(std::move(sr));
    return stop_iteration::no;
}

stop_iteration view_consumer::consume(clustering_row&& cr, row_tombstone, bool is_live) {
    inject_failure("view_builder_consume_clustering_row");
    if (!is_live) {
        return stop_iteration::no;
    }
    if (_views_to_build.empty() || _as.abort_requested()) {
        return stop_iteration::yes;
    }

    add_fragment(std::move(cr));
    return stop_iteration::no;
}

stop_iteration view_consumer::consume(range_tombstone_change&&) {
    inject_failure("view_builder_consume_range_tombstone");
    return stop_iteration::no;
}

stop_iteration view_consumer::consume_end_of_partition() {
    inject_failure("view_builder_consume_end_of_partition");
    utils::get_local_injector().inject("view_builder_consume_end_of_partition_delay", utils::wait_for_message(std::chrono::seconds(60))).get();
    flush_fragments();
    return stop_iteration(should_stop_consuming_end_of_partition());
}

future<> flush_base(lw_shared_ptr<replica::column_family> base, abort_source& as) {
    struct empty_state { };
    return exponential_backoff_retry::do_until_value(1s, 1min, as, [base = std::move(base)] {
        return base->flush().then_wrapped([base] (future<> f) -> std::optional<empty_state> {
            if (f.failed()) {
                vc_logger.error("Error flushing base table {}.{}: {}; retrying", base->schema()->ks_name(), base->schema()->cf_name(), f.get_exception());
                return { };
            }
            return { empty_state{} };
        });
    }).discard_result();
}

query::partition_slice make_partition_slice(const schema& s) {
    query::partition_slice::option_set opts;
    opts.set(query::partition_slice::option::send_partition_key);
    opts.set(query::partition_slice::option::send_clustering_key);
    opts.set(query::partition_slice::option::send_timestamp);
    opts.set(query::partition_slice::option::send_ttl);
    opts.set(query::partition_slice::option::always_return_static_content);
    return query::partition_slice(
            {query::full_clustering_range},
            s.static_columns()
                    | std::views::transform(std::mem_fn(&column_definition::id))
                    | std::ranges::to<query::column_id_vector>(),
            s.regular_columns()
                    | std::views::transform(std::mem_fn(&column_definition::id))
                    | std::ranges::to<query::column_id_vector>(),
            std::move(opts));
}

}

}
