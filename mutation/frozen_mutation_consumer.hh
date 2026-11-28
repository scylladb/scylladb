/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "frozen_mutation.hh"
#include "mutation_partition_view.hh"
#include "range_tombstone_change_generator.hh"

// mutation_partition_view visitor which consumes a frozen_mutation.
template<FlattenedConsumerV2 Consumer>
class frozen_mutation_consumer_adaptor final : public mutation_partition_view_virtual_visitor {
private:
    const schema& _schema;
    std::optional<dht::decorated_key> _dk;
    lazy_row _static_row;
    range_tombstone_change_generator _rt_gen;
    alloc_strategy_unique_ptr<rows_entry> _current_row_entry;
    deletable_row* _current_row = nullptr;
    Consumer& _consumer;
    stop_iteration _stop_consuming = stop_iteration::no;
    // Builds the cells of the static row or the current clustering row, a block at a time.
    std::optional<row::cell_appender> _cells;

    row::cell_appender& cells(row& r, column_kind kind) {
        if (!_cells) {
            _cells.emplace(r, _schema, kind);
        }
        return *_cells;
    }

    stop_iteration flush_rows_and_tombstones(position_in_partition_view pos) {
        if (_cells) {
            _cells->finish();
            _cells.reset();
        }
        if (!_static_row.empty()) {
            auto row = std::move(_static_row.get_existing());
            _stop_consuming = _consumer.consume(static_row(std::move(row)));
            if (_stop_consuming) {
                return _stop_consuming;
            }
        }
        if (_current_row) {
            auto row_entry = std::move(_current_row_entry);
            _current_row = nullptr;
            _stop_consuming = _consumer.consume(clustering_row(std::move(*row_entry)));
            if (_stop_consuming) {
                return _stop_consuming;
            }
        }
        _rt_gen.flush(pos, [this] (range_tombstone_change rtc) {
            _stop_consuming = _consumer.consume(std::move(rtc));
        });
        return _stop_consuming;
    }

public:
    frozen_mutation_consumer_adaptor(schema_ptr s, Consumer& consumer)
        : _schema(*s)
        , _rt_gen(_schema)
        , _consumer(consumer)
    {
    }

    Consumer& consumer() {
        return _consumer;
    }

    void on_new_partition(const partition_key& key) {
        _rt_gen.reset();
        _dk = dht::decorate_key(_schema, key);
        _consumer.consume_new_partition(*_dk);
    }

    virtual void accept_partition_tombstone(tombstone t) override {
        _consumer.consume(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell cell) override {
        cells(_static_row.maybe_create(), column_kind::static_column).append(id, atomic_cell_or_collection(std::move(cell)));
    }

    virtual void accept_static_cell(column_id id, collection_mutation collection) override {
        cells(_static_row.maybe_create(), column_kind::static_column).append(id, std::move(collection));
    }

    virtual stop_iteration accept_row_tombstone(range_tombstone rt) override {
        flush_rows_and_tombstones(rt.position());
        _rt_gen.consume(std::move(rt));
        return _stop_consuming;
    }

    virtual stop_iteration accept_row(position_in_partition_view key, row_tombstone deleted_at, row_marker rm, is_dummy dummy, is_continuous continuous) override {
        if (flush_rows_and_tombstones(key)) {
            return stop_iteration::yes;
        }
        _current_row_entry = alloc_strategy_unique_ptr<rows_entry>(current_allocator().construct<rows_entry>(_schema, key, dummy, continuous));
        deletable_row& r = _current_row_entry->row();
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
        return stop_iteration::no;
    }

    void accept_row_cell(column_id id, atomic_cell cell) override {
        cells(_current_row->cells(), column_kind::regular_column).append(id, std::move(cell));
    }

    virtual void accept_row_cell(column_id id, collection_mutation collection) override {
        cells(_current_row->cells(), column_kind::regular_column).append(id, std::move(collection));
    }

    auto on_end_of_partition() {
        flush_rows_and_tombstones(position_in_partition::after_all_clustered_rows());
        _stop_consuming = _consumer.consume_end_of_partition();
        using consume_res_type = decltype(_consumer.consume_end_of_stream());
        if constexpr (std::is_same_v<consume_res_type, void>) {
            _consumer.consume_end_of_stream();
            return frozen_mutation_consume_result<void>{_stop_consuming};
        } else {
            return frozen_mutation_consume_result<consume_res_type>{_stop_consuming, _consumer.consume_end_of_stream()};
        }
    }
};

template<FlattenedConsumerV2 Consumer>
auto frozen_mutation::consume(schema_ptr s, frozen_mutation_consumer_adaptor<Consumer>& adaptor) const -> frozen_mutation_consume_result<decltype(adaptor.consumer().consume_end_of_stream())> {
    check_schema_version(schema_version(), *s);
    try {
        adaptor.on_new_partition(_pk);
        partition().accept_ordered(*s, adaptor);
        return adaptor.on_end_of_partition();
    } catch (...) {
        std::throw_with_nested(std::runtime_error(format(
                "frozen_mutation::consume(): failed consuming mutation {} of {}.{}", key(), s->ks_name(), s->cf_name())));
    }
}

template<FlattenedConsumerV2 Consumer>
auto frozen_mutation::consume(schema_ptr s, Consumer& consumer) const -> frozen_mutation_consume_result<decltype(consumer.consume_end_of_stream())> {
    frozen_mutation_consumer_adaptor adaptor(s, consumer);
    return consume(s, adaptor);
}

template<FlattenedConsumerV2 Consumer>
auto frozen_mutation::consume_gently(schema_ptr s, frozen_mutation_consumer_adaptor<Consumer>& adaptor) const -> future<frozen_mutation_consume_result<decltype(adaptor.consumer().consume_end_of_stream())>> {
    check_schema_version(schema_version(), *s);
    try {
        adaptor.on_new_partition(_pk);
        auto p = partition();
        co_await p.accept_gently_ordered(*s, adaptor);
        co_return adaptor.on_end_of_partition();
    } catch (...) {
        std::throw_with_nested(std::runtime_error(format(
                "frozen_mutation::consume_gently(): failed consuming mutation {} of {}.{}", key(), s->ks_name(), s->cf_name())));
    }
}

template<FlattenedConsumerV2 Consumer>
auto frozen_mutation::consume_gently(schema_ptr s, Consumer& consumer) const -> future<frozen_mutation_consume_result<decltype(consumer.consume_end_of_stream())>> {
    frozen_mutation_consumer_adaptor adaptor(s, consumer);
    co_return co_await consume_gently(s, adaptor);
}
