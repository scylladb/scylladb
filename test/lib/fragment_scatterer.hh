/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation.hh"

// A StreamedMutationConsumer which distributes fragments randomly into several mutations.
class fragment_scatterer {
    schema_ptr _schema;
    size_t _n;
    std::vector<mutation> _mutations;
    size_t _next = 0;
private:
    void for_each_target(noncopyable_function<void (mutation&)> func) {
        // round-robin
        func(_mutations[_next % _mutations.size()]);
        ++_next;
    }
public:
    explicit fragment_scatterer(schema_ptr schema, size_t n) : _schema(std::move(schema)), _n(n)
    { }

    void consume_new_partition(const dht::decorated_key& dk) {
        _mutations.reserve(_n);
        for (size_t i = 0; i < _n; ++i) {
            _mutations.emplace_back(_schema, dk);
        }
    }

    stop_iteration consume(tombstone t) {
        for_each_target([&] (mutation& m) {
            m.partition().apply(t);
        });
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        for_each_target([&] (mutation& m) {
            m.partition().apply_row_tombstone(*m.schema(), std::move(rt));
        });
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        for_each_target([&] (mutation& m) {
            m.partition().static_row().apply(*m.schema(), column_kind::static_column, std::move(sr.cells()));
        });
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        for_each_target([&] (mutation& m) {
            auto& dr = m.partition().clustered_row(*m.schema(), std::move(cr.key()));
            dr.apply(cr.tomb());
            dr.apply(cr.marker());
            dr.cells().apply(*m.schema(), column_kind::regular_column, std::move(cr.cells()));
        });
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }

    std::vector<mutation> consume_end_of_stream() {
        return std::move(_mutations);
    }
};
