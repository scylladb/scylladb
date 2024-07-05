/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/mutation.hh"
#include "mutation/mutation_rebuilder.hh"

// A StreamedMutationConsumer which distributes fragments randomly into several mutations.
class fragment_scatterer {
    schema_ptr _schema;
    size_t _n;
    std::vector<mutation_rebuilder_v2> _mutations;
    size_t _next = 0;
    std::optional<size_t> _last_rt;
private:
    void for_each_target(noncopyable_function<void (mutation_rebuilder_v2&)> func) {
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
            _mutations.emplace_back(_schema);
            _mutations.back().consume_new_partition(dk);
        }
    }

    stop_iteration consume(tombstone t) {
        for_each_target([&] (mutation_rebuilder_v2& m) {
            m.consume(t);
        });
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone_change&& rtc) {
        if (_last_rt) {
            _mutations[*_last_rt].consume(range_tombstone_change(rtc.position(), {}));
        }
        if (rtc.tombstone()) {
            const auto i = _next % _mutations.size();
            _mutations[i].consume(std::move(rtc));
            _last_rt = i;
        } else {
            _last_rt.reset();
        }
        ++_next;
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        for_each_target([&] (mutation_rebuilder_v2& m) {
            m.consume(std::move(sr));
        });
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        for_each_target([&] (mutation_rebuilder_v2& m) {
            m.consume(std::move(cr));
        });
        return stop_iteration::no;
    }

    stop_iteration consume_end_of_partition() {
        return stop_iteration::no;
    }

    std::vector<mutation> consume_end_of_stream() {
        std::vector<mutation> muts;
        muts.reserve(_mutations.size());
        for (auto& mut_builder : _mutations) {
            muts.emplace_back(*mut_builder.consume_end_of_stream());
        }
        return muts;
    }
};
