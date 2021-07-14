/*
 * Copyright (C) 2019-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <boost/range/adaptor/indirected.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/find_if.hpp>
#include <boost/range/algorithm/sort.hpp>

#include "clustering_bounds_comparator.hh"
#include "database_fwd.hh"
#include "db/system_keyspace.hh"
#include "dht/i_partitioner.hh"
#include "partition_range_compat.hh"
#include "range.hh"
#include "mutation_fragment.hh"
#include "sstables/sstables.hh"
#include "db/timeout_clock.hh"
#include "database.hh"

#include "db/size_estimates_virtual_reader.hh"

namespace db {

namespace size_estimates {

struct virtual_row {
    const bytes& cf_name;
    const token_range& tokens;
    clustering_key_prefix as_key() const {
        return clustering_key_prefix::from_exploded(std::vector<bytes_view>{cf_name, tokens.start, tokens.end});
    }
};

struct virtual_row_comparator {
    schema_ptr _schema;
    virtual_row_comparator(schema_ptr schema) : _schema(schema) { }
    bool operator()(const clustering_key_prefix& key1, const clustering_key_prefix& key2) {
        return clustering_key_prefix::prefix_equality_less_compare(*_schema)(key1, key2);
    }
    bool operator()(const virtual_row& row, const clustering_key_prefix& key) {
        return operator()(row.as_key(), key);
    }
    bool operator()(const clustering_key_prefix& key, const virtual_row& row) {
        return operator()(key, row.as_key());
    }
};

// Iterating over the cartesian product of cf_names and token_ranges.
class virtual_row_iterator {
public:
    using iterator_category = std::input_iterator_tag;
    using value_type = const virtual_row;
    using difference_type = std::ptrdiff_t;
    using pointer = const virtual_row*;
    using reference = const virtual_row&;
private:
    std::reference_wrapper<const std::vector<bytes>> _cf_names;
    std::reference_wrapper<const std::vector<token_range>> _ranges;
    size_t _cf_names_idx = 0;
    size_t _ranges_idx = 0;
public:
    struct end_iterator_tag {};
    virtual_row_iterator(const std::vector<bytes>& cf_names, const std::vector<token_range>& ranges)
            : _cf_names(std::ref(cf_names))
            , _ranges(std::ref(ranges))
    { }
    virtual_row_iterator(const std::vector<bytes>& cf_names, const std::vector<token_range>& ranges, end_iterator_tag)
            : _cf_names(std::ref(cf_names))
            , _ranges(std::ref(ranges))
            , _cf_names_idx(cf_names.size())
            , _ranges_idx(ranges.size())
    {
        if (cf_names.empty() || ranges.empty()) {
            // The product of an empty range with any range is an empty range.
            // In this case we want the end iterator to be equal to the begin iterator,
            // which has_ranges_idx = _cf_names_idx = 0.
            _ranges_idx = _cf_names_idx = 0;
        }
    }
    virtual_row_iterator& operator++() {
        if (++_ranges_idx == _ranges.get().size() && ++_cf_names_idx < _cf_names.get().size()) {
            _ranges_idx = 0;
        }
        return *this;
    }
    virtual_row_iterator operator++(int) {
        virtual_row_iterator i(*this);
        ++(*this);
        return i;
    }
    const value_type operator*() const {
        return { _cf_names.get()[_cf_names_idx], _ranges.get()[_ranges_idx] };
    }
    bool operator==(const virtual_row_iterator& i) const {
        return _cf_names_idx == i._cf_names_idx
            && _ranges_idx == i._ranges_idx;
    }
    bool operator!=(const virtual_row_iterator& i) const {
        return !(*this == i);
    }
};

/**
 * Returns the keyspaces, ordered by name, as selected by the partition_range.
 */
static std::vector<sstring> get_keyspaces(const schema& s, const database& db, dht::partition_range range) {
    struct keyspace_less_comparator {
        const schema& _s;
        keyspace_less_comparator(const schema& s) : _s(s) { }
        dht::ring_position as_ring_position(const sstring& ks) {
            auto pkey = partition_key::from_single_value(_s, utf8_type->decompose(ks));
            return dht::decorate_key(_s, std::move(pkey));
        }
        bool operator()(const sstring& ks1, const sstring& ks2) {
            return as_ring_position(ks1).less_compare(_s, as_ring_position(ks2));
        }
        bool operator()(const sstring& ks, const dht::ring_position& rp) {
            return as_ring_position(ks).less_compare(_s, rp);
        }
        bool operator()(const dht::ring_position& rp, const sstring& ks) {
            return rp.less_compare(_s, as_ring_position(ks));
        }
    };
    auto keyspaces = db.get_non_system_keyspaces();
    auto cmp = keyspace_less_comparator(s);
    boost::sort(keyspaces, cmp);
    return boost::copy_range<std::vector<sstring>>(
        range.slice(keyspaces, std::move(cmp)) | boost::adaptors::filtered([&s] (const auto& ks) {
            // If this is a range query, results are divided between shards by the partition key (keyspace_name).
            return shard_of(s, dht::get_token(s,
                        partition_key::from_single_value(s, utf8_type->decompose(ks))))
                == this_shard_id();
        })
    );
}

/**
 * Makes a wrapping range of ring_position from a nonwrapping range of token, used to select sstables.
 */
static dht::partition_range as_ring_position_range(dht::token_range& r) {
    std::optional<range<dht::ring_position>::bound> start_bound, end_bound;
    if (r.start()) {
        start_bound = {{ dht::ring_position(r.start()->value(), dht::ring_position::token_bound::start), r.start()->is_inclusive() }};
    }
    if (r.end()) {
        end_bound = {{ dht::ring_position(r.end()->value(), dht::ring_position::token_bound::end), r.end()->is_inclusive() }};
    }
    return dht::partition_range(std::move(start_bound), std::move(end_bound), r.is_singular());
}

/**
 * Add a new range_estimates for the specified range, considering the sstables associated with `cf`.
 */
static system_keyspace::range_estimates estimate(const column_family& cf, const token_range& r) {
    int64_t count{0};
    utils::estimated_histogram hist{0};
    auto from_bytes = [] (auto& b) {
        return dht::token::from_sstring(utf8_type->to_string(b));
    };
    dht::token_range_vector ranges;
    ::compat::unwrap_into(
        wrapping_range<dht::token>({{ from_bytes(r.start), false }}, {{ from_bytes(r.end) }}),
        dht::token_comparator(),
        [&] (auto&& rng) { ranges.push_back(std::move(rng)); });
    for (auto&& r : ranges) {
        auto rp_range = as_ring_position_range(r);
        for (auto&& sstable : cf.select_sstables(rp_range)) {
            count += sstable->estimated_keys_for_range(r);
            hist.merge(sstable->get_stats_metadata().estimated_partition_size);
        }
    }
    return {cf.schema(), r.start, r.end, count, count > 0 ? hist.mean() : 0};
}

/**
 * Returns the primary ranges for the local node.
 */
static future<std::vector<token_range>> get_local_ranges(database& db) {
    return db::system_keyspace::get_local_tokens().then([&db] (auto&& tokens) {
        auto ranges = db.get_token_metadata().get_primary_ranges_for(std::move(tokens));
        std::vector<token_range> local_ranges;
        auto to_bytes = [](const std::optional<dht::token_range::bound>& b) {
            assert(b);
            return utf8_type->decompose(b->value().to_sstring());
        };
        // We merge the ranges to be compatible with how Cassandra shows it's size estimates table.
        // All queries will be on that table, where all entries are text and there's no notion of
        // token ranges form the CQL point of view.
        auto left_inf = boost::find_if(ranges, [] (auto&& r) {
            return r.end() && (!r.start() || r.start()->value() == dht::minimum_token());
        });
        auto right_inf = boost::find_if(ranges, [] (auto&& r) {
            return r.start() && (!r.end() || r.end()->value() == dht::maximum_token());
        });
        if (left_inf != right_inf && left_inf != ranges.end() && right_inf != ranges.end()) {
            local_ranges.push_back(token_range{to_bytes(right_inf->start()), to_bytes(left_inf->end())});
            ranges.erase(left_inf);
            ranges.erase(right_inf);
        }
        for (auto&& r : ranges) {
            local_ranges.push_back(token_range{to_bytes(r.start()), to_bytes(r.end())});
        }
        boost::sort(local_ranges, [] (auto&& tr1, auto&& tr2) {
            return utf8_type->less(tr1.start, tr2.start);
        });
        return local_ranges;
    });
}

future<std::vector<token_range>> test_get_local_ranges(database& db) {
    return get_local_ranges(db);
}

size_estimates_mutation_reader::size_estimates_mutation_reader(database& db, schema_ptr schema, reader_permit permit, const dht::partition_range& prange,
        const query::partition_slice& slice, streamed_mutation::forwarding fwd)
            : impl(std::move(schema), std::move(permit))
            , _db(db)
            , _prange(&prange)
            , _slice(slice)
            , _fwd(fwd)
    { }

future<> size_estimates_mutation_reader::get_next_partition() {
    if (!_keyspaces) {
        _keyspaces = get_keyspaces(*_schema, _db, *_prange);
        _current_partition = _keyspaces->begin();
    }
    if (_current_partition == _keyspaces->end()) {
        _end_of_stream = true;
        return make_ready_future<>();
    }
    return do_with(reader_permit::blocked_guard(_permit), [this] (reader_permit::blocked_guard&) {
        return get_local_ranges(_db);
    }).then([this] (auto&& ranges) {
        auto estimates = this->estimates_for_current_keyspace(std::move(ranges));
        auto mutations = db::system_keyspace::make_size_estimates_mutation(*_current_partition, std::move(estimates));
        ++_current_partition;
        std::vector<mutation> ms;
        ms.emplace_back(std::move(mutations));
        auto reader = flat_mutation_reader_from_mutations(_permit, std::move(ms), _fwd);
        auto close_partition_reader = _partition_reader ? _partition_reader->close() : make_ready_future<>();
        return close_partition_reader.then([this, reader = std::move(reader)] () mutable {
            _partition_reader = std::move(reader);
        });
    });
}

future<> size_estimates_mutation_reader::close_partition_reader() noexcept {
    return _partition_reader ? _partition_reader->close() : make_ready_future<>();
}

future<> size_estimates_mutation_reader::fill_buffer(db::timeout_clock::time_point timeout) {
    return do_until([this, timeout] { return is_end_of_stream() || is_buffer_full(); }, [this, timeout] {
        if (!_partition_reader) {
            return get_next_partition();
        }
        return _partition_reader->consume_pausable([this] (mutation_fragment mf) {
            push_mutation_fragment(std::move(mf));
            return stop_iteration(is_buffer_full());
        }, timeout).then([this] {
            if (_partition_reader->is_end_of_stream() && _partition_reader->is_buffer_empty()) {
                return _partition_reader->close();
            }
            return make_ready_future<>();
        });
    });
}

future<> size_estimates_mutation_reader::next_partition() {
    clear_buffer_to_next_partition();
    if (is_buffer_empty()) {
        return close_partition_reader();
    }
    return make_ready_future<>();
}

future<> size_estimates_mutation_reader::fast_forward_to(const dht::partition_range& pr, db::timeout_clock::time_point timeout) {
    clear_buffer();
    _prange = &pr;
    _keyspaces = std::nullopt;
    _end_of_stream = false;
    return close_partition_reader();
}

future<> size_estimates_mutation_reader::fast_forward_to(position_range pr, db::timeout_clock::time_point timeout) {
    forward_buffer_to(pr.start());
    _end_of_stream = false;
    if (_partition_reader) {
        return _partition_reader->fast_forward_to(std::move(pr), timeout);
    }
    return make_ready_future<>();
}

future<> size_estimates_mutation_reader::close() noexcept {
    return close_partition_reader();
}

std::vector<db::system_keyspace::range_estimates>
size_estimates_mutation_reader::estimates_for_current_keyspace(std::vector<token_range> local_ranges) const {
    // For each specified range, estimate (crudely) mean partition size and partitions count.
    auto pkey = partition_key::from_single_value(*_schema, utf8_type->decompose(*_current_partition));
    auto cfs = _db.find_keyspace(*_current_partition).metadata()->cf_meta_data();
    auto cf_names = boost::copy_range<std::vector<bytes>>(cfs | boost::adaptors::transformed([] (auto&& cf) {
        return utf8_type->decompose(cf.first);
    }));
    boost::sort(cf_names, [] (auto&& n1, auto&& n2) {
        return utf8_type->less(n1, n2);
    });
    std::vector<db::system_keyspace::range_estimates> estimates;
    for (auto& range : _slice.row_ranges(*_schema, pkey)) {
        auto rows = boost::make_iterator_range(
                virtual_row_iterator(cf_names, local_ranges),
                virtual_row_iterator(cf_names, local_ranges, virtual_row_iterator::end_iterator_tag()));
        auto rows_to_estimate = range.slice(rows, virtual_row_comparator(_schema));
        for (auto&& r : rows_to_estimate) {
            auto& cf = _db.find_column_family(*_current_partition, utf8_type->to_string(r.cf_name));
            estimates.push_back(estimate(cf, r.tokens));
            if (estimates.size() >= _slice.partition_row_limit()) {
                return estimates;
            }
        }
    }
    return estimates;
}

} // namespace size_estimates

} // namespace db
