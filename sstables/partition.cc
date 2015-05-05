/*
 * Copyright 2015 Cloudius Systems
 */
#include "mutation.hh"
#include "sstables.hh"
#include "types.hh"
#include "core/future-util.hh"
#include "key.hh"
#include "keys.hh"
#include "core/do_with.hh"

#include "dht/i_partitioner.hh"

namespace sstables {

/**
 * @returns: >= 0, if key is found. That is the index where the key is found.
 *             -1, if key is not found, and is smaller than the first key in the list.
 *          <= -2, if key is not found, but is greater than one of the keys. By adding 2 and
 *                 negating, one can determine the index before which the key would have to
 *                 be inserted.
 *
 * Origin uses this slightly modified binary search for the Summary, that will
 * indicate in which bucket the element would be in case it is not a match.
 *
 * For the Index entries, it uses a "normal", java.lang binary search. Because
 * we have made the explicit decision to open code the comparator for
 * efficiency, using a separate binary search would be possible, but very
 * messy.
 *
 * It's easier to reuse the same code for both binary searches, and just ignore
 * the extra information when not needed.
 *
 * This code should work in all kinds of vectors in whose's elements is possible to aquire
 * a key view.
 */
template <typename T>
int sstable::binary_search(const T& entries, const key& sk, const dht::token& token) {
    int low = 0, mid = entries.size(), high = mid - 1, result = -1;

    auto& partitioner = dht::global_partitioner();
    auto sk_bytes = bytes_view(sk);

    while (low <= high) {
        // The token comparison should yield the right result most of the time.
        // So we avoid expensive copying operations that happens at key
        // creation by keeping only a key view, and then manually carrying out
        // both parts of the comparison ourselves.
        mid = low + ((high - low) >> 1);
        auto mid_bytes = bytes_view(entries[mid]);
        auto mid_key = key_view(mid_bytes);
        auto mid_token = partitioner.get_token(mid_key);

        if (token == mid_token) {
            result = compare_unsigned(sk_bytes, mid_bytes);
        } else {
            result = token < mid_token ? -1 : 1;
        }

        if (result > 0) {
            low = mid + 1;
        } else if (result < 0) {
            high = mid - 1;
        } else {
            return mid;
        }
    }

    return -mid - (result < 0 ? 1 : 2);
}

// Force generation, so we make it available outside this compilation unit without moving that
// much code to .hh
template int sstable::binary_search<>(const std::vector<summary_entry>& entries, const key& sk);
template int sstable::binary_search<>(const std::vector<index_entry>& entries, const key& sk);

class mp_row_consumer : public row_consumer {
    schema_ptr _schema;
    key _key;

    struct column {
        bool is_static;
        bytes_view col_name;
        std::vector<bytes> clustering;
        bytes cell;
        const column_definition *cdef;

        static constexpr size_t static_size = 2;

        bool check_static(bytes_view col) const {
            static bytes static_row(static_size, 0xff);
            return col.compare(0, static_size, static_row) == 0;
        }

        bytes_view fix_static_name(bytes_view col) {
            if (is_static) {
                col.remove_prefix(static_size);
            }
            return col;
        }

        column(schema_ptr schema, bytes_view col)
            : is_static(check_static(col))
            , col_name(fix_static_name(col))
            , clustering(composite_view(col_name).explode())
            , cell(std::move(clustering.back()))
            , cdef(schema->get_column_definition(cell))
        {

            if (is_static) {
                for (auto& e: clustering) {
                    if (e.size() != 0) {
                        throw malformed_sstable_exception("Static row has clustering key information. I didn't expect that!");
                    }
                }
            }

            if (cell.size() && !cdef) {
                throw malformed_sstable_exception(sprint("schema does not contain colum: %s", cell.c_str()));
            }

            clustering.pop_back();
        }
    };
public:
    lw_shared_ptr<mutation> mut;

    mp_row_consumer(const key& key, const schema_ptr _schema)
            : _schema(_schema)
            , _key(key)
            , mut(make_lw_shared<mutation>(partition_key::from_exploded(*_schema, key.explode(*_schema)), _schema))
    { }

    void validate_row_marker() {
        if (_schema->is_dense()) {
            throw malformed_sstable_exception("row marker found in dense table");
        }
    }

    virtual void consume_row_start(bytes_view key, sstables::deletion_time deltime) override {
        // FIXME: We should be doing more than that: We need to check the deletion time and propagate the tombstone information
        auto k = bytes_view(_key);
        if (key != k) {
            throw malformed_sstable_exception(sprint("Key mismatch. Got %s while processing %s", to_hex(key).c_str(), to_hex(k).c_str()));
        }
    }

    virtual void consume_cell(bytes_view col_name, bytes_view value, uint64_t timestamp, uint32_t ttl, uint32_t expiration) override {
        static bytes cql_row_marker(3, bytes::value_type(0x0));

        // The row marker exists mainly so that one can create empty rows. It should not be present
        // in dense tables. It serializes to \x0\x0\x0 and should yield an empty vector.
        // FIXME: What to do with its timestamp ? We are not setting any row-wide timestamp in the mutation partition
        if (col_name == cql_row_marker) {
            validate_row_marker();
            return;
        }

        struct column col(_schema, col_name);

        // FIXME: collections are different, but not yet handled.
        if (col.clustering.size() > (_schema->clustering_key_type()->types().size() + 1)) {
            throw malformed_sstable_exception("wrong number of clustering columns");
        }

        ttl_opt opt;
        if (ttl) {
            gc_clock::duration secs(expiration);
            auto tp = gc_clock::time_point(secs);
            if (tp < gc_clock::now()) {
                consume_deleted_cell(col, timestamp, tp);
                return;
            }

            opt = ttl_opt(tp);
        } else {
            opt = {};
        }

        auto ac = atomic_cell::make_live(timestamp, opt, value);

        if (col.is_static) {
            mut->set_static_cell(*(col.cdef), ac);
            return;
        }

        auto clustering_prefix = exploded_clustering_prefix(std::move(col.clustering));

        if (col.cell.size() == 0) {
            auto clustering_key = clustering_key::from_clustering_prefix(*_schema, clustering_prefix);
            auto& dr = mut->partition().clustered_row(clustering_key);
            dr.created_at = timestamp;
            return;
        }

        mut->set_cell(clustering_prefix, *(col.cdef), atomic_cell_or_collection(ac));
    }

    virtual void consume_deleted_cell(bytes_view col_name, sstables::deletion_time deltime) override {
        struct column col(_schema, col_name);
        gc_clock::duration secs(deltime.local_deletion_time);

        consume_deleted_cell(col, deltime.marked_for_delete_at, gc_clock::time_point(secs));
    }

    void consume_deleted_cell(column &col, uint64_t timestamp, gc_clock::time_point ttl) {
        auto ac = atomic_cell::make_dead(timestamp, ttl);

        if (col.is_static) {
            mut->set_static_cell(*(col.cdef), atomic_cell_or_collection(ac));
        } else {
            auto clustering_prefix = exploded_clustering_prefix(std::move(col.clustering));
            mut->set_cell(clustering_prefix, *(col.cdef), atomic_cell_or_collection(ac));
        }
    }
    virtual void consume_row_end() override {
    }

    virtual void consume_range_tombstone(
            bytes_view start_col, bytes_view end_col,
            sstables::deletion_time deltime) override {
        throw runtime_exception("Not implemented");
    }
};


future<lw_shared_ptr<mutation>>
sstables::sstable::convert_row(schema_ptr schema, const sstables::key& key) {

    assert(schema);

    auto& partitioner = dht::global_partitioner();
    auto token = partitioner.get_token(key_view(key));

    if (!filter_has_key(token)) {
        return make_ready_future<lw_shared_ptr<mutation>>();
    }

    auto& summary = _summary;
    auto summary_idx = binary_search(summary.entries, key, token);
    if (summary_idx < 0) {
        // binary search gives us the first index _greater_ than the key searched for,
        // i.e., its insertion position
        auto gt = (summary_idx + 1) * -1;
        summary_idx = gt - 1;
    }
    if (summary_idx < 0) {
        return make_ready_future<lw_shared_ptr<mutation>>();
    }

    auto position = _summary.entries[summary_idx].position;
    return read_indexes(position).then([this, schema, &key, token] (auto index_list) {
        auto index_idx = this->binary_search(index_list, key, token);
        if (index_idx < 0) {
            return make_ready_future<lw_shared_ptr<mutation>>();
        }

        auto position = index_list[index_idx].position;
        size_t end;
        if (size_t(index_idx + 1) < index_list.size()) {
            end = index_list[index_idx + 1].position;
        } else {
            end = this->data_size();
        }

        return do_with(mp_row_consumer(key, schema), [this, position, end] (auto& c) {
            return this->data_consume_rows_at_once(c, position, end).then([&c] {
                return make_ready_future<lw_shared_ptr<mutation>>(c.mut);
            });
        });
    });
}
}
