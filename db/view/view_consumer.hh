/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "db/view/view_update_generator.hh"
#include "mutation/mutation_fragment_v2.hh"

namespace db {

namespace view {

class view_update_generator;

// Must be called in the context of a seastar::thread.
class view_consumer {
protected:
    shared_ptr<view_update_generator> _gen;
    gc_clock::time_point _now;
    std::vector<view_ptr> _views_to_build;
    abort_source& _as;

    std::deque<mutation_fragment_v2> _fragments;
    // The compact_for_query<> that feeds this consumer is already configured
    // to feed us up to view_builder::batchsize (128) rows and not an entire
    // partition. Still, if rows contain large blobs, saving 128 of them in
    // _fragments may be too much. So we want to track _fragment's memory
    // usage, and flush the _fragments if it has grown too large.
    // Additionally, limiting _fragment's size also solves issue #4213:
    // A single view mutation can be as large as the size of the base rows
    // used to build it, and we cannot allow its serialized size to grow
    // beyond our limit on mutation size (by default 32 MB).
    size_t _fragments_memory_usage = 0;

    virtual void check_for_built_views() = 0;
    virtual void load_views_to_build() = 0;

    virtual bool should_stop_consuming_end_of_partition() = 0;

    virtual dht::decorated_key& get_current_key() = 0;
    virtual void set_current_key(dht::decorated_key key) = 0;

    virtual lw_shared_ptr<replica::table> base() = 0;
    virtual mutation_reader& reader() = 0;
    virtual reader_permit& permit() = 0;

    void add_fragment(auto&& fragment);
    void flush_fragments();

public:
    view_consumer(shared_ptr<view_update_generator> gen, gc_clock::time_point now, abort_source& as);
    
    stop_iteration consume_new_partition(const dht::decorated_key& dk);
    stop_iteration consume(tombstone);
    stop_iteration consume(static_row&& sr, tombstone, bool);
    stop_iteration consume(clustering_row&& cr, row_tombstone, bool is_live);
    stop_iteration consume(range_tombstone_change&&);
    stop_iteration consume_end_of_partition();
};

future<> flush_base(lw_shared_ptr<replica::column_family> base, abort_source& as);

query::partition_slice make_partition_slice(const schema& s);

}

}
