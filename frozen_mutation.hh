/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/i_partitioner.hh"
#include "replica/database_fwd.hh"
#include "mutation_fragment.hh"

class mutation;
class mutation_partition_view;
class flat_mutation_reader_v2;

namespace ser {
class mutation_view;
}

// Immutable, compact form of mutation.
//
// This form is primarily destined to be sent over the network channel.
// Regular mutation can't be deserialized because its complex data structures
// need schema reference at the time object is constructed. We can't lookup
// schema before we deserialize column family ID. Another problem is that even
// if we had the ID somehow, low level RPC layer doesn't know how to lookup
// the schema. Data can be wrapped in frozen_mutation without schema
// information, the schema is only needed to access some of the fields.
//
class frozen_mutation final {
private:
    bytes_ostream _bytes;
    partition_key _pk;
private:
    partition_key deserialize_key() const;
    ser::mutation_view mutation_view() const;
public:
    explicit frozen_mutation(const mutation& m);
    explicit frozen_mutation(bytes_ostream&& b);
    frozen_mutation(bytes_ostream&& b, partition_key key);
    frozen_mutation(frozen_mutation&& m) = default;
    frozen_mutation(const frozen_mutation& m) = default;
    frozen_mutation& operator=(frozen_mutation&&) = default;
    frozen_mutation& operator=(const frozen_mutation&) = default;
    const bytes_ostream& representation() const { return _bytes; }
    utils::UUID column_family_id() const;
    utils::UUID schema_version() const; // FIXME: Should replace column_family_id()
    partition_key_view key() const;
    dht::decorated_key decorated_key(const schema& s) const;
    mutation_partition_view partition() const;
    // The supplied schema must be of the same version as the schema of
    // the mutation which was used to create this instance.
    // throws schema_mismatch_error otherwise.
    mutation unfreeze(schema_ptr s) const;

    // Automatically upgrades the stored mutation to the supplied schema with custom column mapping.
    mutation unfreeze_upgrading(schema_ptr schema, const column_mapping& cm) const;

    struct printer {
        const frozen_mutation& self;
        schema_ptr schema;
        friend std::ostream& operator<<(std::ostream&, const printer&);
    };

    // Same requirements about the schema as unfreeze().
    printer pretty_printer(schema_ptr) const;
};

frozen_mutation freeze(const mutation& m);

struct frozen_mutation_and_schema {
    frozen_mutation fm;
    schema_ptr s;
};

// Can receive streamed_mutation in reversed order.
class streamed_mutation_freezer {
    const schema& _schema;
    partition_key _key;
    bool _reversed;

    tombstone _partition_tombstone;
    std::optional<static_row> _sr;
    std::deque<clustering_row> _crs;
    range_tombstone_list _rts;
public:
    streamed_mutation_freezer(const schema& s, const partition_key& key, bool reversed = false)
        : _schema(s), _key(key), _reversed(reversed), _rts(s) { }

    stop_iteration consume(tombstone pt);

    stop_iteration consume(static_row&& sr);
    stop_iteration consume(clustering_row&& cr);

    stop_iteration consume(range_tombstone&& rt);

    frozen_mutation consume_end_of_stream();
};

static constexpr size_t default_frozen_fragment_size = 128 * 1024;

using frozen_mutation_consumer_fn = std::function<future<stop_iteration>(frozen_mutation, bool)>;
future<> fragment_and_freeze(flat_mutation_reader_v2 mr, frozen_mutation_consumer_fn c,
                             size_t fragment_size = default_frozen_fragment_size);

class reader_permit;

class frozen_mutation_fragment {
    bytes_ostream _bytes;
public:
    explicit frozen_mutation_fragment(bytes_ostream bytes) : _bytes(std::move(bytes)) { }
    const bytes_ostream& representation() const { return _bytes; }

    mutation_fragment unfreeze(const schema& s, reader_permit permit);

    future<> clear_gently() noexcept {
        return _bytes.clear_gently();
    }
};

frozen_mutation_fragment freeze(const schema& s, const mutation_fragment& mf);

