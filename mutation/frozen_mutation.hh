/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "dht/i_partitioner.hh"
#include "replica/database_fwd.hh"
#include "mutation_fragment.hh"
#include "mutation_fragment_v2.hh"
#include "mutation_consumer_concepts.hh"
#include "range_tombstone_list.hh"
#include "schema/schema.hh"

class mutation;
class mutation_reader;
class mutation_partition_view;

namespace ser {
class mutation_view;
}

template<typename Result>
struct frozen_mutation_consume_result {
    stop_iteration stop;
    Result result;
};

template<>
struct frozen_mutation_consume_result<void> {
    stop_iteration stop;
};

template<FlattenedConsumerV2 Consumer>
class frozen_mutation_consumer_adaptor;

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
    explicit frozen_mutation(const partition_key& key) : _pk(key) {}
    explicit frozen_mutation(const mutation& m);
    explicit frozen_mutation(bytes_ostream&& b);
    frozen_mutation(bytes_ostream&& b, partition_key key);
    frozen_mutation(frozen_mutation&& m) = default;
    frozen_mutation(const frozen_mutation& m) = default;
    frozen_mutation& operator=(frozen_mutation&&) = default;
    frozen_mutation& operator=(const frozen_mutation&) = default;
    bytes_ostream& representation() { return _bytes; }
    const bytes_ostream& representation() const { return _bytes; }
    table_id column_family_id() const;
    table_schema_version schema_version() const; // FIXME: Should replace column_family_id()
    partition_key_view key() const;
    dht::decorated_key decorated_key(const schema& s) const;
    mutation_partition_view partition() const;
    // The supplied schema must be of the same version as the schema of
    // the mutation which was used to create this instance.
    // throws schema_mismatch_error otherwise.
    mutation unfreeze(schema_ptr s) const;

    // Automatically upgrades the stored mutation to the supplied schema with custom column mapping.
    mutation unfreeze_upgrading(schema_ptr schema, const column_mapping& cm) const;

    // Consumes the frozen mutation's content.
    //
    // The consume operation is stoppable:
    // * To stop, return stop_iteration::yes from one of the consume() methods;
    // * The consume will now stop and return;
    //
    // Note that `consume_end_of_partition()` and `consume_end_of_stream()`
    // will be called each time the consume is stopping, regardless of whether
    // you are pausing or the consumption is ending for good.
    template<FlattenedConsumerV2 Consumer>
    auto consume(schema_ptr s, Consumer& consumer) const -> frozen_mutation_consume_result<decltype(consumer.consume_end_of_stream())>;

    template<FlattenedConsumerV2 Consumer>
    auto consume(schema_ptr s, frozen_mutation_consumer_adaptor<Consumer>& adaptor) const -> frozen_mutation_consume_result<decltype(adaptor.consumer().consume_end_of_stream())>;

    // Consumes the frozen mutation's content.
    //
    // The consume operation is stoppable:
    // * To stop, return stop_iteration::yes from one of the consume() methods;
    // * The consume will now stop and return;
    //
    // Note that `consume_end_of_partition()` and `consume_end_of_stream()`
    // will be called each time the consume is stopping, regardless of whether
    // you are pausing or the consumption is ending for good.
    template<FlattenedConsumerV2 Consumer>
    auto consume_gently(schema_ptr s, Consumer& consumer) const -> future<frozen_mutation_consume_result<decltype(consumer.consume_end_of_stream())>>;

    template<FlattenedConsumerV2 Consumer>
    auto consume_gently(schema_ptr s, frozen_mutation_consumer_adaptor<Consumer>& adaptor) const -> future<frozen_mutation_consume_result<decltype(adaptor.consumer().consume_end_of_stream())>>;

    dht::token token(const schema& s) const {
        return dht::get_token(s, key());
    }

    struct printer {
        const frozen_mutation& self;
        schema_ptr schema;
    };

    // Same requirements about the schema as unfreeze().
    printer pretty_printer(schema_ptr) const;
};

frozen_mutation freeze(const mutation& m);
utils::chunked_vector<frozen_mutation> freeze(const utils::chunked_vector<mutation>&);
utils::chunked_vector<mutation> unfreeze(const utils::chunked_vector<frozen_mutation>&);

struct frozen_mutation_and_schema {
    frozen_mutation fm;
    schema_ptr s;
};

class streamed_mutation_freezer {
    const schema& _schema;
    partition_key _key;

    tombstone _partition_tombstone;
    std::optional<static_row> _sr;
    utils::chunked_vector<clustering_row> _crs;
    range_tombstone_list _rts;
public:
    streamed_mutation_freezer(const schema& s, const partition_key& key)
        : _schema(s), _key(key), _rts(s) { }

    stop_iteration consume(tombstone pt);

    stop_iteration consume(static_row&& sr);
    stop_iteration consume(clustering_row&& cr);

    stop_iteration consume(range_tombstone&& rt);

    frozen_mutation consume_end_of_stream();
};

static constexpr size_t default_frozen_fragment_size = 128 * 1024;

using frozen_mutation_consumer_fn = std::function<future<stop_iteration>(frozen_mutation, bool)>;
future<> fragment_and_freeze(mutation_reader mr, frozen_mutation_consumer_fn c,
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

class frozen_mutation_fragment_v2 {
    bytes_ostream _bytes;
public:
    explicit frozen_mutation_fragment_v2(bytes_ostream bytes) : _bytes(std::move(bytes)) { }
    const bytes_ostream& representation() const { return _bytes; }
    bytes_ostream&& representation() && { return std::move(_bytes); }

    mutation_fragment_v2 unfreeze(const schema& s, reader_permit permit);

    future<> clear_gently() noexcept {
        return _bytes.clear_gently();
    }
};

frozen_mutation_fragment_v2 freeze(const schema& s, const mutation_fragment_v2& mf);

template <> struct fmt::formatter<frozen_mutation::printer> : fmt::formatter<string_view> {
    auto format(const frozen_mutation::printer& pr, fmt::format_context& ctx) const -> decltype(ctx.out());
};
