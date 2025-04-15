#pragma once

#include "repair/decorated_key_with_hash.hh"
#include "readers/evictable.hh"
#include "dht/sharder.hh"
#include "reader_permit.hh"
#include "utils/phased_barrier.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include <fmt/core.h>

class repair_reader {
public:
    enum class read_strategy {
        local,
        multishard_split,
        multishard_filter
    };

private:
    schema_ptr _schema;
    reader_permit _permit;
    dht::partition_range _range;
    // Used to find the range that repair master will work on
    dht::selective_token_range_sharder _sharder;
    // Seed for the repair row hashing
    uint64_t _seed;
    // Pin the table while the reader is alive.
    // Only needed for local readers, the multishard reader takes care
    // of pinning tables on used shards.
    std::optional<utils::phased_barrier::operation> _local_read_op;
    std::optional<evictable_reader_handle_v2> _reader_handle;
    // Fragment stream of either local or multishard reader for the range
    mutation_fragment_v1_stream _reader;
    // Current partition read from disk
    lw_shared_ptr<const decorated_key_with_hash> _current_dk;
    uint64_t _reads_issued = 0;
    uint64_t _reads_finished = 0;

    mutation_reader make_reader(
        seastar::sharded<replica::database>& db,
        replica::column_family& cf,
        read_strategy strategy,
        const dht::sharder& remote_sharder,
        unsigned remote_shard,
        gc_clock::time_point compaction_time);

public:
    repair_reader(
        seastar::sharded<replica::database>& db,
        replica::column_family& cf,
        schema_ptr s,
        reader_permit permit,
        dht::token_range range,
        const dht::static_sharder& remote_sharder,
        unsigned remote_shard,
        uint64_t seed,
        read_strategy strategy,
        gc_clock::time_point compaction_time);

    future<mutation_fragment_opt>
    read_mutation_fragment();

    future<> on_end_of_stream() noexcept;

    future<> close() noexcept;

    lw_shared_ptr<const decorated_key_with_hash>& get_current_dk() {
        return _current_dk;
    }

    void set_current_dk(const dht::decorated_key& key);

    void clear_current_dk();

    void check_current_dk();

    void pause();
};

template <> struct fmt::formatter<repair_reader::read_strategy>  : fmt::formatter<string_view> {
    auto format(repair_reader::read_strategy s, fmt::format_context& ctx) const {
        using enum repair_reader::read_strategy;
        std::string_view name = "unknown";
        switch (s) {
            case local:
                name = "local";
                break;
            case multishard_split:
                name = "multishard_split";
                break;
            case multishard_filter:
                name = "multishard_filter";
                break;
        };
        return formatter<string_view>::format(name, ctx);
    }
};
