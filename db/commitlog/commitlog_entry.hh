/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "utils/assert.hh"
#include <optional>
#include <string_view>
#include "commitlog_types.hh"
#include "mutation/frozen_mutation.hh"
#include "schema/schema_fwd.hh"
#include "schema/schema.hh"
#include "raft/raft.hh"
#include "replay_position.hh"

namespace detail {

    using buffer_type = fragmented_temporary_buffer;
    using base_iterator = typename std::vector<temporary_buffer<char>>::const_iterator;

    static constexpr auto sector_overhead_size = sizeof(uint32_t) + sizeof(db::segment_id_type);

    // iterator adaptor to enable splitting normal
    // frag-buffer temporary buffer objects into 
    // sub-disk-page sized chunks.
    class sector_split_iterator {
        base_iterator _iter, _end;
        char* _ptr;
        size_t _size;
        size_t _sector_size;
    public:
        sector_split_iterator(const sector_split_iterator&) noexcept;
        sector_split_iterator(base_iterator i, base_iterator e, size_t sector_size);
        sector_split_iterator(base_iterator i, base_iterator e, size_t sector_size, size_t overhead);
        sector_split_iterator();

        char* get_write() const {
            return _ptr;
        }
        size_t size() const {
            return _size;
        }
        char* begin() {
            return _ptr;
        }
        char* end() {
            return _ptr + _size;
        }
        const char* begin() const {
            return _ptr;
        }
        const char* end() const {
            return _ptr + _size;
        }

        bool operator==(const sector_split_iterator& rhs) const {
            return _iter == rhs._iter && _ptr == rhs._ptr;
        }

        auto& operator*() const {
            return *this;
        }
        auto* operator->() const {
            return this;
        }

        sector_split_iterator& operator++();
        sector_split_iterator operator++(int);
    };

    static constexpr std::string_view variant_format_tag = "variant";

    enum commitlog_entry_serialization_format : uint8_t { mutation, variant };
} // namespace detail


// A frozen mutation together with its optional column mapping, as stored
// in the commitlog.  This is the original (and still default) payload type
// for commitlog entries — every normal table write goes through here.
struct mutation_entry {
    std::optional<column_mapping> _mapping;
    frozen_mutation _mutation;
public:
    mutation_entry(std::optional<column_mapping> mapping, frozen_mutation&& mutation)
        : _mapping(std::move(mapping)), _mutation(std::move(mutation)) { }
    const std::optional<column_mapping>& mapping() const { return _mapping; }
    const frozen_mutation& mutation() const & { return _mutation; }
    frozen_mutation&& mutation() && { return std::move(_mutation); }
};

// A raft log position: an entry's index and the term it was appended in.
// Always the pair of one real entry — the commit_idx entries and the covering
// raft_groups mutations both persist such a pair, and raft requires the term
// at a given index to be exact.
struct raft_term_and_index {
    raft::index_t idx{0};
    raft::term_t term{0};
};

// One raft batch as it is stored in the commitlog: the group id once, the
// entries the batch appended (commands, configuration changes, dummies), and
// how far the group had committed when the batch was written.
//
// The whole batch is a single commitlog entry, which is what keeps the group
// id and the entry envelope from being repeated per raft entry. The batch also
// carries its own retention: one claim comes back from the write, and
// raft_commitlog takes one claim per entry from it
// (commitlog::acquire_cf_count) so each entry's lifetime can be tracked
// separately even though they share a position.
//
// (commit_idx, commit_idx_term) is the crash-replay floor: startup restores
// the highest surviving value per group into system.raft_groups, so replay
// applies the entries below it to memtables instead of re-adding them to the
// raft log. The term is stored next to the index because the entry at that
// index is normally in an earlier batch, whose segment may already be gone —
// so the restore cannot look the term up. Zero means this group had not
// observed a commit index yet; the floor only ever advances, so an
// unrecoverable batch costs only floor tightness.
struct raft_commitlog_entry {
    raft::group_id group_id;
    std::vector<raft::log_entry_ptr> entries;
    raft::index_t commit_idx{0};
    raft::term_t commit_idx_term{0};
};

// The on-disk envelope for variant-format commitlog segments. Each entry
// contains exactly one of the variant alternatives: a mutation_entry (normal
// table write) or a raft_commitlog_entry (one batch of raft entries for a
// strongly-consistent table).
//
// NOTE: the variant alternative index is the on-disk discriminator, so new
// alternatives must only ever be appended at the end. Reordering or inserting
// in the middle would change the discriminator of existing alternatives and
// corrupt the reading of previously written segments.
using commitlog_entry_variant = std::variant<raft_commitlog_entry, mutation_entry>;
struct commitlog_entry {
    commitlog_entry_variant item;
};

class commitlog_mutation_entry_writer {
public:
    using force_sync = db::commitlog_force_sync;
private:
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size = std::numeric_limits<size_t>::max();
    force_sync _sync;
    detail::commitlog_entry_serialization_format _entry_format = detail::commitlog_entry_serialization_format::mutation;
private:
    template<typename Output>
    void serialize(Output&) const;
    void compute_size();
public:
    commitlog_mutation_entry_writer(schema_ptr s, const frozen_mutation& fm, force_sync sync)
        : _schema(std::move(s)), _mutation(fm), _sync(sync)
    {}

    void setup_for_segment(std::string_view segment_tag, bool encode_schema) {
        const auto new_format = segment_tag == detail::variant_format_tag
            ? detail::commitlog_entry_serialization_format::variant
            : detail::commitlog_entry_serialization_format::mutation;
        bool size_changed = std::exchange(_entry_format, new_format) != new_format;
        size_changed = std::exchange(_with_schema, encode_schema) != encode_schema || size_changed;
        if (size_changed || _size == std::numeric_limits<size_t>::max()) {
            compute_size();
        }
    }
    bool with_schema() const {
        return _with_schema;
    }
    bool use_variant_commitlog_entry_format() const {
        return _entry_format == detail::commitlog_entry_serialization_format::variant;
    }
    schema_ptr schema() const {
        return _schema;
    }

    size_t size() const {
        SCYLLA_ASSERT(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    size_t mutation_size() const {
        return _mutation.representation().size();
    }
    force_sync sync() const {
        return _sync;
    }

    using ostream = typename seastar::memory_output_stream<detail::sector_split_iterator>;

    void write(ostream& out) const;
};

// Mutation entry reader for hints commitlog (reads raw mutation_entry format).
class commitlog_mutation_entry_reader {
    mutation_entry _me;
public:
    commitlog_mutation_entry_reader(const fragmented_temporary_buffer& buffer);

    const std::optional<column_mapping>& get_column_mapping() const { return _me.mapping(); }
    const frozen_mutation& mutation() const & { return _me.mutation(); }
    frozen_mutation&& mutation() && { return std::move(_me).mutation(); }
};

// Writer for one raft batch in the database commit log, using the
// commitlog_entry format. Produced by raft_commitlog::write_batches() and
// accounted to the raft group's target table — the batch's cf.
class commitlog_raft_log_entry_writer {
protected:
    raft_commitlog_entry _item;
    std::size_t _size = std::numeric_limits<std::size_t>::max();

    template<typename Output>
    void serialize(Output& out) const;

    void compute_size();

public:
    explicit commitlog_raft_log_entry_writer(raft_commitlog_entry item)
        : _item(std::move(item)) { compute_size(); }

    size_t size() const {
        SCYLLA_ASSERT(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    using ostream = typename seastar::memory_output_stream<detail::sector_split_iterator>;
    void write(ostream& out) const;

    raft::group_id group_id() const {
        return _item.group_id;
    }
    const raft_commitlog_entry& item() const {
        return _item;
    }
};

class commitlog_entry_reader {
    commitlog_entry _entry;

public:
    explicit commitlog_entry_reader(const fragmented_temporary_buffer& buffer,
            detail::commitlog_entry_serialization_format format = detail::commitlog_entry_serialization_format::mutation);

    const commitlog_entry& entry() const& {
        return _entry;
    }
    commitlog_entry&& entry() && {
        return std::move(_entry);
    }
};
