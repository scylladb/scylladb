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

// A Raft log entry (command, configuration change, or dummy) together with
// its group ID.  Stored in the database commitlog when the strongly-consistent
// tables experimental feature is enabled and the segment uses the variant
// serialization format.
struct raft_commitlog_entry {
    raft::group_id group_id;
    raft::log_entry_ptr entry;
};

// Commitlog entry that carries the current commit index of a strongly-consistent
// raft group. Used to persist the commit index (see raft_commitlog::store_log_entries
// and raft_groups_storage::store_log_entries).
struct raft_commit_idx_entry {
    raft::group_id group_id;
    raft::index_t commit_idx{0};
};

// The on-disk envelope for variant-format commitlog segments. Each entry
// contains exactly one of the variant alternatives: a mutation_entry (normal
// table write), a raft_commitlog_entry (Raft log entry for a strongly-consistent
// table), or a raft_commit_idx_entry (per-batch commit_idx entry).
//
// NOTE: the variant alternative index is the on-disk discriminator, so new
// alternatives must only ever be appended at the end. Reordering or inserting
// in the middle would change the discriminator of existing alternatives and
// corrupt the reading of previously written segments.
using commitlog_entry_variant = std::variant<raft_commitlog_entry, mutation_entry, raft_commit_idx_entry>;
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

// Writer for Raft-related entries in the database commit log using the
// commitlog_entry format. Handles both the raft log entry variant
// (raft_commitlog_entry) and the per-batch commit-index entry
// (raft_commit_idx_entry); both are produced by raft_commitlog::
// store_log_entries as part of a single batched commit_log.add_raft_entries().
//
// Each writer carries its own cf_id_type so a single batch can contain
// entries destined for different column families — used to associate the
// commit_idx entry with system.raft_groups rather than the raft log's target table.
class commitlog_raft_log_entry_writer {
public:
    using item_variant = std::variant<raft_commitlog_entry, raft_commit_idx_entry>;
protected:
    db::cf_id_type _cf_id;
    item_variant _item;
    std::size_t _size = std::numeric_limits<std::size_t>::max();

    template<typename Output>
    void serialize(Output& out) const;
    void compute_size();

public:
    explicit commitlog_raft_log_entry_writer(db::cf_id_type cf_id, raft_commitlog_entry item)
        : _cf_id(cf_id), _item(std::move(item)) { compute_size(); }
    explicit commitlog_raft_log_entry_writer(db::cf_id_type cf_id, raft_commit_idx_entry item)
        : _cf_id(cf_id), _item(std::move(item)) { compute_size(); }

    size_t size() const {
        SCYLLA_ASSERT(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    using ostream = typename seastar::memory_output_stream<detail::sector_split_iterator>;
    void write(ostream& out) const;

    const db::cf_id_type& cf_id() const {
        return _cf_id;
    }
    raft::group_id group_id() const {
        return std::visit([] (const auto& item) { return item.group_id; }, _item);
    }
    // Returns the underlying raft_commitlog_entry when this writer holds one.
    // Guarded by holds_raft_log_entry(); prefer visiting item() instead.
    const raft_commitlog_entry& get_raft_log_entry() const {
        return std::get<raft_commitlog_entry>(_item);
    }
    bool holds_raft_log_entry() const {
        return std::holds_alternative<raft_commitlog_entry>(_item);
    }
    const item_variant& item() const {
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
