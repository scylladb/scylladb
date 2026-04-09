/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"
#include <optional>

#include "commitlog_types.hh"
#include "mutation/frozen_mutation.hh"
#include "raft/raft.hh"
#include "schema/schema_fwd.hh"
#include "replay_position.hh"
#include "utils/UUID.hh"

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
}

class commitlog_entry {
    std::optional<column_mapping> _mapping;
    frozen_mutation _mutation;
public:
    commitlog_entry(std::optional<column_mapping> mapping, frozen_mutation&& mutation)
        : _mapping(std::move(mapping)), _mutation(std::move(mutation)) { }
    const std::optional<column_mapping>& mapping() const { return _mapping; }
    const frozen_mutation& mutation() const & { return _mutation; }
    frozen_mutation&& mutation() && { return std::move(_mutation); }
};

class raft_commit_log_entry {
    utils::UUID _group_id;
    std::optional<raft::log_entry> _entry;
    std::optional<raft::index_t> _truncate_idx;
    std::optional<raft::index_t> _truncate_prefix_idx;
public:
    raft_commit_log_entry(utils::UUID group_id, std::optional<raft::log_entry> entry, std::optional<raft::index_t> truncate_idx, std::optional<raft::index_t> truncate_prefix_idx)
        : _group_id(group_id)
        , _entry(std::move(entry))
        , _truncate_idx(std::move(truncate_idx))
        , _truncate_prefix_idx(std::move(truncate_prefix_idx)) {
    }

    raft_commit_log_entry(utils::UUID group_id, raft::log_entry entry)
        : _group_id(group_id)
        , _entry(std::move(entry)) {
    }

    raft_commit_log_entry(utils::UUID group_id, std::optional<raft::index_t> truncate_idx, std::optional<raft::index_t> truncate_prefix_idx)
        : _group_id(group_id)
        , _truncate_idx(std::move(truncate_idx))
        , _truncate_prefix_idx(std::move(truncate_prefix_idx)) {
    }

    const utils::UUID& group_id() const { return _group_id; }
    const std::optional<raft::log_entry>& entry() const { return _entry; }
    const std::optional<raft::index_t>& truncate_idx() const { return _truncate_idx; }
    const std::optional<raft::index_t>& truncate_prefix_idx() const { return _truncate_prefix_idx; }
};

class commitlog_entry_writer {
public:
    using force_sync = db::commitlog_force_sync;
private:
    schema_ptr _schema;
    const frozen_mutation& _mutation;
    bool _with_schema = true;
    size_t _size = std::numeric_limits<size_t>::max();
    force_sync _sync;
private:
    template<typename Output>
    void serialize(Output&) const;
    void compute_size();
public:
    commitlog_entry_writer(schema_ptr s, const frozen_mutation& fm, force_sync sync)
        : _schema(std::move(s)), _mutation(fm), _sync(sync)
    {}

    void set_with_schema(bool value) {
        if (std::exchange(_with_schema, value) != value || _size == std::numeric_limits<size_t>::max()) {
            compute_size();
        }
    }
    bool with_schema() const {
        return _with_schema;
    }
    schema_ptr schema() const {
        return _schema;
    }

    size_t size() const {
        SCYLLA_ASSERT(_size != std::numeric_limits<size_t>::max());
        return _size;
    }

    size_t size(uint32_t segment_version) const;

    size_t mutation_size() const {
        return _mutation.representation().size();
    }
    force_sync sync() const {
        return _sync;
    }

    using ostream = typename seastar::memory_output_stream<detail::sector_split_iterator>;

    void write(ostream& out) const;
    void write(ostream& out, uint32_t segment_version) const;
    commitlog_entry make_entry() const;
};

class commitlog_entry_reader {
    std::optional<commitlog_entry> _ce;
    std::optional<raft_commit_log_entry> _raft_ce;
public:
    commitlog_entry_reader(const fragmented_temporary_buffer& buffer);
    commitlog_entry_reader(const fragmented_temporary_buffer& buffer, uint32_t segment_version);

    const std::optional<column_mapping>& get_column_mapping() const;
    const frozen_mutation& mutation() const &;
    frozen_mutation&& mutation() &&;
    const raft_commit_log_entry* get_raft_entry() const {
        return _raft_ce ? &*_raft_ce : nullptr;
    }
};
