/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// This file contains a test of BTI index writers and readers.
//
// It generates a random dataset (or sstable index entries),
// writes it to a BTI index file, and then runs a sequence of
// BTI index reader operations on it, checking that the results
// are consistent with a "reference" index on the same dataset.

#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_case.hh>
#include "seastar/core/fstream.hh"
#include "seastar/core/seastar.hh"
#include "seastar/util/closeable.hh"
#include "seastar/util/defer.hh"
#include "sstables/mx/types.hh"
#include "sstables/trie/bti_index.hh"
#include "schema/schema_builder.hh"
#include "test/lib/log.hh"
#include "test/lib/nondeterministic_choice_stack.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "utils/cached_file.hh"
#include <fmt/std.h>

struct clustering_index_entry {
    sstables::clustering_info first_ck;
    sstables::clustering_info last_ck;
    uint64_t data_file_offset;
    sstables::deletion_time range_tombstone_before_first_ck;
};

struct partition_index_entry {
    dht::decorated_key dk;
    uint64_t data_file_offset;
    sstables::deletion_time partition_tombstone;
};

struct eof_index_entry {
    uint64_t data_file_offset;
};

struct partition_end_entry {
    uint64_t data_file_offset;
};

using index_entry = std::variant<partition_index_entry, clustering_index_entry, partition_end_entry, eof_index_entry>;

// Represents a sequence of entries written to the sstable index file,
// and sets of possible arguments for index reader operations on this index.
struct index_entry_dataset {
    const schema& s;
    // The set of ring positions participating in the test.
    // Will be used as the set of potential arguments for index reader operations
    // that take a ring position, and to construct partition ranges for `advance_to`.
    //
    // Assumed to contain all decorated keys used in `entries`.
    std::vector<dht::ring_position> ring_position_universe;
    // The set of clustering positions participating in the test.
    // Will be used as the set of potential arguments for index reader operations
    // that take a position in partition.
    std::vector<position_in_partition> position_in_partition_universe;
    // The sequence of entries written to the sstable index file.
    // in order.
    // It's assumed that the sequence contains at least one partition index entry,
    // and that it's of the form:
    // (partition_index_entry, clustering_index_entry*, partition_end_entry)+, eof_index_entry
    std::vector<index_entry> entries;
};

struct random_dataset_config {
    // Number of possible distinct values for each "component" of the decorated partition key.
    // (In a loose meaning -- in particular, we consider some individual bytes of the token
    // "components").
    // Needs to be big enough to have enough "power" to create enough trie branches to make for
    // intersesting trie shapes, but small enough to keep the values similar enough to
    // create interesting relationships between them.
    // The number of generated partition keys grows polynomially with this value,
    // but only a certain number of them will be picked to particpate in the test.
    int partition_key_component_values = 3;
    // The number of ring positions participating in the test.
    // The number of partition keys inserted into the index
    // will be some subset of that.
    // The complexity of the test grows exponentially with this value.
    int partition_universe_size = 3;
    // Like partition_key_component_values, but for clustering positions.
    int clustering_key_component_values = 3;
    // Like partition_universe_size, but for clustering positions.
    int clustering_universe_size = 6;
    // The max number of clustering blocks inserted into the index
    // for each partition.
    // Should be at most as big as clustering_universe_size / 2,
    // otherwise there isn't enough clustering positions to fill the blocks.
    int max_clustering_blocks = clustering_universe_size / 2;
};

// A recursive helper for generate_pips().
void generate_pips_impl(
    const schema& s,
    std::vector<data_value>& prefix,
    std::vector<position_in_partition>& result,
    std::span<data_value> possible_values
) {
    auto ckp = clustering_key_prefix::from_deeply_exploded(s, prefix);
    result.push_back(position_in_partition(partition_region::clustered, bound_weight(-1), ckp));
    if (prefix.size() == s.clustering_key_size()) {
        result.push_back(position_in_partition(partition_region::clustered, bound_weight(0), ckp));
    } else {
        for (const auto& value : possible_values) {
            prefix.push_back(value);
            generate_pips_impl(s, prefix, result, possible_values);
            prefix.pop_back();
        }
    }
    result.push_back(position_in_partition(partition_region::clustered, bound_weight(1), ckp));
}

// Generate all partition_in_position values,
// which use the given possible_values as values for clustering key columns. 
// Assumes a clustering key of type `(short, short)`.
std::vector<position_in_partition> generate_pips(const schema& the_schema, std::span<data_value> possible_values) {
    std::vector<data_value> prefix;
    std::vector<position_in_partition> result;
    generate_pips_impl(the_schema, prefix, result, possible_values);
    return result;
}
static sstables::deletion_time make_random_tombstone() {
    if (tests::random::get_bool()) {
        return sstables::deletion_time::make_live();
    }
    return sstables::deletion_time{
        tests::random::get_int<int32_t>(std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()),
        tests::random::get_int<int64_t>(std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()),
    };
}

static sstables::bound_kind_m pip_bound_weight_to_m_bound_weight(const bound_weight bw) {
    switch (bw) {
    case bound_weight::before_all_prefixed:
        switch (tests::random::get_int<int>(0, 2)) {
            case 0:
                return sstables::bound_kind_m::incl_start;
            case 1:
                return sstables::bound_kind_m::excl_end;
            case 2:
                return sstables::bound_kind_m::excl_end_incl_start;
        }
        break;
    case bound_weight::after_all_prefixed:
        switch (tests::random::get_int<int>(0, 2)) {
            case 0:
                return sstables::bound_kind_m::incl_end;
            case 1:
                return sstables::bound_kind_m::excl_start;
            case 2:
                return sstables::bound_kind_m::incl_end_excl_start;
        }
        break;
    case bound_weight::equal:
        return sstables::bound_kind_m::clustering;
    default:
        abort();
    }
    abort();
}

// Generate some interesting dataset of sstable index entries and related key positions.
// Assumes a primary key of type `(short, (short, short))`.
index_entry_dataset generate_random_dataset(const schema& the_schema, const random_dataset_config& cfg) {
    const auto& s = the_schema;
    std::vector<index_entry> result;

    // Generate a few partition keys.
    std::vector<partition_key> pks;
    for (int i = 0; i < cfg.partition_key_component_values; ++i) {
        pks.push_back(partition_key::from_deeply_exploded(s, std::vector<data_value>{data_value(int16_t(i))}));
    }

    // Generate the set of decorated keys participating in the test.
    // Some subset of them will be inserted into the index,
    // and ring positions related to them will be used as arguments for index reader operations.
    std::vector<dht::decorated_key> dk_universe;
    {
        // Generate a big set of possible decorated keys,
        // by taking a cartesian product of several values for each "component".
        // (We do that instead of generating completely random keys
        // because having some common prefixes between keys should explore
        // more logic, because the length of common prefixes is important for tries). 
        std::vector<dht::decorated_key> dk_universe_candidates;
        for (int i = 0; i < cfg.partition_key_component_values; ++i) {
            for (int j = 0; j < cfg.partition_key_component_values; ++j) {
                for (int k = 0; k < cfg.partition_key_component_values; ++k) {
                #if 1
                    // Less readable during debugging, slightly better coverage,
                    // because it tests the hash byte logic.
                    auto hash_byte = tests::random::get_int<uint8_t>(0, cfg.partition_key_component_values - 1);
                    auto token = dht::token::from_int64(i << 16 | j << 8 | hash_byte);
                #else
                    auto token = dht::token::from_int64(i * cfg.partition_key_component_values + j);
                #endif
                    auto dk = dht::decorated_key(token, pks[k]);
                    dk_universe_candidates.push_back(dk);
                }
            }
        }
        // Select the configured number of decorated keys, at random, from the big set. 
        auto indexes = std::vector(std::from_range, std::views::iota(size_t(0), std::size(dk_universe_candidates)));
        auto chosen_indexes = std::vector<size_t>(cfg.partition_universe_size);
        std::sample(indexes.begin(), indexes.end(), chosen_indexes.begin(), chosen_indexes.size(), tests::random::gen());
        for (const auto& i : chosen_indexes) {
            dk_universe.push_back(dk_universe_candidates[i]);
        }
    }

    // Generate some interesting partition_in_position values.
    // A subset of them will be inserted into the index,
    // and they will be used as arguments for index reader operations.
    std::vector<position_in_partition> pip_universe;
    {
        std::vector<data_value> clustering_key_value_set;
        for (int i = 0; i < cfg.clustering_key_component_values; ++i) {
            clustering_key_value_set.push_back(data_value(int16_t(i)));
        }
        auto pip_universe_candidates = generate_pips(s, clustering_key_value_set);
        auto pip_indexes = std::vector(std::from_range, std::views::iota(size_t(0), std::size(pip_universe_candidates)));
        auto chosen_pips = std::vector<size_t>(cfg.clustering_universe_size);
        std::sample(pip_indexes.begin(), pip_indexes.end(), chosen_pips.begin(), chosen_pips.size(), tests::random::gen());
        for (const auto& i : chosen_pips) {
            pip_universe.push_back(pip_universe_candidates[i]);
        }
    }

    // The exact value of this is unimportant,
    // but it must strictly grow with each entry.
    int64_t data_file_offset = 0;

    const int inserted_partitions = tests::random::get_int(1, cfg.partition_universe_size);
    std::vector<dht::decorated_key> inserted_dks;
    std::sample(dk_universe.begin(), dk_universe.end(), std::back_inserter(inserted_dks), inserted_partitions, tests::random::gen());
    for (int p = 0; p < inserted_partitions; ++p) {
        const auto dk = inserted_dks[p];

        const auto tombstone = make_random_tombstone();
        result.push_back(partition_index_entry{
            .dk = dk,
            .data_file_offset = data_file_offset++,
            .partition_tombstone = tombstone,
        });

        const int inserted_clustering_blocks = tests::random::get_int(0, cfg.max_clustering_blocks);
        std::vector<position_in_partition> inserted_pips;
        std::sample(pip_universe.begin(), pip_universe.end(), std::back_inserter(inserted_pips), inserted_clustering_blocks * 2, tests::random::gen());
        for (int c = 0; c < inserted_clustering_blocks; ++c) {
            auto tombstone = make_random_tombstone();
            auto first_pip = inserted_pips[c * 2];
            auto first_pip_weight = pip_bound_weight_to_m_bound_weight(first_pip.get_bound_weight());
            auto last_pip = inserted_pips[c * 2 + 1];
            auto last_pip_weight = pip_bound_weight_to_m_bound_weight(last_pip.get_bound_weight());
            result.push_back(clustering_index_entry{
                .first_ck = sstables::clustering_info(first_pip.key(), first_pip_weight),
                .last_ck = sstables::clustering_info(last_pip.key(), last_pip_weight),
                .data_file_offset = data_file_offset++,
                .range_tombstone_before_first_ck = tombstone,
            });
        }
        result.push_back(partition_end_entry{
            .data_file_offset = data_file_offset++,
        });
    }
    result.push_back(eof_index_entry{
        .data_file_offset = data_file_offset,
    });
    std::vector<dht::ring_position> rp_universe;
    // We only use the decorated keys as ring positions,
    // we don't bother with token positions or min/max positions.
    // They shouldnt be distinguishable from a non-inserted key
    // at the same position relative to inserted keys.
    for (const auto& dk : dk_universe) {
        rp_universe.push_back(dk);
    }
    return index_entry_dataset {
        .s = s,
        .ring_position_universe = std::move(rp_universe),
        .position_in_partition_universe = std::move(pip_universe),
        .entries = std::move(result),
    };
}

// A reference index that can be used to check the results of actual index reader operations.
// Tries to implement the contract of abstact index_reader in a relatively simple way.
// 
struct reference_index {
    enum entry_idx : uint64_t {};
    enum rp_idx : uint64_t {};

    // These are directly from the generated index_entry_dataset.
    const schema& _s;
    std::vector<dht::ring_position> _ring_position_universe;
    std::vector<position_in_partition> _position_in_partition_universe;
    std::vector<index_entry> _entries;

    // These are some helpers for navigating the dataset above.
    
    // The set of decorated keys present in _entries.
    std::vector<dht::decorated_key> _present_dks;
    // `_present_dk_indices_in_rp_universe[i]`
    // is the index of `_present_dks[i]` within `_ring_position_universe`.
    std::vector<rp_idx> _present_dk_indices_in_rp_universe;
    // `_present_dk_indices[i]`
    // is the index of `_present_dks[i]` within `_entries`.
    // `_present_dk_indices[_present_dks.size()]` equals `_entries.size() - 1`.
    std::vector<entry_idx> _present_dk_indices;
    // `_data_file_offsets[i]` is the `_entries[i].data_file_offset`.
    // (Extracted for convenience, because `_entries[i]` is a variant).
    std::vector<uint64_t> _data_file_offsets;

    // The mutable state of this index reader.
    // An index reader is expected to behave like a (lower bound, upper bound) pair,
    // this is a "model" of that.
    entry_idx _lower = entry_idx(0);
    std::optional<entry_idx> _upper;
    bool _initialized = false;

    reference_index(const index_entry_dataset& raw)
        : _s(raw.s)
        , _ring_position_universe(raw.ring_position_universe)
        , _position_in_partition_universe(raw.position_in_partition_universe)
        , _entries(raw.entries)
    {
        for (const auto& e : _entries) {
            std::visit([this](const auto& entry) {
                _data_file_offsets.push_back(entry.data_file_offset);
            }, e);
        }
        {
            uint64_t idx = 0;
            for (const auto& e : _entries) {
                if (auto p = std::get_if<partition_index_entry>(&e)) {
                    _present_dks.push_back(p->dk);
                    _present_dk_indices.push_back(entry_idx(idx));
                } else if (std::get_if<eof_index_entry>(&e)) {
                    _present_dk_indices.push_back(entry_idx(idx));
                }
                ++idx;
            }
        }
        {
            auto cmp = dht::ring_position_comparator(_s);
            auto it = _present_dks.begin();
            uint64_t idx = 0;
            while (it != _present_dks.end() && idx < _ring_position_universe.size()) {
                if (cmp(_ring_position_universe[idx], *it) == std::strong_ordering::equal) {
                    _present_dk_indices_in_rp_universe.push_back(rp_idx(idx));
                    ++it;
                }
                ++idx;
            }
            SCYLLA_ASSERT(_present_dks.size() == _present_dk_indices_in_rp_universe.size());
        }
    }

    entry_idx entry_idx_from_data_position(uint64_t data_position) const {
        auto it = std::find(_data_file_offsets.begin(), _data_file_offsets.end(), data_position);
        if (it != _data_file_offsets.end()) {
            return static_cast<entry_idx>(std::distance(_data_file_offsets.begin(), it));
        }
        abort();
    }

    entry_idx get_partition_of_entry(entry_idx idx) const {
        for (int64_t i = std::to_underlying(idx); i >= 0; --i) {
            if (std::holds_alternative<partition_index_entry>(_entries[i])
                || std::holds_alternative<eof_index_entry>(_entries[i])
            ) {
                return entry_idx(i);
            }
        }
        abort();
    }

    entry_idx get_current_partition() const {
        return get_partition_of_entry(_lower);
    }

    void recalibrate(sstables::data_file_positions_range r) {
        _lower = entry_idx_from_data_position(r.start);
        if (r.end) {
            _upper = entry_idx_from_data_position(*r.end);
        } else {
            _upper.reset();
        }
    }

    std::span<const dht::ring_position> valid_targets_for_advance_lower_and_check_if_present() const {
        return _ring_position_universe;
    }
    bool advance_lower_and_check_if_present(dht::ring_position_view key) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        auto tricmp = dht::ring_position_comparator(_s);
        auto it = std::ranges::lower_bound(_present_dks, key, cmp);
        _lower = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
        if (it == _present_dks.end() || tricmp(*it, key) != std::strong_ordering::equal) {
            return false;
        }
        return true;
    }

    std::span<const dht::decorated_key> valid_targets_for_advance_past_definitely_present_partition() const {
        return _present_dks;
    }
    void advance_past_definitely_present_partition(const dht::decorated_key& dk) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        auto it = std::ranges::lower_bound(_present_dks, dk, cmp);
        _lower = entry_idx(_present_dk_indices[it - _present_dks.begin() + 1]);
    }
    void advance_to_definitely_present_partition(const dht::decorated_key& dk) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        auto it = std::ranges::lower_bound(_present_dks, dk, cmp);
        _lower = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
    }
    std::span<const dht::ring_position> valid_lb_targets_for_advance_to() const {
        auto cmp = dht::ring_position_less_comparator(_s);
        auto boundary_idx = _upper ? *_upper : _lower;
        auto it = std::ranges::lower_bound(_ring_position_universe, ring_position_of_entry(boundary_idx), cmp);
        return std::span<const dht::ring_position>(it, _ring_position_universe.end());
    }
    std::span<const dht::ring_position> valid_ub_targets_for_advance_to(const dht::ring_position& lb) const {
        auto cmp = dht::ring_position_less_comparator(_s);
        auto it = std::ranges::lower_bound(_ring_position_universe, lb, cmp);
        return std::span<const dht::ring_position>(it, _ring_position_universe.end());
    }
    void advance_to(const dht::partition_range& range) {
        _initialized = true;
        auto cmp = dht::ring_position_less_comparator(_s);
        {
            auto rpv = dht::ring_position_view::for_range_start(range);
            auto it = std::ranges::lower_bound(_present_dks, rpv, cmp);
            _lower = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
        }
        {
            auto rpv = dht::ring_position_view::for_range_end(range);
            auto it = std::ranges::lower_bound(_present_dks, rpv, cmp);
            _upper = entry_idx(_present_dk_indices[it - _present_dks.begin()]);
        }
    }
    void advance_to_next_partition() {
        _initialized = true;
        auto idx = std::to_underlying(_lower);
        if (std::holds_alternative<partition_index_entry>(_entries[idx])) {
            ++idx;
        }
        while (std::holds_alternative<clustering_index_entry>(_entries[idx])
            || std::holds_alternative<partition_end_entry>(_entries[idx])
        ) {
            ++idx;
        }
        _lower = entry_idx(idx);
    }
    void advance_reverse_to_next_partition() {
        _initialized = true;
        auto idx = std::to_underlying(_lower);
        if (std::holds_alternative<partition_index_entry>(_entries[idx])) {
            ++idx;
        }
        while (std::holds_alternative<clustering_index_entry>(_entries[idx])
            || std::holds_alternative<partition_end_entry>(_entries[idx])
        ) {
            ++idx;
        }
        _upper = entry_idx(idx);
    }
    entry_idx advance_bound_before(entry_idx eidx, position_in_partition_view pos) {
        auto less = position_in_partition::less_compare(_s);
        auto idx = std::to_underlying(get_current_partition());
        if (std::holds_alternative<eof_index_entry>(_entries[idx])) {
            return entry_idx(idx);
        }
        if (std::holds_alternative<clustering_index_entry>(_entries[idx + 1])) {
            ++idx;
            while (auto e = std::get_if<clustering_index_entry>(&_entries[idx])) {
                if (less(pip_of_clustering_info(e->last_ck), pos)) {
                    ++idx;
                } else {
                    break;
                }
            }
        }
        return entry_idx(idx);
    }
    static int weight_of_bound_kind_m(sstables::bound_kind_m b){
        using sstables::bound_kind_m;
        switch (b) {
            case bound_kind_m::incl_start:
            case bound_kind_m::excl_end_incl_start:
            case bound_kind_m::excl_end:
                return -1;
            case bound_kind_m::static_clustering:
            case bound_kind_m::clustering:
                return 0;
            case bound_kind_m::incl_end_excl_start:
            case bound_kind_m::incl_end:
            case bound_kind_m::excl_start:
                return 1;
        }
        abort();
    }
    static position_in_partition pip_of_clustering_info(const sstables::clustering_info& e) {
        int weight = weight_of_bound_kind_m(e.kind);
        return position_in_partition(partition_region::clustered, bound_weight(weight), e.clustering);
    }
    position_in_partition past_previous_pip() const {
        if (std::holds_alternative<eof_index_entry>(_entries[_lower])) {
            return position_in_partition::for_partition_end();
        }
        if (std::holds_alternative<partition_index_entry>(_entries[_lower])) {
            return position_in_partition::after_static_row_tag_t();
        }
        if (auto e = std::get_if<clustering_index_entry>(&_entries[_lower - 1])) {
            return pip_of_clustering_info(e->last_ck);
        } 
        if (std::holds_alternative<partition_end_entry>(_entries[_lower])) {
            return position_in_partition::for_partition_end();
        }
        return position_in_partition::after_static_row_tag_t();
    }
    std::span<const position_in_partition> valid_targets_for_advance_to_pip() const {
        auto cmp = position_in_partition::less_compare(_s);
        auto cp = past_previous_pip();
        testlog.debug("valid_targets_for_advance_to_pip: current_pip={}", cp);
        auto it = std::ranges::upper_bound(_position_in_partition_universe, cp, cmp);
        return std::span<const position_in_partition>(it, _position_in_partition_universe.end());
    }
    std::span<const position_in_partition> valid_targets_for_advance_reverse() const {
        return _position_in_partition_universe;
    }
    void advance_to(position_in_partition_view pos) {
        _initialized = true;
        _lower = advance_bound_before(_lower, pos);
    }
    void advance_upper_past(position_in_partition_view pos) {
        _initialized = true;
        _upper = advance_bound_before(_lower, pos);
    }
    void advance_reverse(position_in_partition_view pos) {
        _initialized = true;
        auto less = position_in_partition::less_compare(_s);
        auto idx = std::to_underlying(get_partition_of_entry(_lower));
        if (std::holds_alternative<partition_index_entry>(_entries[idx])) {
            ++idx;
        }
        while (auto e = std::get_if<clustering_index_entry>(&_entries[idx])) {
            if (less(pos, pip_of_clustering_info(e->first_ck))) {
                break;
            } else {
                ++idx;
            }
        }
        _upper = entry_idx(idx);
    }

    bool has_row_index() const {
        auto curpar = get_current_partition();
        if (std::holds_alternative<eof_index_entry>(_entries[curpar])) {
            return false;
        }
        if (std::holds_alternative<clustering_index_entry>(_entries[curpar + 1])) {
            return true;
        }
        return false;
    }
    std::optional<sstables::deletion_time> partition_tombstone() const {
        if (const auto& hdr = std::get_if<partition_index_entry>(&_entries[get_current_partition()])) {
            return hdr->partition_tombstone;
        }
        return std::nullopt;
    }
    std::optional<partition_key> get_partition_key() const {
        if (const auto& hdr = std::get_if<partition_index_entry>(&_entries[get_current_partition()])) {
            return hdr->dk.key();
        }
        return std::nullopt;
    }
    sstables::data_file_positions_range data_file_positions() const {
        auto lo = _data_file_offsets[_lower];
        std::optional<uint64_t> hi;
        if (_upper) {
            hi = _data_file_offsets[*_upper];
        }
        return {lo, hi};
    }
    std::optional<uint64_t> last_block_offset() {
        auto curpar = get_current_partition();
        if (std::holds_alternative<eof_index_entry>(_entries[curpar])) {
            return std::nullopt;
        }
        if (!std::holds_alternative<clustering_index_entry>(_entries[curpar + 1])) {
            return std::nullopt;
        }
        for (uint64_t idx = curpar + 1; idx < _entries.size(); ++idx) {
            if (std::holds_alternative<partition_end_entry>(_entries[idx + 1])) {
                return _data_file_offsets[idx];
            }
        }
        abort();
    }
    sstables::indexable_element element_kind_for_entry(entry_idx idx) const {
        if (std::holds_alternative<clustering_index_entry>(_entries[idx])
            || std::holds_alternative<partition_end_entry>(_entries[idx])) {
            return sstables::indexable_element::cell;
        }
        return sstables::indexable_element::partition;
    }
    sstables::indexable_element element_kind() const {
        return element_kind_for_entry(_lower);
    }
    sstables::indexable_element element_kind_for_position(std::optional<uint64_t> pos) const {
        if (!pos) {
            return sstables::indexable_element::partition;
        }
        auto entry = entry_idx_from_data_position(*pos);
        return element_kind_for_entry(entry);
    }
    std::optional<sstables::open_rt_marker> end_open_marker() const {
        if (auto e = std::get_if<clustering_index_entry>(&_entries[_lower])) {
            auto tomb = tombstone(e->range_tombstone_before_first_ck);
            if (!tomb) {
                return std::nullopt;
            }
            return sstables::open_rt_marker{
                .pos = {position_in_partition::after_static_row_tag_t()},
                .tomb = tomb,
            };
        }
        return std::nullopt;
    }
    std::optional<sstables::open_rt_marker> reverse_end_open_marker() const {
        if (!_upper) {
            return std::nullopt;
        }
        if (auto e = std::get_if<clustering_index_entry>(&_entries[*_upper])) {
            auto tomb = tombstone(e->range_tombstone_before_first_ck);
            if (!tomb) {
                return std::nullopt;
            }
            return sstables::open_rt_marker{
                .pos = {position_in_partition::after_static_row_tag_t()},
                .tomb = tomb,
            };
        }
        return std::nullopt;
    }
    bool eof() const {
        return _lower == _entries.size() - 1;
    }
    bool partition_data_ready() const {
        return _initialized;
    }
    void read_partition_data() {
        _initialized = true;
    }
    void reset() {
        _initialized = false;
        _lower = entry_idx(0);
        _upper.reset();
    }
    dht::ring_position_view ring_position_of_entry(entry_idx eidx) const {
        auto curpar = get_partition_of_entry(eidx);
        if (std::holds_alternative<eof_index_entry>(_entries[curpar])) {
            return dht::ring_position_view::max();
        }
        auto pe = std::get_if<partition_index_entry>(&_entries[curpar]);
        if (curpar == eidx) {
            return dht::ring_position_view(pe->dk);
        } else {
            return dht::ring_position_view::for_after_key(pe->dk);
        }
    }
};

// Test all possible legal sequences of `abstract_index_reader` method calls,
// up to a certain sequence length (max_ops),
// on the given index dataset.
//
// The general structore of the test is:
// 1. We create a real index reader and a reference index reader.
// 2. For max_ops iterations:
// 2.1 We nondeterministically choose an operation to perform and its argument.
// 2.2 We call the operation on both readers.
// 2.3 We check that the real reader's positions after the method are
//     consistent (possibly less accurate, but within the contract of the method)
//     with the reference reader's positions.
// 2.4 We adjust the reference reader to point to exactly the same positions as the real reader,
//     so that their states match for the next method calls.
// 2.5 We check that all the const getters return the same thing for both readers.
//
// And nondeterministic_choice_stack is used to explore all possible outcomes of the nondeterministic choices.
void test_index(const index_entry_dataset& dataset, std::function<std::unique_ptr<sstables::abstract_index_reader>(void)> reader_factory, const int max_ops) {
    auto ri = reference_index(dataset);

    uint64_t n_cases = 0;
    nondeterministic_choice_stack ndcs;
    do {
        ++n_cases;
        auto reader = reader_factory();
        ri.reset();
        auto check_integrity = [&] {
            testlog.debug("check_integrity: reader->data_file_positions()={},{}", reader->data_file_positions().start, reader->data_file_positions().end);
            auto positions = reader->data_file_positions();
            // Adjust the reference index to match the reader's positions exactly.
            // (Before this call, the positions may be different, because the real
            // reader is allowed some degree of inexactness/suboptimality after some method calls).
            ri.recalibrate(positions);
            if (ri.partition_data_ready()) {
                SCYLLA_ASSERT(reader->partition_data_ready());
            }
            SCYLLA_ASSERT(ri.data_file_positions().start == positions.start);
            SCYLLA_ASSERT(ri.data_file_positions().end == positions.end);
            SCYLLA_ASSERT(ri.element_kind() == reader->element_kind());
            SCYLLA_ASSERT(ri.eof() == reader->eof());
            auto get_tombstone = [] (const sstables::open_rt_marker& marker) {
                return marker.tomb;
            };
            SCYLLA_ASSERT(ri.end_open_marker().transform(get_tombstone) == reader->end_open_marker().transform(get_tombstone));
            SCYLLA_ASSERT(ri.reverse_end_open_marker().transform(get_tombstone) == reader->reverse_end_open_marker().transform(get_tombstone));
            if (reader->partition_data_ready()) {
                SCYLLA_ASSERT(ri.last_block_offset() == reader->last_block_offset().get());
                if (ri.has_row_index()) {
                    SCYLLA_ASSERT(reader->partition_tombstone());
                    SCYLLA_ASSERT(reader->get_partition_key());
                    SCYLLA_ASSERT(ri.partition_tombstone() == reader->partition_tombstone());
                    SCYLLA_ASSERT(ri.get_partition_key() == reader->get_partition_key());
                } else {
                    SCYLLA_ASSERT(!reader->partition_tombstone());
                    SCYLLA_ASSERT(!reader->get_partition_key());
                }
            }
        };
        testlog.debug("Initial check_integrity");
        check_integrity();
        for (int op = 0; op < max_ops; ++op) {
            testlog.debug("op={}, start={}, end={}",
                op, reader->data_file_positions().start, reader->data_file_positions().end);
            if (auto vt = ri.valid_targets_for_advance_lower_and_check_if_present(); !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                auto rp = vt[target];
                testlog.debug("advance_lower_and_check_if_present(rp={})", rp);
                
                auto upper_before = reader->data_file_positions().end;
                auto possible_match = reader->advance_lower_and_check_if_present(rp).get();
                auto reference_match = ri.advance_lower_and_check_if_present(rp);
                if (!possible_match) {
                    testlog.debug("No match");
                    SCYLLA_ASSERT(!reference_match);
                    // After a mismatch in the advance_lower_and_check_if_present,
                    // the reader is in a broken state, and can't be used anymore.
                    break;
                }
                SCYLLA_ASSERT(reader->element_kind() == sstables::indexable_element::partition);
                testlog.debug("reader->data_file_positions()={},{}, ri.data_file_positions()={},{}, upper_before={}",
                    reader->data_file_positions().start,
                    reader->data_file_positions().end,
                    ri.data_file_positions().start,
                    ri.data_file_positions().end,
                    upper_before);
                SCYLLA_ASSERT(reader->data_file_positions().start <= ri.data_file_positions().start);
                if (reference_match) {
                    SCYLLA_ASSERT(possible_match);
                    SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
                }
                SCYLLA_ASSERT(reader->data_file_positions().end == upper_before);
            } else if (auto vt = ri.valid_targets_for_advance_past_definitely_present_partition(); !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                auto dk = vt[target];
                auto upper_before = reader->data_file_positions().end;
                testlog.debug("advance_to_definitely_present_partition(dk={})", dk);
                reader->advance_to_definitely_present_partition(dk).get();
                ri.advance_to_definitely_present_partition(dk);
                SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
                SCYLLA_ASSERT(reader->data_file_positions().end == upper_before);
            } else if (auto vt = ri.valid_targets_for_advance_past_definitely_present_partition(); !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                auto dk = vt[target];
                auto upper_before = reader->data_file_positions().end;
                testlog.debug("advance_past_definitely_present_partition(dk={})", dk);
                reader->advance_past_definitely_present_partition(dk).get();
                ri.advance_past_definitely_present_partition(dk);
                SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
                SCYLLA_ASSERT(reader->data_file_positions().end == upper_before);
            } else if (!ndcs.choose_bool()) {
                std::optional<dht::partition_range::bound> lb;
                if (auto vt = ri.valid_lb_targets_for_advance_to(); !vt.empty() && !ndcs.choose_bool()) {
                    auto target = ndcs.choose_up_to(vt.size() - 1);
                    const auto& rp = vt[target];
                    auto inclusive = ndcs.choose_bool();
                    lb = dht::partition_range::bound(rp, inclusive);
                }
                std::optional<dht::partition_range::bound> ub;
                const auto& lb_rp = lb ? lb.value().value() : dht::ring_position::min();
                if (auto vt = ri.valid_ub_targets_for_advance_to(lb_rp); !vt.empty() && !ndcs.choose_bool()) {
                    auto target = ndcs.choose_up_to(vt.size() - 1);
                    const auto& rp = vt[target];
                    bool inclusive;
                    auto tricmp = dht::ring_position_comparator(ri._s);
                    if (lb.has_value() && lb.value().is_inclusive() && tricmp(lb_rp, rp) == std::strong_ordering::equal) {
                        inclusive = false;
                    } else {
                        inclusive = ndcs.choose_bool();
                    }
                    lb = dht::partition_range::bound(rp, inclusive);
                }
                auto pr = dht::partition_range(lb, ub);
                testlog.debug("advance_to(pr={})", pr);
                reader->advance_to(pr).get();
                ri.advance_to(pr);
                auto positions = reader->data_file_positions();
                SCYLLA_ASSERT(reader->element_kind() == sstables::indexable_element::partition);
                SCYLLA_ASSERT(positions.start <= ri.data_file_positions().start);
                if (ri.data_file_positions().end) {
                    SCYLLA_ASSERT(positions.end);
                    SCYLLA_ASSERT(*positions.end >= *ri.data_file_positions().end);
                } else {
                    SCYLLA_ASSERT(!positions.end);
                }
            } else if (!reader->eof() && !ndcs.choose_bool()) {
                testlog.debug("advance_to_next_partition()");
                reader->advance_to_next_partition().get();
                ri.advance_to_next_partition();
                SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
                SCYLLA_ASSERT(reader->data_file_positions().end == ri.data_file_positions().end);
            } else if (!ndcs.choose_bool()) {
                testlog.debug("advance_reverse_to_next_partition()");
                reader->advance_reverse_to_next_partition().get();
                ri.advance_reverse_to_next_partition();
                SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
                SCYLLA_ASSERT(reader->data_file_positions().end == ri.data_file_positions().end);
            } else if (auto vt = ri.valid_targets_for_advance_to_pip(); !reader->eof() && !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                const auto& pos = vt[target];
                testlog.debug("advance_to({})", pos);
                reader->advance_to(pos).get();
                ri.advance_to(pos);
                SCYLLA_ASSERT(reader->data_file_positions().start <= ri.data_file_positions().start);
                SCYLLA_ASSERT(reader->data_file_positions().end == ri.data_file_positions().end);
            } else if (auto vt = ri.valid_targets_for_advance_to_pip(); !reader->eof() && !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                const auto& pos = vt[target];
                testlog.debug("advance_upper_past({})", pos);
                reader->advance_upper_past(pos).get();
                ri.advance_upper_past(pos);
                SCYLLA_ASSERT(reader->data_file_positions().end.value() >= ri.data_file_positions().end.value());
                SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
            } else if (auto vt = ri.valid_targets_for_advance_reverse(); !reader->eof() && !vt.empty() && !ndcs.choose_bool()) {
                auto target = ndcs.choose_up_to(vt.size() - 1);
                const auto& pos = vt[target];
                testlog.debug("advance_upper_past({})", pos);
                reader->advance_reverse(pos).get();
                ri.advance_reverse(pos);
                SCYLLA_ASSERT(reader->data_file_positions().end.value() >= ri.data_file_positions().end.value());
                SCYLLA_ASSERT(reader->data_file_positions().start == ri.data_file_positions().start);
            } else {
                testlog.debug("read_partition_data()");
                auto positions_before = reader->data_file_positions();
                reader->read_partition_data().get();
                ri.read_partition_data();
                SCYLLA_ASSERT(reader->partition_data_ready());
                auto positions_after = reader->data_file_positions();
                SCYLLA_ASSERT(positions_before.start == positions_after.start);
                SCYLLA_ASSERT(positions_before.end == positions_after.end);
            }
            check_integrity();
        }
    } while (ndcs.rewind());
    testlog.info("Number of run method sequences: {}", n_cases);
};

SEASTAR_THREAD_TEST_CASE(test_exhaustive) {
    auto the_schema = schema_builder("ks", "t")
        .with_column("pk", short_type, column_kind::partition_key)
        .with_column("ck1", short_type, column_kind::clustering_key)
        .with_column("ck2", short_type, column_kind::clustering_key)
        .build();
    auto sst_ver = sstables::sstable_version_types::me;

    random_dataset_config cfg;
    const int max_ops = 3;
#ifdef SEASTAR_DEBUG
    // The test generates millions of futures,
    // so it's extremely slow in debug mode which induces a preemption
    // after every future.
    // So we downsize the test.
    // FIXME: that's not big enough for full coverage.
    // E.g. there are branches in partition index writer which are only taken
    // since third partition key onward.
    cfg.partition_universe_size = 2;
    cfg.clustering_universe_size = 4;
    cfg.max_clustering_blocks = 2;
#endif
    // Step 1: generate the test dataset.
    testlog.debug("Generating a random dataset.");
    auto dataset = generate_random_dataset(*the_schema, cfg);

    // Log the contents of the dataset for debugging purposes.
    for (const auto& entry : dataset.entries) {
        std::visit(overloaded_functor {
            [](const partition_index_entry& e) {
                testlog.debug("Partition index entry: dk={}, data_file_offset={}, partition_tombstone={}",
                    e.dk, e.data_file_offset, e.partition_tombstone);
            },
            [](const clustering_index_entry& e) {
                sstables::clustering_info first = e.first_ck;
                sstables::clustering_info last = e.last_ck;
                testlog.debug("Clustering index entry: first={}@{}, last={}@{}, data_file_offset={}, tombstone_before_first_ck={}",
                    first.kind, first.clustering, last.kind, last.clustering, e.data_file_offset, e.range_tombstone_before_first_ck);
            },
            [](const partition_end_entry& e) {
                testlog.debug("Partition end entry: data_file_offset={}", e.data_file_offset);
            },
            [](const eof_index_entry& e) {
                testlog.debug("Eof index entry: data_file_offset={}", e.data_file_offset);
            },
        }, entry);
    }

    // Step 2: write the index to BTI files.
    tmpdir dir;
    auto partitions_path = dir.path() / "Partitions.db";
    auto rows_path = dir.path() / "Rows.db";
    testlog.debug("Writing index to {} and {}", partitions_path.c_str(), rows_path.c_str());
    {
        file partitions_db = open_file_dma(partitions_path.c_str(), open_flags::create | open_flags::wo).get();
        file rows_db = open_file_dma(rows_path.c_str(), open_flags::create | open_flags::wo).get();

        sstables::file_writer partitions_db_writer(make_file_output_stream(partitions_db).get());
        sstables::file_writer rows_db_writer(make_file_output_stream(rows_db).get());

        auto close_partitions_db = defer([&] { partitions_db_writer.close(); });
        auto close_rows_db = defer([&] { rows_db_writer.close(); });

        auto partition_index_writer = sstables::trie::bti_partition_index_writer(partitions_db_writer);
        auto row_index_writer = sstables::trie::bti_row_index_writer(rows_db_writer);

        std::optional<partition_index_entry> last_partition_entry;
        std::optional<partition_end_entry> last_partition_end_entry;
        auto push_partition = [&] () {
            if (last_partition_entry) {
                auto& last = *last_partition_entry;
                auto payload = row_index_writer.finish(
                    sst_ver,
                    *the_schema,
                    last.data_file_offset,
                    last_partition_end_entry.value().data_file_offset,
                    last.dk.key(),
                    last.partition_tombstone);
                partition_index_writer.add(*the_schema, last.dk, payload);
            }
            last_partition_entry.reset();
        };

        for (const auto& entry : dataset.entries) {
            std::visit(overloaded_functor {
                [&](const partition_index_entry& e) {
                    push_partition();
                    last_partition_entry = e;
                },
                [&](const clustering_index_entry& e) {
                    row_index_writer.add(
                        *the_schema,
                        e.first_ck,
                        e.last_ck,
                        e.data_file_offset - last_partition_entry.value().data_file_offset,
                        e.range_tombstone_before_first_ck);
                },
                [&](const partition_end_entry& e) {
                    last_partition_end_entry = e;
                },
                [&](const eof_index_entry& e) {
                    push_partition();
                },
            }, entry);
        }
        std::move(partition_index_writer).finish(sst_ver, {}, {});
    }

    // Step 3: create the reader (or, more precisely, a factory of readers) over the index files.
    testlog.debug("Opening index from {} and {}", partitions_path.c_str(), rows_path.c_str());
    {
        file partitions_db = open_file_dma(partitions_path.c_str(), open_flags::create | open_flags::ro).get();
        file rows_db = open_file_dma(rows_path.c_str(), open_flags::create | open_flags::ro).get();
        auto close_partitions_db = deferred_close(partitions_db);
        auto close_rows_db = deferred_close(rows_db);

        auto stats = cached_file_stats();
        auto cached_file_lru = lru();
        auto region = logalloc::region();
        auto partitions_db_size = partitions_db.size().get();
        auto rows_db_size = rows_db.size().get();
        auto partitions_db_cached = cached_file(partitions_db, stats, cached_file_lru, region, partitions_db_size, "Partitions.db");
        auto rows_db_cached = cached_file(rows_db, stats, cached_file_lru, region, rows_db_size, "Rows.db");

        auto p = partitions_db_cached.get_file().dma_read_exactly<char>(partitions_db_size - 24, 24).get();
        auto partitions_db_root_pos = read_be<uint64_t>(p.get() + 16);

        auto semaphore = tests::reader_concurrency_semaphore_wrapper();

        // Step 4: run the reader test on the opened readers.
        test_index(dataset, [&] {
            return sstables::trie::make_bti_index_reader(
            partitions_db_cached,
            rows_db_cached,
            partitions_db_root_pos,
            std::get<eof_index_entry>(dataset.entries.back()).data_file_offset,
            the_schema,
            semaphore.make_permit());
        }, max_ops);
    }
}