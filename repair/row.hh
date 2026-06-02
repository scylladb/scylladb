/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once
#include <memory>
#include <optional>
#include "mutation/frozen_mutation.hh"
#include <seastar/core/shared_ptr.hh>
#include "repair/decorated_key_with_hash.hh"
#include "repair/hash.hh"
#include "repair/sync_boundary.hh"

using namespace seastar;

using is_dirty_on_master = bool_class<class is_dirty_on_master_tag>;
class decorated_key_with_hash;
class repair_hash;

class repair_row {
    std::optional<frozen_mutation_fragment> _fm;
    lw_shared_ptr<const decorated_key_with_hash> _dk_with_hash;
    // Only the master side needs a sync boundary; on the follower side it is
    // always empty. Hold it behind a unique_ptr so an empty boundary costs one
    // pointer (8B) instead of an inline std::optional<repair_sync_boundary>
    // (64B), shrinking the per-row footprint that is multiplied by the number
    // of rows buffered during a repair round.
    std::unique_ptr<repair_sync_boundary> _boundary;
    std::optional<repair_hash> _hash;
    is_dirty_on_master _dirty_on_master;
    lw_shared_ptr<mutation_fragment> _mf;
public:
    repair_row() = default;
    repair_row(std::optional<frozen_mutation_fragment> fm,
            std::optional<position_in_partition> pos,
            lw_shared_ptr<const decorated_key_with_hash> dk_with_hash,
            std::optional<repair_hash> hash,
            is_dirty_on_master dirty_on_master,
            lw_shared_ptr<mutation_fragment> mf = {})
            : _fm(std::move(fm))
            , _dk_with_hash(std::move(dk_with_hash))
            , _boundary(pos ? std::make_unique<repair_sync_boundary>(repair_sync_boundary{_dk_with_hash->dk, std::move(*pos)}) : nullptr)
            , _hash(std::move(hash))
            , _dirty_on_master(dirty_on_master)
            , _mf(std::move(mf))
    { }
    // repair_row is move-only because _boundary is held behind a unique_ptr.
    // The few places that intentionally duplicate a row (copying out of the
    // working buffer) must use copy() so the deep copy of the sync boundary is
    // explicit rather than accidental.
    repair_row copy() const {
        repair_row r;
        r._fm = _fm;
        r._dk_with_hash = _dk_with_hash;
        r._boundary = _boundary ? std::make_unique<repair_sync_boundary>(*_boundary) : nullptr;
        r._hash = _hash;
        r._dirty_on_master = _dirty_on_master;
        r._mf = _mf;
        return r;
    }
    lw_shared_ptr<mutation_fragment>& get_mutation_fragment_ptr() { return _mf; }
    mutation_fragment& get_mutation_fragment() {
        if (!_mf) {
            throw std::runtime_error("empty mutation_fragment");
        }
        return *_mf;
    }
    void reset_mutation_fragment() {
        _mf = nullptr;
    }
    frozen_mutation_fragment& get_frozen_mutation() {
        if (!_fm) {
            throw std::runtime_error("empty frozen_mutation_fragment");
        }
        return *_fm;
    }
    const frozen_mutation_fragment& get_frozen_mutation() const {
        if (!_fm) {
            throw std::runtime_error("empty frozen_mutation_fragment");
        }
        return *_fm;
    }
    const lw_shared_ptr<const decorated_key_with_hash>& get_dk_with_hash() const {
        return _dk_with_hash;
    }
    size_t size() const {
        if (!_fm) {
            throw std::runtime_error("empty size due to empty frozen_mutation_fragment");
        }
        auto size = sizeof(repair_row) + _fm->representation().size();
        if (_boundary) {
            // Account for the heap node itself; it used to be inline in sizeof(repair_row).
            size += sizeof(repair_sync_boundary) + _boundary->pk.external_memory_usage() + _boundary->position.external_memory_usage();
        }
        if (_mf) {
            size += _mf->memory_usage();
        }
        return size;
    }
    const repair_sync_boundary& boundary() const {
        if (!_boundary) {
            throw std::runtime_error("empty repair_sync_boundary");
        }
        return *_boundary;
    }
    const repair_hash& hash() const {
        if (!_hash) {
            throw std::runtime_error("empty hash");
        }
        return *_hash;
    }
    is_dirty_on_master dirty_on_master() const {
        return _dirty_on_master;
    }
    future<> clear_gently() noexcept {
        if (_fm) {
            co_await _fm->clear_gently();
            _fm.reset();
        }
        _dk_with_hash = {};
        _boundary.reset();
        _hash.reset();
        _mf = {};
    }
};


