/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include <optional>
#include "mutation/frozen_mutation.hh"
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/coroutine.hh>
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
    std::optional<repair_sync_boundary> _boundary;
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
            , _boundary(pos ? std::optional<repair_sync_boundary>(repair_sync_boundary{_dk_with_hash->dk, std::move(*pos)}) : std::nullopt)
            , _hash(std::move(hash))
            , _dirty_on_master(dirty_on_master)
            , _mf(std::move(mf))
    { }
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
            size += _boundary->pk.external_memory_usage() + _boundary->position.external_memory_usage();
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


