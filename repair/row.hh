/*
 * Copyright (C) 2018 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <vector>
#include <optional>
#include <stdexcept>
#include <seastar/core/shared_ptr.hh>
#include "repair/repair.hh"
#include "mutation_fragment.hh"
#include "repair/hash.hh"

using is_dirty_on_master = bool_class<class is_dirty_on_master_tag>;

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
               lw_shared_ptr<mutation_fragment> mf = {});

    lw_shared_ptr<mutation_fragment>& get_mutation_fragment_ptr () {
        return _mf;
    }

    mutation_fragment& get_mutation_fragment() {
        if (!_mf) {
            throw std::runtime_error("empty mutation_fragment");
        }
        return *_mf;
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
        return _fm->representation().size();
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
};
