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

#include "repair/row.hh"

repair_row::repair_row(std::optional<frozen_mutation_fragment> fm,
           std::optional<position_in_partition> pos,
           lw_shared_ptr<const decorated_key_with_hash> dk_with_hash,
           std::optional<repair_hash> hash,
           is_dirty_on_master dirty_on_master,
           lw_shared_ptr<mutation_fragment> mf)
    : _fm(std::move(fm))
    , _dk_with_hash(std::move(dk_with_hash))
    , _boundary(pos ? std::optional<repair_sync_boundary>(repair_sync_boundary{_dk_with_hash->dk, std::move(*pos)}) : std::nullopt)
    , _hash(std::move(hash))
    , _dirty_on_master(dirty_on_master)
    , _mf(std::move(mf)) {
}
