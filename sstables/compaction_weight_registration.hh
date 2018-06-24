/*
 * Copyright (C) 2017 ScyllaDB
 *
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

class compaction_manager;

class compaction_weight_registration {
    compaction_manager* _cm;
    int _weight;
public:
    compaction_weight_registration(compaction_manager* cm, int weight);

    compaction_weight_registration& operator=(const compaction_weight_registration&) = delete;
    compaction_weight_registration(const compaction_weight_registration&) = delete;

    compaction_weight_registration& operator=(compaction_weight_registration&& other) noexcept;

    compaction_weight_registration(compaction_weight_registration&& other) noexcept;

    ~compaction_weight_registration();

    // Release immediately the weight hold by this object
    void deregister();

    int weight() const;
};
