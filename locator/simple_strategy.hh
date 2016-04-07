/*
 * Copyright (C) 2015 ScyllaDB
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

#include "abstract_replication_strategy.hh"

#include <experimental/optional>
#include <set>

namespace locator {

class simple_strategy : public abstract_replication_strategy {
protected:
    virtual std::vector<inet_address> calculate_natural_endpoints(const token& search_token, token_metadata& tm) const override;
public:
    simple_strategy(const sstring& keyspace_name, token_metadata& token_metadata, snitch_ptr& snitch, const std::map<sstring, sstring>& config_options);
    virtual ~simple_strategy() {};
    virtual size_t get_replication_factor() const override;
    virtual void validate_options() const override;
    virtual std::experimental::optional<std::set<sstring>> recognized_options() const override;
private:
    size_t _replication_factor = 1;
};

}
