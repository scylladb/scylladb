/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <iosfwd>
#include <set>
#include <seastar/core/sstring.hh>

namespace auth {

class data_resource {
private:
    enum class level {
        ROOT, KEYSPACE, COLUMN_FAMILY
    };

    static const sstring ROOT_NAME;

    sstring _ks;
    sstring _cf;

    data_resource(level, const sstring& ks = {}, const sstring& cf = {});

    level get_level() const;
public:
    /**
     * Creates a DataResource representing the root-level resource.
     * @return the root-level resource.
     */
    data_resource();
    /**
     * Creates a DataResource representing a keyspace.
     *
     * @param keyspace Name of the keyspace.
     */
    data_resource(const sstring& ks);
    /**
     * Creates a DataResource instance representing a column family.
     *
     * @param keyspace Name of the keyspace.
     * @param columnFamily Name of the column family.
     */
    data_resource(const sstring& ks, const sstring& cf);

    /**
     * Parses a data resource name into a DataResource instance.
     *
     * @param name Name of the data resource.
     * @return DataResource instance matching the name.
     */
    static data_resource from_name(const sstring&);

    /**
     * @return Printable name of the resource.
     */
    sstring name() const;

    /**
     * @return Parent of the resource, if any. Throws IllegalStateException if it's the root-level resource.
     */
    data_resource get_parent() const;

    bool is_root_level() const {
        return get_level() == level::ROOT;
    }

    bool is_keyspace_level() const {
        return get_level() == level::KEYSPACE;
    }

    bool is_column_family_level() const {
        return get_level() == level::COLUMN_FAMILY;
    }

    /**
     * @return keyspace of the resource.
     * @throws std::invalid_argument if it's the root-level resource.
     */
    const sstring& keyspace() const throw(std::invalid_argument);

    /**
     * @return column family of the resource.
     * @throws std::invalid_argument if it's not a cf-level resource.
     */
    const sstring& column_family() const throw(std::invalid_argument);

    /**
     * @return Whether or not the resource has a parent in the hierarchy.
     */
    bool has_parent() const;

    /**
     * @return Whether or not the resource exists in scylla.
     */
    bool exists() const;

    sstring to_string() const;

    bool operator==(const data_resource&) const;
    bool operator<(const data_resource&) const;
};

/**
 * Resource id mappings, i.e. keyspace and/or column families.
 */
using resource_ids = std::set<data_resource>;

std::ostream& operator<<(std::ostream&, const data_resource&);

}



