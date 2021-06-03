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
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
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

#include <set>
#include <stdexcept>
#include <functional>
#include <map>
#include <variant>
#include <vector>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "schema_fwd.hh"

namespace sstables {
class file_io_extension;
}

namespace db {
class commitlog_file_extension;

class extensions {
public:
    extensions();
    ~extensions();

    using map_type = std::map<sstring, sstring>;
    using schema_ext_config = std::variant<sstring, map_type, bytes>;
    using schema_ext_create_func = std::function<seastar::shared_ptr<schema_extension>(schema_ext_config)>;
    using sstable_file_io_extension = std::unique_ptr<sstables::file_io_extension>;
    using commitlog_file_extension_ptr = std::unique_ptr<db::commitlog_file_extension>;

    /**
     * Registered extensions
     */
    const std::map<sstring, schema_ext_create_func>& schema_extensions() const {
        return _schema_extensions;
    }
    /**
     * Returns iterable range of registered sstable IO extensions (see sstable.hh#sstable_file_io_extension)
     * For any sstables wanting to call these on file open...
     */
    std::vector<sstables::file_io_extension*> sstable_file_io_extensions() const;

    /**
     * Returns iterable range of registered commitlog IO extensions (see commitlog_extensions.hh#commitlog_file_extension)
     * For any commitlogs wanting to call these on file open or descriptor scan...
     */
    std::vector<db::commitlog_file_extension*> commitlog_file_extensions() const;

    /**
     * Registered extensions keywords, i.e. custom properties/propery sets
     * for schema extensions
     */
    std::set<sstring> schema_extension_keywords() const;

    /**
     * Init time method to add schema extension.
     */
    void add_schema_extension(sstring w, schema_ext_create_func f) {
        _schema_extensions.emplace(std::move(w), std::move(f));
    }
    /**
     * A shorthand for the add_schema_extension. Adds a function that adds
     * the extension to a schema with appropriate constructor overload.
     */
    template<typename Extension>
    void add_schema_extension(sstring w) {
        add_schema_extension(std::move(w), [] (db::extensions::schema_ext_config cfg) {
            return std::visit([] (auto v) {
                return ::make_shared<Extension>(v);
            }, cfg);
        });
    }
    /**
     * Init time method to add sstable extension
     */
    void add_sstable_file_io_extension(sstring n, sstable_file_io_extension);
    /**
     * Init time method to add sstable extension
     */
    void add_commitlog_file_extension(sstring n, commitlog_file_extension_ptr);

    /**
     * Allows forcible modification of schema extensions of a schema. This should
     * not be done lightly however. In fact, it should only be done on startup
     * at most, and thus this method is non-const, i.e. you can only use it on
     * config apply.
     */
    void add_extension_to_schema(schema_ptr, const sstring&, shared_ptr<schema_extension>);
private:
    std::map<sstring, schema_ext_create_func> _schema_extensions;
    std::map<sstring, sstable_file_io_extension> _sstable_file_io_extensions;
    std::map<sstring, commitlog_file_extension_ptr> _commitlog_file_extensions;
};
}
