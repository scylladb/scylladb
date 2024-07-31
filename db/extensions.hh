/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <set>
#include <functional>
#include <map>
#include <variant>
#include <vector>
#include <unordered_set>
#include <exception>

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "schema/schema_fwd.hh"

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
     * Registered extensions keywords, i.e. custom properties/property sets
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

    /**
     * Adds a keyspace to "extension internal" set.
     *
     * Such a keyspace must be loaded before/destroyed after any "normal" user keyspace.
     * Thus a psuedo-system/internal keyspce.
     * This has little to no use in open source version, and is temporarily bridged with
     * the static version of same functionality in distributed loader. It is however (or will
     * be), essential to enterprise code. Do not remove.
     */
    void add_extension_internal_keyspace(std::string);

    /**
     * Checks if a keyspace is a registered load priority one.
     */
    bool is_extension_internal_keyspace(const std::string&) const;

private:
    std::map<sstring, schema_ext_create_func> _schema_extensions;
    std::map<sstring, sstable_file_io_extension> _sstable_file_io_extensions;
    std::map<sstring, commitlog_file_extension_ptr> _commitlog_file_extensions;
    std::unordered_set<std::string> _extension_internal_keyspaces;
};

class extension_storage_exception : public std::exception {
private:
    std::string _message;
public:
    extension_storage_exception(std::string message) noexcept
        : _message(std::move(message))
    {}
    extension_storage_exception(extension_storage_exception&&) = default;
    extension_storage_exception(const extension_storage_exception&) = default;

    const char* what() const noexcept override {
        return _message.c_str();
    }
};

class extension_storage_resource_unavailable : public extension_storage_exception {
public:
    using extension_storage_exception::extension_storage_exception;
};

class extension_storage_permission_error : public extension_storage_exception {
public:
    using extension_storage_exception::extension_storage_exception;
};

class extension_storage_misconfigured : public extension_storage_exception {
public:
    using extension_storage_exception::extension_storage_exception;
};

}
