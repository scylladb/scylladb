/*
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

#include <unordered_set>
#include <filesystem>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/file.hh>
#include <seastar/core/enum.hh>
#include <seastar/util/bool_class.hh>

#include "seastarx.hh"

namespace fs = std::filesystem;

class lister final {
public:
    /**
     * Types of entries to list. If empty - list all present entries except for
     * hidden if not requested to.
     */
    using dir_entry_types = std::unordered_set<directory_entry_type, enum_hash<directory_entry_type>>;
    /**
     * This callback is going to be called for each entry in the given directory
     * that has the corresponding type and meets the filter demands.
     *
     * First parameter is a path object for the base directory.
     *
     * Second parameter is a directory_entry object of the file for which this
     * callback is being called.
     *
     * The first parameter of the callback represents a parent directory of
     * each entry defined by the second parameter.
     */
    using walker_type = std::function<future<> (fs::path, directory_entry)>;
    using filter_type = std::function<bool (const fs::path&, const directory_entry&)>;

    struct show_hidden_tag {};
    using show_hidden = bool_class<show_hidden_tag>;

private:
    file _f;
    walker_type _walker;
    filter_type _filter;
    dir_entry_types _expected_type;
    future<> _listing_done;
    fs::path _dir;
    show_hidden _show_hidden;

public:
    /**
     * Scans the directory calling a "walker" callback for each entry that satisfies the filtering.
     *
     * @param dir Directory to scan.
     * @param type Type of entries to process. Entries of other types will be ignored.
     * @param do_show_hidden if TRUE - the hidden entries are going to be processed as well.
     * @param walker A callback to be called for each entry that satisfies the filtering rules.
     * @param filter A filter callback that is called for each entry of the requested type: if returns FALSE - the entry will be skipped.
     *
     * @return A future that resolves when all entries processing is finished or an error occurs. In the later case an exceptional future is returened.
     */
    static future<> scan_dir(fs::path dir, dir_entry_types type, show_hidden do_show_hidden, walker_type walker, filter_type filter);

    /**
     * Overload of scan_dir() that uses a show_hidden::no when it's not given.
     */
    static future<> scan_dir(fs::path dir, dir_entry_types type, walker_type walker, filter_type filter) {
        return scan_dir(std::move(dir), std::move(type), show_hidden::no, std::move(walker), std::move(filter));
    }

    /**
     * Overload of scan_dir() that uses a show_hidden::no and a filter that returns TRUE for every entry when they are not given.
     */
    static future<> scan_dir(fs::path dir, dir_entry_types type, walker_type walker) {
        return scan_dir(std::move(dir), std::move(type), show_hidden::no, std::move(walker), [] (const fs::path& parent_dir, const directory_entry& entry) { return true; });
    }

    /**
     * Overload of scan_dir() that uses a filter that returns TRUE for every entry when filter is not given.
     */
    static future<> scan_dir(fs::path dir, dir_entry_types type, show_hidden do_show_hidden, walker_type walker) {
        return scan_dir(std::move(dir), std::move(type), do_show_hidden, std::move(walker), [] (const fs::path& parent_dir, const directory_entry& entry) { return true; });
    }

    /** Overloads accepting sstring as the first parameter */
    static future<> scan_dir(sstring dir, dir_entry_types type, show_hidden do_show_hidden, walker_type walker, filter_type filter) {
        return scan_dir(fs::path(std::move(dir)), std::move(type), do_show_hidden, std::move(walker), std::move(filter));
    }
    static future<> scan_dir(sstring dir, dir_entry_types type, walker_type walker, filter_type filter) {
        return scan_dir(fs::path(std::move(dir)), std::move(type), show_hidden::no, std::move(walker), std::move(filter));
    }
    static future<> scan_dir(sstring dir, dir_entry_types type, walker_type walker) {
        return scan_dir(fs::path(std::move(dir)), std::move(type), show_hidden::no, std::move(walker), [] (const fs::path& parent_dir, const directory_entry& entry) { return true; });
    }
    static future<> scan_dir(sstring dir, dir_entry_types type, show_hidden do_show_hidden, walker_type walker) {
        return scan_dir(fs::path(std::move(dir)), std::move(type), do_show_hidden, std::move(walker), [] (const fs::path& parent_dir, const directory_entry& entry) { return true; });
    }

    /**
     * Removes the given directory with all its contents (like 'rm -rf <dir>' shell command).
     *
     * @param dir Directory to remove.
     * @return A future that resolves when the operation is complete or an error occurs.
     */
    static future<> rmdir(fs::path dir);

    /**
     * Constructor
     *
     * @param f A file instance for the directory to scan.
     * @param type Types of entries to scan.
     * @param walker A callback to be called for each entry that satisfies the filtering rules.
     * @param filter A filter callback that is called for each entry of the requested type: if returns FALSE - the entry will be skipped.
     * @param dir A seastar::path object for the directory to scan.
     * @param do_show_hidden if TRUE - scan hidden entries as well.
     */
    lister(file f, dir_entry_types type, walker_type walker, filter_type filter, fs::path dir, show_hidden do_show_hidden);

    /**
     * @return a future that resolves when the directory scanning is complete.
     */
    future<> done();

private:
    /**
     * Handle a single entry.
     *
     * @param de Descriptor of the entry to handle.
     * @return A future that resolves when the handling is over.
     */
    future<> visit(directory_entry de);

    /**
     * Validates that the input parameter has its "type" optional field engaged.
     *
     * This helper method is called before further processing the @param de in order
     * to ensure that its "type" field is engaged.
     *
     * If it is engaged - returns the input value as is.
     * If "type"  isn't engaged - calls the file_type() for file represented by @param de and sets
     * "type" field of @param de to the returned value and then returns @param de.
     *
     * @param de entry to check and return
     * @return a future that resolves with the @param de with the engaged de.type field or an
     * exceptional future with std::system_error exception if type of the file represented by @param de may not be retrieved.
     */
    future<directory_entry> guarantee_type(directory_entry de);
};

static inline fs::path operator/(const fs::path& lhs, const char* rhs) {
    return lhs / fs::path(rhs);
}

static inline fs::path operator/(const fs::path& lhs, const sstring& rhs) {
    return lhs / fs::path(rhs);
}
