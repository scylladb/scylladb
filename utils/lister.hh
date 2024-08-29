/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <unordered_set>
#include <filesystem>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/file.hh>
#include <seastar/core/enum.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/pipe.hh>
#include <seastar/util/bool_class.hh>
#include "enum_set.hh"
#include "seastarx.hh"

namespace fs = std::filesystem;

class lister final {
public:
    /**
     * Types of entries to list. If empty - list all present entries except for
     * hidden if not requested to.
     */
    using listable_entry_types = super_enum<directory_entry_type, directory_entry_type::regular, directory_entry_type::directory>;
    using dir_entry_types = enum_set<listable_entry_types>;
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

class abstract_lister {
public:
    class impl {
    public:
        // Get the next directory_entry from the lister,
        // if available.  When the directory listing is done,
        // a disengaged optional is returned.
        //
        // Caller should either drain all entries using get()
        // until it gets a disengaged result or an error, or
        // close() can be called to terminate the listing prematurely,
        // and wait on any background work to complete.
        //
        // Calling get() after the listing is done and a disengaged
        // result has been returned results in a broken_pipe_exception.
        virtual future<std::optional<directory_entry>> get() = 0;

        // Close the directory_lister, ignoring any errors.
        // Must be called after get() if not all entries were retrieved.
        //
        // Close aborts the lister, waking up get() if any is waiting,
        // and waits for all background work to complete.
        virtual future<> close() noexcept = 0;
        virtual ~impl() = default;
    };

private:
    std::unique_ptr<impl> _impl;
    abstract_lister(std::unique_ptr<impl> i) noexcept : _impl(std::move(i)) {}

public:
    future<std::optional<directory_entry>> get() {
        return _impl->get();
    }
    future<> close() noexcept {
        return _impl->close();
    }

    abstract_lister(abstract_lister&&) noexcept = default;

    template <typename L, typename... Args>
    static abstract_lister make(Args&&... args) {
        return abstract_lister(std::make_unique<L>(std::forward<Args>(args)...));
    }
};

class directory_lister final : public abstract_lister::impl {
    fs::path _dir;
    lister::dir_entry_types _type;
    lister::filter_type _filter;
    lister::show_hidden _do_show_hidden;
    seastar::queue<std::optional<directory_entry>> _queue;
    std::unique_ptr<lister> _lister;
    std::optional<future<>> _opt_done_fut;
public:
    directory_lister(fs::path dir,
            lister::dir_entry_types type = lister::dir_entry_types::full(),
            lister::filter_type filter = [] (const fs::path& parent_dir, const directory_entry& entry) { return true; },
            lister::show_hidden do_show_hidden = lister::show_hidden::yes) noexcept
        : _dir(std::move(dir))
        , _type(type)
        , _filter(std::move(filter))
        , _do_show_hidden(do_show_hidden)
        , _queue(512 / sizeof(std::optional<directory_entry>))
    { }

    ~directory_lister();

    future<std::optional<directory_entry>> get() override;
    future<> close() noexcept override;
};

static inline fs::path operator/(const fs::path& lhs, const char* rhs) {
    return lhs / fs::path(rhs);
}

static inline fs::path operator/(const fs::path& lhs, const sstring& rhs) {
    return lhs / fs::path(rhs);
}

static inline fs::path operator/(const fs::path& lhs, std::string_view rhs) {
    return lhs / fs::path(rhs);
}

static inline fs::path operator/(const fs::path& lhs, const std::string& rhs) {
    return lhs / fs::path(rhs);
}
