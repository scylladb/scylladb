/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage.hh"

#include <cerrno>
#include <boost/algorithm/string.hpp>

#include <exception>
#include <stdexcept>
#include <fmt/std.h>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/util/file.hh>
#include <seastar/util/closeable.hh>

#include "db/config.hh"
#include "sstables/exceptions.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_version.hh"
#include "sstables/integrity_checked_file_impl.hh"
#include "sstables/writer.hh"
#include "utils/assert.hh"
#include "utils/lister.hh"
#include "utils/overloaded_functor.hh"
#include "utils/memory_data_sink.hh"
#include "utils/s3/client.hh"
#include "utils/exceptions.hh"
#include "utils/to_string.hh"

#include "checked-file-impl.hh"

namespace sstables {

class opened_directory final {
    std::filesystem::path _pathname;
    file _file;

public:
    explicit opened_directory(std::filesystem::path pathname) : _pathname(std::move(pathname)) {};
    explicit opened_directory(const sstring &dir) : _pathname(std::string_view(dir)) {};
    opened_directory(const opened_directory&) = delete;
    opened_directory& operator=(const opened_directory&) = delete;
    opened_directory(opened_directory&&) = default;
    opened_directory& operator=(opened_directory&&) = default;
    ~opened_directory() = default;

    const std::filesystem::path::string_type& native() const noexcept {
        return _pathname.native();
    }

    const std::filesystem::path& path() const noexcept {
        return _pathname;
    }

    future<> sync(io_error_handler error_handler) {
        if (!_file) {
            _file = co_await do_io_check(error_handler, open_directory, _pathname.native());
        }
        co_await do_io_check(error_handler, std::mem_fn(&file::flush), _file);
    };

    future<> close() {
        return _file ? _file.close() : make_ready_future<>();
    }
};

// cannot define these classes in an anonymous namespace, as we need to
// declare these storage classes as "friend" of class sstable
class filesystem_storage final : public sstables::storage {
    mutable opened_directory _dir;
    std::optional<std::filesystem::path> _temp_dir; // Valid while the sstable is being created, until sealed

private:
    using mark_for_removal = bool_class<class mark_for_removal_tag>;


    future<> check_create_links_replay(const sstable& sst, const sstring& dst_dir, generation_type dst_gen, const std::vector<std::pair<sstables::component_type, sstring>>& comps) const;
    future<> remove_temp_dir();
    virtual future<> create_links(const sstable& sst, const std::filesystem::path& dir) const override;
    future<> create_links_common(const sstable& sst, sstring dst_dir, generation_type dst_gen, mark_for_removal mark_for_removal) const;
    future<> create_links_common(const sstable& sst, const std::filesystem::path& dir, std::optional<generation_type> dst_gen) const;
    future<> touch_temp_dir(const sstable& sst);
    future<> move(const sstable& sst, sstring new_dir, generation_type generation, delayed_commit_changes* delay) override;
    future<> rename_new_file(const sstable& sst, sstring from_name, sstring to_name) const;

    future<> change_dir(sstring new_dir) {
        auto old_dir = std::exchange(_dir, opened_directory(new_dir));
        return old_dir.close();
    }

    virtual future<> change_dir_for_test(sstring nd) override {
        return change_dir(nd);
    }

public:
    explicit filesystem_storage(sstring dir, sstable_state state)
        : _dir(make_path(dir, state))
    {}

    virtual future<> seal(const sstable& sst) override;
    virtual future<> snapshot(const sstable& sst, sstring dir, absolute_path abs, std::optional<generation_type> gen) const override;
    virtual future<> change_state(const sstable& sst, sstable_state state, generation_type generation, delayed_commit_changes* delay) override;
    // runs in async context
    virtual void open(sstable& sst) override;
    virtual future<> wipe(const sstable& sst, sync_dir) noexcept override;
    virtual future<file> open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) override;
    virtual future<data_sink> make_data_or_index_sink(sstable& sst, component_type type) override;
    virtual future<data_sink> make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) override;
    virtual future<> destroy(const sstable& sst) override { return make_ready_future<>(); }
    virtual future<atomic_delete_context> atomic_delete_prepare(const std::vector<shared_sstable>&) const override;
    virtual future<> atomic_delete_complete(atomic_delete_context ctx) const override;
    virtual future<> remove_by_registry_entry(entry_descriptor desc) override;
    virtual future<uint64_t> free_space() const override {
        return seastar::fs_avail(prefix());
    }

    virtual sstring prefix() const override { return _dir.native(); }
};

future<data_sink> filesystem_storage::make_data_or_index_sink(sstable& sst, component_type type) {
    file_output_stream_options options;
    options.buffer_size = sst.sstable_buffer_size;
    options.write_behind = 10;

    SCYLLA_ASSERT(type == component_type::Data || type == component_type::Index);
    return make_file_data_sink(type == component_type::Data ? std::move(sst._data_file) : std::move(sst._index_file), options);
}

future<data_sink> filesystem_storage::make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) {
    return sst.new_sstable_component_file(sst._write_error_handler, type, oflags).then([options = std::move(options)] (file f) mutable {
        return make_file_data_sink(std::move(f), std::move(options));
    });
}

static future<file> open_sstable_component_file_non_checked(std::string_view name, open_flags flags, file_open_options options,
        bool check_integrity) noexcept {
    if (flags != open_flags::ro && check_integrity) {
        return open_integrity_checked_file_dma(name, flags, options);
    }
    return open_file_dma(name, flags, options);
}

future<> filesystem_storage::rename_new_file(const sstable& sst, sstring from_name, sstring to_name) const {
    return sst.sstable_write_io_check(rename_file, from_name, to_name).handle_exception([from_name, to_name] (std::exception_ptr ep) {
        sstlog.error("Could not rename SSTable component {} to {}. Found exception: {}", from_name, to_name, ep);
        return make_exception_future<>(ep);
    });
}

future<file> filesystem_storage::open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) {
    auto create_flags = open_flags::create | open_flags::exclusive;
    auto readonly = (flags & create_flags) != create_flags;
    auto tgt_dir = !readonly && _temp_dir ? *_temp_dir : _dir.path();
    auto name = tgt_dir / sst.component_basename(type);

    auto f = open_sstable_component_file_non_checked(name.native(), flags, options, check_integrity);

    if (!readonly) {
        f = with_file_close_on_failure(std::move(f), [this, &sst, type, name = std::move(name)] (file fd) mutable {
            return rename_new_file(sst, name.native(), sst.filename(type)).then([fd = std::move(fd)] () mutable {
                return make_ready_future<file>(std::move(fd));
            });
        });
    }

    return f;
}

void filesystem_storage::open(sstable& sst) {
    touch_temp_dir(sst).get();
    auto file_path = sst.filename(component_type::TemporaryTOC);

    // Writing TOC content to temporary file.
    // If creation of temporary TOC failed, it implies that that boot failed to
    // delete a sstable with temporary for this column family, or there is a
    // sstable being created in parallel with the same generation.
    file_output_stream_options options;
    options.buffer_size = 4096;
    auto sink = make_component_sink(sst, component_type::TemporaryTOC,
                                    open_flags::wo |
                                    open_flags::create |
                                    open_flags::exclusive,
                                    options).get();
    auto w = file_writer(output_stream<char>(std::move(sink)), std::move(file_path));

    bool toc_exists = file_exists(sst.filename(component_type::TOC)).get();
    if (toc_exists) {
        // TOC will exist at this point if write_components() was called with
        // the generation of a sstable that exists.
        w.close();
        remove_file(sst.filename(component_type::TemporaryTOC)).get();
        throw std::runtime_error(format("SSTable write failed due to existence of TOC file for generation {} of {}.{}", sst._generation, sst._schema->ks_name(), sst._schema->cf_name()));
    }

    sst.write_toc(std::move(w));

    // Flushing parent directory to guarantee that temporary TOC file reached
    // the disk.
    _dir.sync(sst._write_error_handler).get();
}

future<> filesystem_storage::seal(const sstable& sst) {
    // SSTable sealing is about renaming temporary TOC file after guaranteeing
    // that each component reached the disk safely.
    co_await remove_temp_dir();
    // Guarantee that every component of this sstable reached the disk.
    co_await _dir.sync(sst._write_error_handler);
    // Rename TOC because it's no longer temporary.
    co_await sst.sstable_write_io_check(rename_file, sst.filename(component_type::TemporaryTOC), sst.filename(component_type::TOC));
    co_await _dir.sync(sst._write_error_handler);
    // If this point was reached, sstable should be safe in disk.
    sstlog.debug("SSTable with generation {} of {}.{} was sealed successfully.", sst._generation, sst._schema->ks_name(), sst._schema->cf_name());
}

future<> filesystem_storage::touch_temp_dir(const sstable& sst) {
    if (_temp_dir) {
        co_return;
    }
    auto tmp = _dir.path() / fmt::format("{}{}", sst._generation, tempdir_extension);
    sstlog.debug("Touching temp_dir={}", tmp);
    co_await sst.sstable_touch_directory_io_check(tmp);
    _temp_dir = std::move(tmp);
}

future<> filesystem_storage::remove_temp_dir() {
    if (!_temp_dir) {
        co_return;
    }
    std::optional<int> opt;
    sstlog.debug("Removing temp_dir={}", opt);
    //sstlog.debug("Removing temp_dir={}", _temp_dir);
    try {
        co_await remove_file(_temp_dir->native());
    } catch (...) {
        sstlog.error("Could not remove temporary directory: {}", std::current_exception());
        throw;
    }

    _temp_dir.reset();
}

static bool is_same_file(const seastar::stat_data& sd1, const seastar::stat_data& sd2) noexcept {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

static future<bool> same_file(sstring path1, sstring path2) noexcept {
    return when_all_succeed(file_stat(std::move(path1)), file_stat(std::move(path2))).then_unpack([] (seastar::stat_data sd1, seastar::stat_data sd2) {
        return is_same_file(sd1, sd2);
    });
}

// support replay of link by considering link_file EEXIST error as successful when the newpath is hard linked to oldpath.
future<> idempotent_link_file(sstring oldpath, sstring newpath) noexcept {
    bool exists = false;
    std::exception_ptr ex;
    try {
        co_await link_file(oldpath, newpath);
    } catch (const std::system_error& e) {
        ex = std::current_exception();
        exists = (e.code().value() == EEXIST);
    } catch (...) {
        ex = std::current_exception();
    }
    if (!ex) {
        co_return;
    }
    if (exists && (co_await same_file(oldpath, newpath))) {
        co_return;
    }
    co_await coroutine::return_exception_ptr(std::move(ex));
}

// Check is the operation is replayed, possibly when moving sstables
// from staging to the base dir, for example, right after create_links completes,
// and right before deleting the source links.
// We end up in two valid sstables in this case, so make create_links idempotent.
future<> filesystem_storage::check_create_links_replay(const sstable& sst, const sstring& dst_dir, generation_type dst_gen,
        const std::vector<std::pair<sstables::component_type, sstring>>& comps) const {
    return parallel_for_each(comps, [this, &sst, &dst_dir, dst_gen] (const auto& p) mutable {
        auto comp = p.second;
        auto src = sstable::filename(_dir.native(), sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, comp);
        auto dst = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, dst_gen, sst._format, comp);
        return do_with(std::move(src), std::move(dst), [this] (const sstring& src, const sstring& dst) mutable {
            return file_exists(dst).then([&, this] (bool exists) mutable {
                if (!exists) {
                    return make_ready_future<>();
                }
                return same_file(src, dst).then_wrapped([&, this] (future<bool> fut) {
                    if (fut.failed()) {
                        auto eptr = fut.get_exception();
                        sstlog.error("Error while linking SSTable: {} to {}: {}", src, dst, eptr);
                        return make_exception_future<>(eptr);
                    }
                    auto same = fut.get();
                    if (!same) {
                        auto msg = format("Error while linking SSTable: {} to {}: File exists", src, dst);
                        sstlog.error("{}", msg);
                        return make_exception_future<>(malformed_sstable_exception(msg, _dir.native()));
                    }
                    return make_ready_future<>();
                });
            });
        });
    });
}

/// create_links_common links all component files from the sstable directory to
/// the given destination directory, using the provided generation.
///
/// It first checks if this is a replay of a previous
/// create_links call, by testing if the destination names already
/// exist, and if so, if they point to the same inodes as the
/// source names.  Otherwise, we return an error.
/// This is an indication that something went wrong.
///
/// Creating the links is done by:
/// First, linking the source TOC component to the destination TemporaryTOC,
/// to mark the destination for rollback, in case we crash mid-way.
/// Then, all components are linked.
///
/// Note that if scylla crashes at this point, the destination SSTable
/// will have both a TemporaryTOC file and a regular TOC file.
/// It should be deleted on restart, thus rolling the operation backwards.
///
/// Eventually, if \c mark_for_removal is unset, the destination
/// TemporaryTOC is removed, to "commit" the destination sstable;
///
/// Otherwise, if \c mark_for_removal is set, the TemporaryTOC at the destination
/// is moved to the source directory to mark the source sstable for removal,
/// thus atomically toggling crash recovery from roll-back to roll-forward.
///
/// Similar to the scenario described above, crashing at this point
/// would leave the source sstable marked for removal, possibly
/// having both a TemporaryTOC file and a regular TOC file, and
/// then the source sstable should be deleted on restart, rolling the
/// operation forward.
///
/// Note that idempotent versions of link_file and rename_file
/// are used.  These versions handle EEXIST errors that may happen
/// when the respective operations are replayed.
///
/// \param sst - the sstable to work on
/// \param dst_dir - the destination directory.
/// \param generation - the generation of the destination sstable
/// \param mark_for_removal - mark the sstable for removal after linking it to the destination dst_dir
future<> filesystem_storage::create_links_common(const sstable& sst, sstring dst_dir, generation_type generation, mark_for_removal mark_for_removal) const {
    sstlog.trace("create_links: {} -> {} generation={} mark_for_removal={}", sst.get_filename(), dst_dir, generation, mark_for_removal);
    auto comps = sst.all_components();
    co_await check_create_links_replay(sst, dst_dir, generation, comps);
    // TemporaryTOC is always first, TOC is always last
    auto dst = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, generation, sst._format, component_type::TemporaryTOC);
    co_await sst.sstable_write_io_check(idempotent_link_file, sst.filename(component_type::TOC), std::move(dst));
    auto dir = opened_directory(dst_dir);
    co_await dir.sync(sst._write_error_handler);
    co_await parallel_for_each(comps, [this, &sst, &dst_dir, generation] (auto p) {
        auto src = sstable::filename(_dir.native(), sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, p.second);
        auto dst = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, generation, sst._format, p.second);
        return sst.sstable_write_io_check(idempotent_link_file, std::move(src), std::move(dst));
    });
    co_await dir.sync(sst._write_error_handler);
    auto dst_temp_toc = sstable::filename(dst_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, generation, sst._format, component_type::TemporaryTOC);
    if (mark_for_removal) {
        // Now that the source sstable is linked to new_dir, mark the source links for
        // deletion by leaving a TemporaryTOC file in the source directory.
        auto src_temp_toc = sstable::filename(_dir.native(), sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, component_type::TemporaryTOC);
        co_await sst.sstable_write_io_check(rename_file, std::move(dst_temp_toc), std::move(src_temp_toc));
        co_await _dir.sync(sst._write_error_handler);
    } else {
        // Now that the source sstable is linked to dir, remove
        // the TemporaryTOC file at the destination.
        co_await sst.sstable_write_io_check(remove_file, std::move(dst_temp_toc));
    }
    co_await dir.sync(sst._write_error_handler);
    co_await dir.close();
    sstlog.trace("create_links: {} -> {} generation={}: done", sst.get_filename(), dst_dir, generation);
}

future<> filesystem_storage::create_links_common(const sstable& sst, const std::filesystem::path& dir, std::optional<generation_type> gen) const {
    return create_links_common(sst, dir.native(), gen.value_or(sst._generation), mark_for_removal::no);
}

future<> filesystem_storage::create_links(const sstable& sst, const std::filesystem::path& dir) const {
    return create_links_common(sst, dir.native(), sst._generation, mark_for_removal::no);
}

future<> filesystem_storage::snapshot(const sstable& sst, sstring dir, absolute_path abs, std::optional<generation_type> gen) const {
    std::filesystem::path snapshot_dir;
    if (abs) {
        snapshot_dir = dir;
    } else {
        snapshot_dir = _dir.path() / dir;
    }
    co_await sst.sstable_touch_directory_io_check(snapshot_dir);
    co_await create_links_common(sst, snapshot_dir, std::move(gen));
}

future<> filesystem_storage::move(const sstable& sst, sstring new_dir, generation_type new_generation, delayed_commit_changes* delay_commit) {
    co_await touch_directory(new_dir);
    sstring old_dir = _dir.native();
    sstlog.debug("Moving {} old_generation={} to {} new_generation={} do_sync_dirs={}",
            sst.get_filename(), sst._generation, new_dir, new_generation, delay_commit == nullptr);
    co_await create_links_common(sst, new_dir, new_generation, mark_for_removal::yes);
    co_await change_dir(new_dir);
    generation_type old_generation = sst._generation;
    co_await coroutine::parallel_for_each(sst.all_components(), [&sst, old_generation, old_dir] (auto p) {
        return sst.sstable_write_io_check(remove_file, sstable::filename(old_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, old_generation, sst._format, p.second));
    });
    auto temp_toc = sstable_version_constants::get_component_map(sst._version).at(component_type::TemporaryTOC);
    co_await sst.sstable_write_io_check(remove_file, sstable::filename(old_dir, sst._schema->ks_name(), sst._schema->cf_name(), sst._version, old_generation, sst._format, temp_toc));
    if (delay_commit == nullptr) {
        co_await when_all(sst.sstable_write_io_check(sync_directory, old_dir), _dir.sync(sst._write_error_handler)).discard_result();
    } else {
        delay_commit->_dirs.insert(old_dir);
        delay_commit->_dirs.insert(new_dir);
    }
}

future<> filesystem_storage::change_state(const sstable& sst, sstable_state state, generation_type new_generation, delayed_commit_changes* delay_commit) {
    auto to = state_to_dir(state);
    auto path = _dir.path();
    auto current = path.filename().native();

    // Moving between states means moving between basedir/state subdirectories.
    // However, normal state maps to the basedir itself and thus there's no way
    // to check if current is normal_dir. The best that can be done here is to
    // check that it's not anything else
    if (current == staging_dir || current == upload_dir || current == quarantine_dir) {
        if (to == quarantine_dir && current != staging_dir) {
            // Legacy exception -- quarantine from anything but staging
            // moves to the current directory quarantine subdir
            path = path / to;
        } else {
            path = path.parent_path() / to;
        }
    } else {
        current = normal_dir;
        path = path / to;
    }

    if (current == to) {
        co_return; // Already there
    }

    sstlog.info("Moving sstable {} to {}", sst.get_filename(), path);
    co_await move(sst, path.native(), std::move(new_generation), delay_commit);
}

static inline fs::path parent_path(const sstring& fname) {
    return fs::canonical(fs::path(fname)).parent_path();
}

future<> filesystem_storage::wipe(const sstable& sst, sync_dir sync) noexcept {
    // We must be able to generate toc_filename()
    // in order to delete the sstable.
    // Running out of memory here will terminate.
    auto name = [&sst] () noexcept {
        memory::scoped_critical_alloc_section _;
        return sst.toc_filename();
    }();

    try {
        auto new_toc_name = co_await make_toc_temporary(name, sync);
        if (!new_toc_name.empty()) {
            auto dir_name = parent_path(new_toc_name);

            co_await coroutine::parallel_for_each(sst.all_components(), [&sst, &dir_name] (auto component) -> future<> {
                if (component.first == component_type::TOC) {
                    // already renamed
                    co_return;
                }

                auto fname = sstable::filename(dir_name.native(), sst._schema->ks_name(), sst._schema->cf_name(), sst._version, sst._generation, sst._format, component.second);
                try {
                    co_await sst.sstable_write_io_check(remove_file, fname);
                } catch (...) {
                    if (!is_system_error_errno(ENOENT)) {
                        throw;
                    }
                    sstlog.debug("Forgiving ENOENT when deleting file {}", fname);
                }
            });
            if (sync) {
                co_await sst.sstable_write_io_check(sync_directory, dir_name.native());
            }
            co_await sst.sstable_write_io_check(remove_file, new_toc_name);
        }
    } catch (...) {
        // Log and ignore the failure since there is nothing much we can do about it at this point.
        // a. Compaction will retry deleting the sstable in the next pass, and
        // b. in the future sstables_manager is planned to handle sstables deletion.
        // c. Eventually we may want to record these failures in a system table
        //    and notify the administrator about that for manual handling (rather than aborting).
        sstlog.warn("Failed to delete {}: {}. Ignoring.", name, std::current_exception());
    }

    if (_temp_dir) {
        try {
            co_await recursive_remove_directory(*_temp_dir);
            _temp_dir.reset();
        } catch (...) {
            sstlog.warn("Exception when deleting temporary sstable directory {}: {}", *_temp_dir, std::current_exception());
        }
    }
}

future<atomic_delete_context> filesystem_storage::atomic_delete_prepare(const std::vector<shared_sstable>& ssts) const {
    return sstable_directory::create_pending_deletion_log(ssts);
}

future<> filesystem_storage::atomic_delete_complete(atomic_delete_context ctx) const {
    co_await coroutine::parallel_for_each(ctx, [] (const auto& x) -> future<> {
        const auto& dir = x.first;
        const auto& log = x.second;

        co_await sync_directory(dir);

        // Once all sstables are deleted, the log file can be removed.
        // Note: the log file will be removed also if unlink failed to remove
        // any sstable and ignored the error.
        try {
            co_await remove_file(log);
            sstlog.debug("{} removed.", log);
        } catch (...) {
            sstlog.warn("Error removing {}: {}. Ignoring.", log, std::current_exception());
        }
    });
}

future<> filesystem_storage::remove_by_registry_entry(entry_descriptor desc) {
    on_internal_error(sstlog, "Filesystem storage doesn't keep its entries in registry");
}

class s3_storage : public sstables::storage {
    shared_ptr<s3::client> _client;
    sstring _bucket;
    sstring _location;

    static constexpr auto status_creating = "creating";
    static constexpr auto status_sealed = "sealed";
    static constexpr auto status_removing = "removing";

    sstring make_s3_object_name(const sstable& sst, component_type type) const;

public:
    s3_storage(shared_ptr<s3::client> client, sstring bucket, sstring dir)
        : _client(std::move(client))
        , _bucket(std::move(bucket))
        , _location(std::move(dir))
    {
    }

    virtual future<> seal(const sstable& sst) override;
    virtual future<> snapshot(const sstable& sst, sstring dir, absolute_path abs, std::optional<generation_type>) const override;
    virtual future<> change_state(const sstable& sst, sstable_state state, generation_type generation, delayed_commit_changes* delay) override;
    // runs in async context
    virtual void open(sstable& sst) override;
    virtual future<> wipe(const sstable& sst, sync_dir) noexcept override;
    virtual future<file> open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) override;
    virtual future<data_sink> make_data_or_index_sink(sstable& sst, component_type type) override;
    virtual future<data_sink> make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) override;
    virtual future<> destroy(const sstable& sst) override {
        return make_ready_future<>();
    }
    virtual future<atomic_delete_context> atomic_delete_prepare(const std::vector<shared_sstable>&) const override;
    virtual future<> atomic_delete_complete(atomic_delete_context ctx) const override;
    virtual future<> remove_by_registry_entry(entry_descriptor desc) override;
    virtual future<uint64_t> free_space() const override {
        // assumes infinite space on s3 (https://aws.amazon.com/s3/faqs/#How_much_data_can_I_store).
        return make_ready_future<uint64_t>(std::numeric_limits<uint64_t>::max());
    }

    virtual sstring prefix() const override { return _location; }
};

sstring s3_storage::make_s3_object_name(const sstable& sst, component_type type) const {
    if (!sst.generation().is_uuid_based()) {
        throw std::runtime_error("'S3' STORAGE only works with uuid_sstable_identifier enabled");
    }
    if (sst.is_uploaded()) {
        return format("/{}/{}", _bucket, sst.filename(type));
    }
    return format("/{}/{}/{}", _bucket, sst.generation(), sstable_version_constants::get_component_map(sst.get_version()).at(type));
}

void s3_storage::open(sstable& sst) {
    entry_descriptor desc(sst._generation, sst._version, sst._format, component_type::TOC);
    sst.manager().sstables_registry().create_entry(_location, status_creating, sst._state, std::move(desc)).get();

    memory_data_sink_buffers bufs;
    sst.write_toc(
        file_writer(
            output_stream<char>(
                data_sink(
                    std::make_unique<memory_data_sink>(bufs)
                )
            )
        )
    );
    _client->put_object(make_s3_object_name(sst, component_type::TOC), std::move(bufs)).get();
}

future<file> s3_storage::open_component(const sstable& sst, component_type type, open_flags flags, file_open_options options, bool check_integrity) {
    co_return _client->make_readable_file(make_s3_object_name(sst, type));
}

future<data_sink> s3_storage::make_data_or_index_sink(sstable& sst, component_type type) {
    SCYLLA_ASSERT(type == component_type::Data || type == component_type::Index);
    // FIXME: if we have file size upper bound upfront, it's better to use make_upload_sink() instead
    co_return _client->make_upload_jumbo_sink(make_s3_object_name(sst, type));
}

future<data_sink> s3_storage::make_component_sink(sstable& sst, component_type type, open_flags oflags, file_output_stream_options options) {
    co_return _client->make_upload_sink(make_s3_object_name(sst, type));
}

future<> s3_storage::seal(const sstable& sst) {
    co_await sst.manager().sstables_registry().update_entry_status(_location, sst.generation(), status_sealed);
}

future<> s3_storage::change_state(const sstable& sst, sstable_state state, generation_type generation, delayed_commit_changes* delay) {
    if (generation != sst._generation) {
        // The 'generation' field is clustering key in system.sstables and cannot be
        // changed. However, that's fine, state AND generation change means the sstable
        // is moved from upload directory and this is another issue for S3 (#13018)
        co_await coroutine::return_exception(std::runtime_error("Cannot change state and generation of an S3 object"));
    }
    co_await sst.manager().sstables_registry().update_entry_state(_location, sst.generation(), state);
}

future<> s3_storage::wipe(const sstable& sst, sync_dir) noexcept {
    auto& sstables_registry = sst.manager().sstables_registry();

    co_await sstables_registry.update_entry_status(_location, sst.generation(), status_removing);

    co_await coroutine::parallel_for_each(sst._recognized_components, [this, &sst] (auto type) -> future<> {
        co_await _client->delete_object(make_s3_object_name(sst, type));
    });

    co_await sstables_registry.delete_entry(_location, sst.generation());
}

future<atomic_delete_context> s3_storage::atomic_delete_prepare(const std::vector<shared_sstable>&) const {
    // FIXME -- need atomicity, see #13567
    co_return atomic_delete_context{};
}

future<> s3_storage::atomic_delete_complete(atomic_delete_context ctx) const {
    co_return;
}

future<> s3_storage::remove_by_registry_entry(entry_descriptor desc) {
    auto prefix = format("/{}/{}", _bucket, desc.generation);
    std::vector<sstring> components;

    try {
        auto f = _client->make_readable_file(prefix + "/" + sstable_version_constants::get_component_map(desc.version).at(component_type::TOC));
        components = co_await with_closeable(std::move(f), [] (file& f) {
            return sstable::read_and_parse_toc(f);
        });
    } catch (const storage_io_error& e) {
        if (e.code().value() != ENOENT) {
            throw;
        }
    }

    co_await coroutine::parallel_for_each(components, [this, &prefix] (sstring comp) -> future<> {
        if (comp != sstable_version_constants::TOC_SUFFIX) {
            co_await _client->delete_object(prefix + "/" + comp);
        }
    });
    co_await _client->delete_object(prefix + "/" + sstable_version_constants::TOC_SUFFIX);
}

future<> s3_storage::snapshot(const sstable& sst, sstring dir, absolute_path abs, std::optional<generation_type> gen) const {
    co_await coroutine::return_exception(std::runtime_error("Snapshotting S3 objects not implemented"));
}

std::unique_ptr<sstables::storage> make_storage(sstables_manager& manager, const data_dictionary::storage_options& s_opts, sstring dir, sstable_state state) {
    return std::visit(overloaded_functor {
        [dir, state] (const data_dictionary::storage_options::local& loc) mutable -> std::unique_ptr<sstables::storage> {
            return std::make_unique<sstables::filesystem_storage>(std::move(dir), state);
        },
        [dir, &manager] (const data_dictionary::storage_options::s3& os) mutable -> std::unique_ptr<sstables::storage> {
            return std::make_unique<sstables::s3_storage>(manager.get_endpoint_client(os.endpoint), os.bucket, std::move(dir));
        }
    }, s_opts.value);
}

future<> init_table_storage(const data_dictionary::storage_options& so, sstring dir) {
    co_await std::visit(overloaded_functor {
        [&dir] (const data_dictionary::storage_options::local&) -> future<> {
            co_await io_check([&dir] { return recursive_touch_directory(dir); });
            co_await io_check([&dir] { return touch_directory(dir + "/upload"); });
            co_await io_check([&dir] { return touch_directory(dir + "/staging"); });
        },
        [] (const data_dictionary::storage_options::s3&) -> future<> {
            co_return;
        }
    }, so.value);
}

future<> init_keyspace_storage(const sstables_manager& mgr, const data_dictionary::storage_options& so, sstring ks_name) {
    co_await std::visit(overloaded_functor {
        [&mgr, &ks_name] (const data_dictionary::storage_options::local&) -> future<> {
            const auto& data_dirs = mgr.config().data_file_directories();
            if (data_dirs.size() > 0) {
                auto dir = format("{}/{}", data_dirs[0], ks_name);
                co_await io_check([&dir] { return touch_directory(dir); });
            }
        },
        [] (const data_dictionary::storage_options::s3&) -> future<> {
            co_return;
        }
    }, so.value);
}

future<> destroy_table_storage(const data_dictionary::storage_options& so, sstring dir) {
    co_await std::visit(overloaded_functor {
        [&dir] (const data_dictionary::storage_options::local&) -> future<> {
            co_await sstables::remove_table_directory_if_has_no_snapshots(fs::path(dir));
        },
        [] (const data_dictionary::storage_options::s3&) -> future<> {
            co_return;
        }
    }, so.value);
}

} // namespace sstables
