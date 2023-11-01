// -*- mode:C++; tab-width:4; c-basic-offset:4; indent-tabs-mode:nil -*-
/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mirrored_storage.hh"

#include <boost/algorithm/string.hpp>

#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/when_all.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/file.hh>

#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_version.hh"
#include "sstables/writer.hh"

#include "db/system_keyspace.hh"

#include "utils/memory_data_sink.hh"
#include "utils/s3/client.hh"

namespace {
    static const auto status_creating = "creating";
    static const auto status_sealed = "sealed";
    static const auto status_removing = "removing";
}

namespace sstables {

mirrored_storage::mirrored_storage(shared_ptr<s3::client> s3_client,
                               sstring bucket,
                               sstring dir)
    : _s3_client{std::move(s3_client)}
    , _bucket{std::move(bucket)}
    , _location{dir}
    , _dir{dir} // TODO: respect the state
{}

mirrored_storage::~mirrored_storage() = default;

// run in async context
void mirrored_storage::open(sstable& sst) {
    _open_local(sst);
    _open_remote(sst);
}

future<> mirrored_storage::_seal_local(const sstable& sst) {
    sstlog.debug("mirrored_storage: seal_local({})", sst.generation());

    assert(!_temp_dir.empty());
    co_await remove_file(_temp_dir.native());
    _temp_dir.clear();

    auto dir_f = co_await open_directory(_dir.native());
    // Guarantee that every component of this sstable reached the disk.
    co_await dir_f.flush();
    // Rename TOC because it's no longer temporary.
    co_await rename_file(sst.filename(component_type::TemporaryTOC), sst.filename(component_type::TOC));
    co_await dir_f.flush();
    co_await dir_f.close();
}

future<> mirrored_storage::_seal_remote(const sstable& sst) {
    sstlog.debug("mirrored_storage: seal_remote({})", sst.generation());
    co_await sst.manager().system_keyspace().sstables_registry_update_entry_status(_location, sst.generation(), status_sealed);
}

future<> mirrored_storage::seal(const sstable& sst) {
    sstlog.debug("mirrored_storage: seal({})", sst.generation());
    co_await when_all_succeed(
        _seal_local(sst),
        _seal_remote(sst)).discard_result();
}


future<> mirrored_storage::snapshot(const sstable& sst, sstring dir,
                                  storage::absolute_path abs) const {
    co_await coroutine::return_exception(std::runtime_error("not implemented"));
}

future<> mirrored_storage::change_state(const sstable& sst,
                                      sstable_state to,
                                      generation_type generation,
                                      delayed_commit_changes* delay) {
    co_await coroutine::return_exception(std::runtime_error("not implemented"));
}

void mirrored_storage::_open_local(sstable& sst) {
    // the caller have to call open() before
    auto temp_dir = _dir / fs::path(fmt::format("{}{}", sst._generation, tempdir_extension));
    touch_directory(temp_dir.native()).get0();
    _temp_dir = temp_dir;

    auto file_path = sst.filename(component_type::TemporaryTOC);

    // Writing TOC content to temporary file.
    // If creation of temporary TOC failed, it implies that that boot failed to
    // delete a sstable with temporary for this column family, or there is a
    // sstable being created in parallel with the same generation.
    file_output_stream_options options {
        .buffer_size = 4096
    };
    auto sink = make_component_sink(sst, component_type::TemporaryTOC,
                                    open_flags::wo |
                                    open_flags::create |
                                    open_flags::exclusive,
                                    options).get0();
    auto w = file_writer(output_stream<char>(std::move(sink)), std::move(file_path));

    bool toc_exists = file_exists(sst.filename(component_type::TOC)).get0();
    if (toc_exists) {
        // TOC will exist at this point if write_components() was called with
        // the generation of a sstable that exists.
        w.close();
        remove_file(file_path).get();
        throw std::runtime_error(format("SSTable write failed due to existence of TOC file for generation {} of {}.{}", sst._generation, sst._schema->ks_name(), sst._schema->cf_name()));
    }

    sst.write_toc(std::move(w));

    // Flushing parent directory to guarantee that temporary TOC file reached
    // the disk.
    sync_directory(_dir.native()).get();
}

void mirrored_storage::_open_remote(sstable& sst) {
    entry_descriptor desc(sst._generation, sst._version, sst._format, component_type::TOC);
    sst.manager().system_keyspace().sstables_registry_create_entry(_location, status_creating, sst._state, std::move(desc)).get();

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

    auto s3_object_name = _make_s3_object_name(sst, component_type::TOC);
    _s3_client->put_object(s3_object_name, std::move(bufs)).get();
}

future<> mirrored_storage::wipe(const sstable& sst, sync_dir sync) noexcept {
    return when_all_succeed(_wipe_local(sst, sync), _wipe_remote(sst))
        .discard_result();
}

future<> mirrored_storage::_wipe_local(const sstable& sst, sync_dir sync) noexcept {
    co_await remove_by_toc_name(sst.toc_filename(), sync);
    if (!_temp_dir.empty()) {
        co_await recursive_remove_directory(_temp_dir);
        _temp_dir.clear();
    }
}

future<> mirrored_storage::_wipe_remote(const sstable& sst) noexcept {
    auto& sys_ks = sst.manager().system_keyspace();

    co_await sys_ks.sstables_registry_update_entry_status(_location, sst.generation(), status_removing);

    co_await coroutine::parallel_for_each(sst._recognized_components, [this, &sst] (auto type) -> future<> {
        co_await _s3_client->delete_object(_make_s3_object_name(sst, type));
    });

    co_await sys_ks.sstables_registry_delete_entry(_location, sst.generation());
}

sstring mirrored_storage::_make_s3_object_name(const sstable& sst, component_type type) const {
    assert(sst.generation().is_uuid_based());
    auto component_ext = sstable_version_constants::get_component_map(sst.get_version()).at(type);
    return format("/{}/{}/{}", _bucket, sst.generation(), component_ext);
}

future<file> mirrored_storage::_open_component_local(const sstable& sst,
                                                   component_type type,
                                                   open_flags flags,
                                                   file_open_options options,
                                                   bool check_integrity) {
    const auto create_flags = open_flags::create | open_flags::exclusive;
    bool read_only = (flags & create_flags) != create_flags;
    fs::path path;
    if (read_only || _temp_dir.empty()) {
        path = _dir;
    } else {
        path = _temp_dir;
    }
    path /= fs::path(sst.component_basename(type));
    try {
        auto f = co_await open_file_dma(path.native(), flags, options);
        if (!read_only) {
            co_await rename_file(path.native(), sst.filename(type));
        }
        co_return f;
    } catch (const std::system_error& e) {
        if (e.code().value() == ENOENT) {
            co_return file{};
        }
        throw;
    }
}


future<file> mirrored_storage::_open_component_remote(const sstable& sst,
                                                    component_type type,
                                                    open_flags flags,
                                                    file_open_options options,
                                                    bool check_integrity) {
    co_return _s3_client->make_readable_file(_make_s3_object_name(sst, type));
}

future<file> mirrored_storage::open_component(const sstable& sst,
                                            component_type type,
                                            open_flags flags,
                                            file_open_options options,
                                            bool check_integrity) {
    co_return co_await _open_component_local(sst, type, flags, options, check_integrity);
}

future<data_sink> mirrored_storage::_make_data_sink_local(sstable& sst,
                                                        component_type type) {
    assert(type == component_type::Data || type == component_type::Index);
    file_output_stream_options options {
      .buffer_size = sst.sstable_buffer_size,
      .write_behind = 10,
    };
    if (type == component_type::Data) {
        return make_file_data_sink(std::move(sst._data_file), options);
    } else {
        return make_file_data_sink(std::move(sst._index_file), options);
    }
}

class write_through_sink : public data_sink_impl {
    data_sink _local_sink;
    data_sink _remote_sink;
public:
    write_through_sink(data_sink&& local_sink, data_sink&& remote_sink)
        : _local_sink{std::move(local_sink)}
        , _remote_sink{std::move(remote_sink)}
    {}
    future<> put(net::packet) override {
        co_await coroutine::return_exception(std::runtime_error("not implemented"));
    }
    future<> put(std::vector<temporary_buffer<char>>) override {
        co_await coroutine::return_exception(std::runtime_error("not implemented"));
    }
    future<> put(temporary_buffer<char> buf) override {
        auto clone_buf = buf.share();
        co_await coroutine::all([&] { return _local_sink.put(std::move(buf)); },
                                [&] { return _remote_sink.put(std::move(clone_buf)); });
    }
    future<> close() override {
        co_await coroutine::all([&] { return _local_sink.close(); },
                                [&] { return _remote_sink.close(); });
    }
    size_t buffer_size() const noexcept override {
        return std::max(_local_sink.buffer_size(),
                        _remote_sink.buffer_size());
    }
};


future<data_sink> mirrored_storage::make_data_or_index_sink(sstable& sst,
                                                          component_type type) {
    auto local_sink = co_await _make_data_sink_local(sst, type);
    auto s3_object_name = _make_s3_object_name(sst, type);
    auto remote_sink = _s3_client->make_upload_jumbo_sink(s3_object_name);
    co_return data_sink{std::make_unique<write_through_sink>(std::move(local_sink),
                                                             std::move(remote_sink))};
}

future<data_sink> mirrored_storage::make_component_sink(sstable& sst,
                                                      component_type type,
                                                      open_flags oflags,
                                                      file_output_stream_options options) {
    auto s3_object_name = _make_s3_object_name(sst, type);
    auto remote_sink = _s3_client->make_upload_sink(s3_object_name);

    auto fd = co_await open_component(sst, type, oflags, {}, false);
    auto local_sink = co_await make_file_data_sink(std::move(fd), options);
    // in general, the sizes of these components are small. so always
    // update the local cache in "write_through" mode
    auto sink_impl = std::make_unique<write_through_sink>(std::move(local_sink),
                                                          std::move(remote_sink));
    co_return data_sink{std::move(sink_impl)};
}

future<> mirrored_storage::destroy(const sstable& sst) {
    return make_ready_future();
}

static future<> delete_sstables(std::vector<shared_sstable> ssts) {
    // TODO: delete the sstables with a transaction semantics
    co_await coroutine::parallel_for_each(ssts, [] (shared_sstable sst) -> future<> {
        return sst->unlink();
    });
}

noncopyable_function<future<>(std::vector<shared_sstable>)> mirrored_storage::atomic_deleter() const {
    return delete_sstables;
}

future<> mirrored_storage::remove_by_registry_entry(entry_descriptor desc) {
    // used when performing garbage_collect as requested by sstable_directory::prepare()
    // system_keyspace_components_lister uses this to remove the components and the
    // systable entries yet marked "sealed".
    auto prefix = format("/{}/{}", _bucket, desc.generation);
    std::optional<temporary_buffer<char>> toc;
    std::vector<sstring> components;

    try {
        toc = co_await _s3_client->get_object_contiguous(prefix + "/" + sstable_version_constants::get_component_map(desc.version).at(component_type::TOC));
    } catch (seastar::httpd::unexpected_status_error& e) {
        if (e.status() != seastar::http::reply::status_type::no_content) {
            throw;
        }
    }
    if (!toc) {
        co_return; // missing TOC object is OK
    }

    boost::split(components, std::string_view(toc->get(), toc->size()), boost::is_any_of("\n"));
    co_await coroutine::parallel_for_each(components, [this, &prefix] (sstring comp) -> future<> {
        if (comp != sstable_version_constants::TOC_SUFFIX) {
            co_await _s3_client->delete_object(prefix + "/" + comp);
        }
    });
    co_await _s3_client->delete_object(prefix + "/" + sstable_version_constants::TOC_SUFFIX);
}

sstring mirrored_storage::prefix() const {
    // despite that we don't necessarily write to local cache, sstable still
    // expects a "prefix" so it can contruct a "filename" for the given component
    // this "filename" is used for logging purpose.
    return _dir.native();
}

}
