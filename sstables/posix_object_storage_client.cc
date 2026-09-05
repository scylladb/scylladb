/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <filesystem>
#include <optional>
#include <span>
#include <system_error>
#include <vector>

#include <seastar/core/abort_source.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>
#include <fmt/core.h>

#include "posix_object_storage_client.hh"
#include "object_storage_client.hh"

#include "db/object_storage_endpoint_param.hh"
#include "utils/log.hh"
#include "utils/lister.hh"
#include "utils/memory_data_sink.hh"
#include "utils/upload_progress.hh"

using namespace seastar;

namespace sstables {

namespace fs = std::filesystem;

static logging::logger pxlog("posix_object_storage_client");

static constexpr unsigned upload_buffer_size = 128 * 1024;

static bool is_errno(const std::system_error& e, int err) {
    return e.code() == std::error_code(err, std::system_category());
}

// Maps an object living under <base>/<bucket> to its filesystem path, rejecting
// names that would escape the bucket directory (e.g. via "..").
static fs::path resolve_under_bucket(const fs::path& base, std::string_view bucket, std::string_view sub) {
    auto bucket_root = (base / fs::path(std::string(bucket))).lexically_normal();
    auto full = (bucket_root / fs::path(std::string(sub))).lexically_normal();
    auto rel = full.lexically_relative(bucket_root);
    if (rel.empty() || *rel.begin() == "..") {
        throw std::invalid_argument(fmt::format(
            "posix object storage: '{}' escapes bucket '{}'", sub, bucket));
    }
    return full;
}

static fs::path resolve(const fs::path& base, const object_name& name) {
    return resolve_under_bucket(base, name.bucket(), name.object());
}

static future<> remove_file_if_exists(const fs::path& path) {
    try {
        co_await remove_file(path.native());
    } catch (const std::system_error& e) {
        // Emulate object-storage semantics: deleting a missing object is a no-op.
        if (!is_errno(e, ENOENT)) {
            throw;
        }
    }
}

// Creates every directory from `base` (assumed to exist) down to the parent of
// `object_path`, tolerating concurrent creation (EEXIST), so that a PUT of a
// nested object works like it would in an object store.
static future<> make_object_dirs(const fs::path& base, const fs::path& object_path) {
    auto rel = object_path.parent_path().lexically_relative(base);
    auto dir = base;
    for (auto& comp : rel) {
        dir /= comp;
        co_await touch_directory(dir.native()); // tolerates EEXIST
    }
}

// A read-only file that opens the backing file lazily on first use and caches
// the descriptor. make_readable_file() must hand back a file synchronously,
// while opening it is asynchronous.
class posix_deferred_readable_file_impl final : public file_impl {
    fs::path _path;
    std::optional<shared_future<file>> _open;

    future<file> ensure_open() {
        if (!_open) {
            _open.emplace(open_file_dma(_path.native(), open_flags::ro));
        }
        return _open->get_future();
    }

    [[noreturn]] static void unsupported() {
        throw std::logic_error("unsupported write operation on posix object storage read-only file");
    }

public:
    explicit posix_deferred_readable_file_impl(fs::path path)
        : _path(std::move(path))
    {}

    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        auto f = co_await ensure_open();
        co_return co_await get_file_impl(f)->read_dma(pos, buffer, len, intent);
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        auto f = co_await ensure_open();
        co_return co_await get_file_impl(f)->read_dma(pos, std::move(iov), intent);
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        auto f = co_await ensure_open();
        co_return co_await get_file_impl(f)->dma_read_bulk(offset, range_size, intent);
    }
    future<struct stat> stat() override {
        auto f = co_await ensure_open();
        co_return co_await get_file_impl(f)->stat();
    }
    future<uint64_t> size() override {
        auto f = co_await ensure_open();
        co_return co_await get_file_impl(f)->size();
    }
    future<> close() override {
        if (!_open) {
            co_return;
        }
        file f;
        try {
            f = co_await ensure_open();
        } catch (...) {
            // Open never succeeded, so there is nothing to release; the failure
            // was already observed by whoever tried to read.
            co_return;
        }
        co_await f.close();
    }

    // The path is valid on every shard of a shared filesystem, so a dup'd
    // handle can just reopen it lazily wherever it lands.
    std::unique_ptr<file_handle_impl> dup() override {
        class handle final : public file_handle_impl {
            fs::path _path;
        public:
            explicit handle(fs::path path) : _path(std::move(path)) {}
            std::unique_ptr<file_handle_impl> clone() const override {
                return std::make_unique<handle>(_path);
            }
            shared_ptr<file_impl> to_file() && override {
                return make_shared<posix_deferred_readable_file_impl>(std::move(_path));
            }
        };
        return std::make_unique<handle>(_path);
    }

    future<size_t> write_dma(uint64_t, const void*, size_t, io_intent*) override { unsupported(); }
    future<size_t> write_dma(uint64_t, std::vector<iovec>, io_intent*) override { unsupported(); }
    future<> flush() override { return make_ready_future<>(); }
    future<> truncate(uint64_t) override { unsupported(); }
    // Harmless no-ops on a read-only file, matching the S3 and GCS clients.
    future<> discard(uint64_t, uint64_t) override { return make_ready_future<>(); }
    future<> allocate(uint64_t, uint64_t) override { return make_ready_future<>(); }
    subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override {
        throw std::logic_error("list_directory not supported on posix object storage file");
    }
};

// Closes the backing file on close(): make_file_data_source() doesn't take
// ownership of the file's lifetime.
class closing_file_data_source_impl : public data_source_impl {
    file _file;
    data_source _source;
public:
    closing_file_data_source_impl(file f, data_source source)
        : _file(std::move(f))
        , _source(std::move(source))
    {}
    future<temporary_buffer<char>> get() override {
        return _source.get();
    }
    future<temporary_buffer<char>> skip(uint64_t n) override {
        return _source.skip(n);
    }
    future<> close() override {
        co_await _source.close();
        co_await _file.close();
    }
};

// A write sink that creates <base>/<bucket>/<object> lazily on first use,
// mirroring the deferred readable file above. Unlinks any previous object (so
// hardlinked copies are unaffected) and auto-creates parent directories. An
// output_stream sits on top of the file to buffer arbitrary writes into the
// DMA-aligned blocks that the underlying file expects.
class posix_deferred_upload_sink_impl final : public data_sink_impl {
    fs::path _base;
    fs::path _path;
    std::optional<output_stream<char>> _os;

    future<> ensure_open() {
        if (_os) {
            co_return;
        }
        co_await remove_file_if_exists(_path);
        co_await make_object_dirs(_base, _path);
        auto f = co_await open_file_dma(_path.native(), open_flags::wo | open_flags::create | open_flags::truncate);
        _os.emplace(co_await make_file_output_stream(std::move(f), file_output_stream_options{.buffer_size = upload_buffer_size}));
    }

public:
    posix_deferred_upload_sink_impl(fs::path base, fs::path path)
        : _base(std::move(base))
        , _path(std::move(path))
    {}

    future<> put(std::span<temporary_buffer<char>> data) override {
        // Move the buffers into the coroutine frame before the first suspension
        // point: the span's backing storage may be released once put() returns.
        std::vector<temporary_buffer<char>> bufs(std::make_move_iterator(data.begin()), std::make_move_iterator(data.end()));
        co_await ensure_open();
        for (auto& buf : bufs) {
            co_await _os->write(buf.get(), buf.size());
        }
    }
    future<> flush() override {
        if (_os) {
            co_await _os->flush();
        }
    }
    future<> close() override {
        // An upload sink always yields an object, even if nothing was written.
        co_await ensure_open();
        co_await _os->close();
    }
    size_t buffer_size() const noexcept override {
        return upload_buffer_size;
    }
};

// Recursively walks <base>/<bucket>/<prefix> and yields regular files with
// names relative to the prefix, mirroring the flat object listing of an object
// store. A missing prefix directory lists as empty.
class posix_object_lister : public abstract_lister::impl {
    fs::path _root;
    fs::path _prefix_dir;
    lister::filter_type _filter;

    struct level {
        fs::path rel;
        directory_lister lister;
    };
    std::vector<std::unique_ptr<level>> _stack;
    bool _started = false;

    static directory_lister open_level(const fs::path& dir) {
        return directory_lister(dir,
            lister::dir_entry_types::full(),
            [](const fs::path&, const directory_entry&) { return true; },
            lister::show_hidden::yes);
    }

    future<std::optional<directory_entry>> next_from_top() {
        try {
            co_return co_await _stack.back()->lister.get();
        } catch (const std::system_error& e) {
            // The directory vanished (or the prefix never existed): treat it as
            // the end of this level, like an empty object listing.
            if (is_errno(e, ENOENT)) {
                co_return std::nullopt;
            }
            throw;
        }
    }

public:
    posix_object_lister(fs::path root, fs::path prefix_dir, lister::filter_type filter)
        : _root(std::move(root))
        , _prefix_dir(std::move(prefix_dir))
        , _filter(std::move(filter))
    {}

    future<std::optional<directory_entry>> get() override {
        if (!_started) {
            _started = true;
            _stack.push_back(std::make_unique<level>(fs::path{}, open_level(_root)));
        }
        while (!_stack.empty()) {
            auto de = co_await next_from_top();
            if (!de) {
                co_await _stack.back()->lister.close();
                _stack.pop_back();
                continue;
            }
            auto rel = _stack.back()->rel / de->name;
            if (de->type == directory_entry_type::directory) {
                _stack.push_back(std::make_unique<level>(rel, open_level(_root / rel)));
                continue;
            }
            if (de->type != directory_entry_type::regular) {
                continue;
            }
            directory_entry ent{rel.native(), de->type};
            if (_filter && !_filter(_prefix_dir, ent)) {
                continue;
            }
            co_return ent;
        }
        co_return std::nullopt;
    }

    future<> close() noexcept override {
        while (!_stack.empty()) {
            co_await _stack.back()->lister.close();
            _stack.pop_back();
        }
    }
};

// A thin filesystem backend for object_storage_client. Objects map to regular
// files at <path>/<bucket>/<object>; the shard_client_factory is ignored since
// there is no shared remote state, only local file descriptors.
class posix_client_wrapper : public sstables::object_storage_client {
    fs::path _path;
    seastar::gate _gate;

public:
    explicit posix_client_wrapper(const db::object_storage_endpoint_param& ep)
        : _path(ep.get_posix_storage().path)
    {}

    future<> put_object(object_name name, ::memory_data_sink_buffers bufs, abort_source* as) override {
        auto h = _gate.hold();
        auto base = _path;
        auto path = resolve(base, name);
        co_await remove_file_if_exists(path);
        co_await make_object_dirs(base, path);
        auto f = co_await open_file_dma(path.native(), open_flags::wo | open_flags::create | open_flags::truncate);
        auto os = co_await make_file_output_stream(std::move(f), file_output_stream_options{.buffer_size = upload_buffer_size});
        std::exception_ptr ex;
        try {
            for (auto&& buf : bufs.buffers()) {
                if (as) {
                    as->check();
                }
                co_await os.write(buf.get(), buf.size());
            }
        } catch (...) {
            ex = std::current_exception();
        }
        // close() flushes the buffered data and always releases the file, so
        // it is called exactly once; keep the earlier write error if any.
        try {
            co_await os.close();
        } catch (...) {
            if (!ex) {
                ex = std::current_exception();
            }
        }
        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    future<> copy_object(object_name src, object_name dst, abort_source* as) override {
        auto h = _gate.hold();
        auto base = _path;
        auto src_path = resolve(base, src);
        auto dst_path = resolve(base, dst);
        co_await remove_file_if_exists(dst_path);
        co_await make_object_dirs(base, dst_path);
        co_await link_file(src_path.native(), dst_path.native());
    }

    future<> delete_object(object_name name, abort_source* as) override {
        auto h = _gate.hold();
        auto base = _path;
        auto path = resolve(base, name);
        co_await remove_file_if_exists(path);

        // Prune now-empty parent directories up to (but not including) the
        // bucket root, so that deleting objects doesn't leave stray dirs.
        auto bucket_root = (base / fs::path(std::string(name.bucket()))).lexically_normal();
        for (auto dir = path.parent_path(); dir != bucket_root; dir = dir.parent_path()) {
            if (as) {
                as->check();
            }
            try {
                co_await remove_file(dir.native()); // rmdir on a directory
            } catch (const std::system_error& e) {
                // A concurrent PUT created a new object or subdirectory here, so
                // the directory is no longer empty: stop pruning gracefully.
                if (is_errno(e, EEXIST) || is_errno(e, ENOTEMPTY) || is_errno(e, ENOENT)) {
                    break;
                }
                throw;
            }
        }
    }

    file make_readable_file(object_name name, abort_source* as) override {
        return file(make_shared<posix_deferred_readable_file_impl>(resolve(_path, name)));
    }

    data_sink make_data_upload_sink(object_name name, std::optional<unsigned>, abort_source* as) override {
        return make_upload_sink(std::move(name), as);
    }

    data_sink make_upload_sink(object_name name, abort_source* as) override {
        return data_sink(std::make_unique<posix_deferred_upload_sink_impl>(_path, resolve(_path, name)));
    }

    data_source make_download_source(object_name name, abort_source* as) override {
        auto f = make_readable_file(std::move(name), as);
        auto src = make_file_data_source(f, file_input_stream_options{});
        return data_source(std::make_unique<closing_file_data_source_impl>(std::move(f), std::move(src)));
    }

    future<bool> object_exists(object_name name, abort_source* as) override {
        auto h = _gate.hold();
        auto path = resolve(_path, name);
        try {
            auto st = co_await file_stat(path.native());
            // A directory at this path is a prefix, not an object.
            co_return st.type == directory_entry_type::regular;
        } catch (const std::system_error& e) {
            if (is_errno(e, ENOENT)) {
                co_return false;
            }
            throw;
        }
    }

    abstract_lister make_object_lister(std::string bucket, std::string prefix, lister::filter_type filter) override {
        auto root = resolve_under_bucket(_path, bucket, prefix);
        return abstract_lister::make<posix_object_lister>(std::move(root), fs::path(prefix), std::move(filter));
    }

    future<> upload_file(std::filesystem::path path, object_name name, utils::upload_progress& up, abort_source* as) override {
        auto h = _gate.hold();
        auto src = co_await open_file_dma(path.native(), open_flags::ro);
        auto size = co_await src.size();
        up.total += size;
        auto in = make_file_data_source(std::move(src), file_input_stream_options{});
        auto out = make_upload_sink(std::move(name), as);
        std::exception_ptr ex;
        bool wrote_any = false;
        try {
            while (true) {
                if (as) {
                    as->check();
                }
                auto buf = co_await in.get();
                if (buf.empty()) {
                    break;
                }
                wrote_any = true;
                up.uploaded += buf.size();
                co_await out.put(std::move(buf));
            }
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        if (!ex || wrote_any) {
            co_await out.close();
        }
        co_await in.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
    }

    void update_config_sync(const db::object_storage_endpoint_param& ep) override {
        if (_gate.is_closed()) {
            pxlog.info("config update gate is closed");
            return;
        }
        auto h = _gate.hold();
        _path = ep.get_posix_storage().path;
        pxlog.info("Updated posix object storage base path to {}", _path.native());
    }

    void update_connections_per_shard(unsigned) override {
        // A local filesystem has no per-scheduling-group connection budget.
    }

    future<> close() override {
        co_await _gate.close();
    }
};

shared_ptr<object_storage_client> make_posix_object_storage_client(const db::object_storage_endpoint_param& ep) {
    const auto& path = ep.get_posix_storage().path;
    // Validate the endpoint's mount up-front: the configured path must name an
    // existing directory. This check is intentionally kept out of endpoint
    // decoding (config parsing) so that the filesystem - potentially a slow,
    // network-attached mount - is only touched when the endpoint is actually
    // used to build a client, not for every endpoint at config-load time.
    std::error_code ec;
    if (!fs::is_directory(path, ec)) {
        if (ec) {
            throw std::invalid_argument(fmt::format(
                "posix object storage endpoint path '{}' cannot be used as a directory: {}", path.native(), ec.message()));
        }
        throw std::invalid_argument(fmt::format(
            "posix object storage endpoint path '{}' does not exist or is not a directory", path.native()));
    }
    return seastar::make_shared<posix_client_wrapper>(ep);
}

}
