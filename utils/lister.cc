#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/pipe.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include "utils/assert.hh"
#include "utils/lister.hh"
#include "utils/checked-file-impl.hh"

static seastar::logger llogger("lister");

lister::lister(file f, dir_entry_types type, walker_type walker, filter_type filter, fs::path dir, lister::show_hidden do_show_hidden)
        : _f(std::move(f))
        , _walker(std::move(walker))
        , _filter(std::move(filter))
        , _expected_type(std::move(type))
        , _listing_done(_f.list_directory([this] (directory_entry de) { return visit(de); }).done())
        , _dir(std::move(dir))
        , _show_hidden(do_show_hidden) {}

future<> lister::visit(directory_entry de) {
    return guarantee_type(std::move(de)).then([this] (directory_entry de) {
        // Hide all synthetic directories and hidden files if not requested to show them.
        if ((_expected_type && !_expected_type.contains(*(de.type))) || (!_show_hidden && de.name[0] == '.')) {
            return make_ready_future<>();
        }

        // apply a filter
        if (!_filter(_dir, de)) {
            return make_ready_future<>();
        }

        return _walker(_dir, std::move(de));
    });
}

future<> lister::done() {
    return _listing_done.then([this] {
        return _f.close();
    });
}

future<directory_entry> lister::guarantee_type(directory_entry de) {
    if (de.type) {
        return make_ready_future<directory_entry>(std::move(de));
    } else {
        auto f = file_type((_dir / de.name.c_str()).native(), follow_symlink::no);
        return f.then([dir = _dir, de = std::move(de)] (std::optional<directory_entry_type> t) mutable {
            // If some FS error occurs - return an exceptional future
            if (!t) {
                return make_exception_future<directory_entry>(std::runtime_error(fmt::format("Failed to get {} type.", (dir / de.name.c_str()).native())));
            }
            de.type = t;
            return make_ready_future<directory_entry>(std::move(de));
        });
    }
}

future<> lister::scan_dir(fs::path dir, lister::dir_entry_types type, lister::show_hidden do_show_hidden, walker_type walker, filter_type filter) {
    return open_checked_directory(general_disk_error_handler, dir.native()).then([type = std::move(type), walker = std::move(walker), filter = std::move(filter), dir, do_show_hidden] (file f) {
            auto l = make_lw_shared<lister>(std::move(f), std::move(type), std::move(walker), std::move(filter), std::move(dir), do_show_hidden);
            return l->done().then([l] { });
    });
}

future<> lister::rmdir(fs::path dir) {
    // first, kill the contents of the directory
    return lister::scan_dir(dir, {}, show_hidden::yes, [] (fs::path parent_dir, directory_entry de) mutable {
        fs::path current_entry_path(parent_dir / de.name.c_str());

        if (de.type.value() == directory_entry_type::directory) {
            return rmdir(std::move(current_entry_path));
        } else {
            return remove_file(current_entry_path.native());
        }
    }).then([dir] {
        // ...then kill the directory itself
        return remove_file(dir.native());
    });
}

directory_lister::~directory_lister() {
    if (_lister) {
        on_internal_error(llogger, "directory_lister not closed when destroyed");
    }
}

future<std::optional<directory_entry>> directory_lister::get() {
    if (!_opt_done_fut) {
        auto dir = co_await open_checked_directory(general_disk_error_handler, _dir.native());
        auto walker = [this] (fs::path dir, directory_entry de) {
            return _queue.push_eventually(std::make_optional<directory_entry>(std::move(de)));
        };
        SCYLLA_ASSERT(!_lister);
        _lister = std::make_unique<lister>(std::move(dir), _type, std::move(walker), _filter, _dir, _do_show_hidden);
        _opt_done_fut = _lister->done().then_wrapped([this] (future<> f) {
            if (f.failed()) [[unlikely]] {
                _queue.abort(f.get_exception());
                return make_ready_future<>();
            }
            return _queue.push_eventually(std::nullopt);
        }).finally([this] {
            _lister.reset();
        });
    }
    std::exception_ptr ex;
    try {
        auto ret = co_await _queue.pop_eventually();
        if (ret) {
            co_return ret;
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await close();
    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }
    co_return std::nullopt;
}

future<> directory_lister::close() noexcept {
    if (!_opt_done_fut) {
        return make_ready_future<>();
    }
    // The queue has to be aborted if the user didn't get()
    // all entries.
    _queue.abort(std::make_exception_ptr(broken_pipe_exception()));
    return std::exchange(_opt_done_fut, std::make_optional<future<>>(make_ready_future<>()))->handle_exception([] (std::exception_ptr) {
        // ignore all errors
    });
}
