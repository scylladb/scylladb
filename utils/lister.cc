#include <seastar/core/coroutine.hh>
#include <seastar/core/format.hh>
#include <seastar/core/pipe.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include "utils/assert.hh"
#include "utils/lister.hh"
#include "utils/checked-file-impl.hh"
#include "utils/error_injection.hh"

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
    return refresh_type(std::move(de)).then([this] (directory_entry de) {
        // The entry was removed between readdir and stat, just skip it
        if (!de.type.has_value()) {
            return make_ready_future<>();
        }
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

future<directory_entry> lister::refresh_type(directory_entry de) {
    if (utils::get_local_injector().enter("lister_refresh_type")) {
        de.type.reset();
        if (de.name == *utils::get_local_injector().inject_parameter<std::string_view>("lister_refresh_type", "unlink_file")) {
            fs::remove((_dir / de.name).native());
        }
    }

    if (de.type) {
        return make_ready_future<directory_entry>(std::move(de));
    } else {
        auto f = file_type((_dir / de.name.c_str()).native(), follow_symlink::no);
        return f.then([de = std::move(de)] (std::optional<directory_entry_type> t) mutable {
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
    if (_gen) {
        on_internal_error(llogger, "directory_lister not closed when destroyed");
    }
}

future<std::optional<directory_entry>> directory_lister::get() {
    if (!_opened) {
        _opened = co_await open_checked_directory(general_disk_error_handler, _dir.native());
        _gen.emplace(_opened.experimental_list_directory());
    }
    if (!_gen) {
        co_return coroutine::exception(std::make_exception_ptr(seastar::broken_pipe_exception()));
    }
    std::exception_ptr ex;
    try {
        while (auto de_opt = co_await (*_gen)()) {
            auto& de = de_opt->get();
            if (!de.type) {
                de.type = co_await file_type((_dir / de.name).native(), follow_symlink::no);
                if (!de.type) {
                    continue;
                }
            }
            if (_type && !_type.contains(de.type.value())) {
                continue;
            }
            if (!_do_show_hidden && de.name[0] == '.') {
                continue;
            }
            if (!_filter(_dir, de)) {
                continue;
            }

            co_return de;
        }
    } catch (...) {
        ex = std::current_exception();
    }
    _gen.reset();
    if (ex) {
        co_await _opened.close();
        co_return coroutine::exception(std::move(ex));
    }
    co_return std::nullopt;
}

future<> directory_lister::close() noexcept {
    _gen.reset();
    if (_opened) {
        co_await _opened.close();
    }
}
