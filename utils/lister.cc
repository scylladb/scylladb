#include <dirent.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>
#include "utils/lister.hh"
#include "checked-file-impl.hh"

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
        if ((!_expected_type.empty() && !_expected_type.contains(*(de.type))) || (!_show_hidden && de.name[0] == '.')) {
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
            // If some FS error occures - return an exceptional future
            if (!t) {
                return make_exception_future<directory_entry>(std::runtime_error(format("Failed to get {} type.", (dir / de.name.c_str()).native())));
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

static std::vector<directory_entry> list_directory(int fd) {
    std::vector<directory_entry> ret;
    static constexpr size_t buffer_size = 8192;
    char buffer[buffer_size];

    // From getdents(2):
    struct linux_dirent64 {
        ino64_t        d_ino;    /* 64-bit inode number */
        off64_t        d_off;    /* 64-bit offset to next structure */
        unsigned short d_reclen; /* Size of this dirent */
        unsigned char  d_type;   /* File type */
        char           d_name[]; /* Filename (null-terminated) */
    };

    long count = ::syscall(__NR_getdents64, fd, reinterpret_cast<linux_dirent64*>(buffer), buffer_size);
    if (count == -1) {
        if (errno == ENOENT) {
            return ret;
        }
        throw std::system_error(std::error_code(errno, std::system_category()));
    }

    for (long next = 0; next < count; ) {
        auto de = reinterpret_cast<linux_dirent64*>(buffer + next);
        next += de->d_reclen;
        std::optional<directory_entry_type> type;
        switch (de->d_type) {
        case DT_BLK:
            type = directory_entry_type::block_device;
            break;
        case DT_CHR:
            type = directory_entry_type::char_device;
            break;
        case DT_DIR:
            type = directory_entry_type::directory;
            break;
        case DT_FIFO:
            type = directory_entry_type::fifo;
            break;
        case DT_REG:
            type = directory_entry_type::regular;
            break;
        case DT_LNK:
            type = directory_entry_type::link;
            break;
        case DT_SOCK:
            type = directory_entry_type::socket;
            break;
        default:
            // unknown, ignore
            ;
        }
        sstring name = de->d_name;
        if (name == "." || name == "..") {
            continue;
        }
        ret.emplace_back(directory_entry{std::move(name), type});
    }

    return ret;
}

future<> lister::rmdir(fs::path dir) {
    // first, kill the contents of the directory
    llogger.debug("rmdir: {}", dir);

    int dirfd = ::open(dir.native().c_str(), O_DIRECTORY | O_CLOEXEC | O_RDONLY);
    if (dirfd == -1) {
        if (errno == ENOENT) {
            co_return;
        }
        co_await coroutine::return_exception(std::system_error(std::error_code(errno, std::system_category()), dir.native()));
    }
    auto close_fd = defer([dirfd] {
        ::close(dirfd);
    });

    while (true) {
        auto ents = list_directory(dirfd);
        if (ents.empty()) {
            break;
        }
        for (auto& de : ents) {
            if (de.type != directory_entry_type::directory) {
                llogger.debug("rmdir: unlinking {} from {}", de.name, dir);
                auto res = ::unlinkat(dirfd, de.name.c_str(), 0);
                if (res == -1) {
                    if (errno != ENOENT) {
                        co_await coroutine::return_exception(std::system_error(std::error_code(errno, std::system_category()), (dir / de.name).native()));
                    }
                }
                co_await coroutine::maybe_yield();
            }
        }
        for (auto& de : ents) {
            if (de.type == directory_entry_type::directory) {
                co_await rmdir(dir / de.name);
            }
        }
    }
    // ...then kill the directory itself
    llogger.debug("rmdir: removing {}", dir);
    auto res = ::rmdir(dir.native().c_str());
    if (res == -1 && errno != ENOENT) {
        co_await coroutine::return_exception(std::system_error(std::error_code(errno, std::system_category()), dir.native()));
    }
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
        assert(!_lister);
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
