#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/log.hh>
#include "lister.hh"
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
