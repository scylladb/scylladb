/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <algorithm>
#include <boost/program_options.hpp>
#include "core/reactor.hh"

struct file_test {
    file_test(file&& f) : f(std::move(f)) {}
    file f;
    semaphore sem = { 0 };
};

namespace po = boost::program_options;

int main(int ac, char** av) {
    po::options_description desc("Allowed options");
    desc.add_options()
        ("dev", po::value<std::string>(),
            "e.g. --dev /dev/sdb");
    po::variables_map vm;
    po::store(po::parse_command_line(ac, av, desc), vm);
    po::notify(vm);

    const char *filepath = nullptr;
    if (vm.count("dev")) {
        filepath = vm["dev"].as<std::string>().c_str();
    } else {
        std::cerr << "usage: program --dev /dev/sdx\n";
        ::exit(1);
    }

    static constexpr auto max = 10000;
    engine.open_file_dma(filepath).then([] (file f) {
        auto ft = new file_test{std::move(f)};

        ft->f.stat().then([ft] (struct stat st) mutable {
            assert(S_ISBLK(st.st_mode));
            auto offset = 0;
            auto length = max * 4096;
            ft->f.discard(offset, length).then([ft] () mutable {
                ft->sem.signal();
            });
        });

        ft->sem.wait().then([ft] () mutable {
            return ft->f.flush();
        }).then([ft] () mutable {
            std::cout << "done\n";
            delete ft;
            ::exit(0);
        });
    });
    engine.run();
    return 0;
}
