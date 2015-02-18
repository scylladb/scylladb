/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "app-template.hh"
#include "core/reactor.hh"
#include "core/scollectd.hh"
#include "core/print.hh"
#include "stdio.hh"
#include <boost/program_options.hpp>
#include <boost/make_shared.hpp>
#include <fstream>
#include <cstdlib>

namespace bpo = boost::program_options;

app_template::app_template()
        : _opts("App options") {
    _opts.add_options()
            ("help,h", "show help message")
            ;
    _opts.add(reactor::get_options_description());
    _opts.add(smp::get_options_description());
    _opts.add(scollectd::get_options_description());
}

boost::program_options::options_description_easy_init
app_template::add_options() {
    return _opts.add_options();
}

void
app_template::add_positional_options(std::initializer_list<positional_option> options) {
    for (auto&& o : options) {
        _opts.add(boost::make_shared<bpo::option_description>(o.name, o.value_semantic, o.help));
        _pos_opts.add(o.name, o.max_count);
    }
}


bpo::variables_map&
app_template::configuration() {
    return *_configuration;
}

int
app_template::run(int ac, char ** av, std::function<void ()>&& func) {
#ifdef DEBUG
    print("WARNING: debug mode. Not for benchmarking or production\n");
#endif
    bpo::variables_map configuration;
    try {
        bpo::store(bpo::command_line_parser(ac, av)
                    .options(_opts)
                    .positional(_pos_opts)
                    .run()
            , configuration);
        auto home = std::getenv("HOME");
        if (home) {
            std::ifstream ifs(std::string(home) + "/.config/seastar/seastar.conf");
            if (ifs) {
                bpo::store(bpo::parse_config_file(ifs, _opts), configuration);
            }
        }
    } catch (bpo::error& e) {
        print("error: %s\n\nTry --help.\n", e.what());
        return 2;
    }
    bpo::notify(configuration);
    if (configuration.count("help")) {
        std::cout << _opts << "\n";
        return 1;
    }
    // intentional leak
    stdout = smp_synchronize_lines(stdout);
    smp::configure(configuration);
    _configuration = {std::move(configuration)};
    engine().when_started().then([this] {
        scollectd::configure( this->configuration());
    }).then(
        std::move(func)
    ).rescue([] (auto get_ex) {
        try {
            get_ex();
        } catch (std::exception& ex) {
            std::cout << "program failed with uncaught exception: " << ex.what() << "\n";
            engine().exit(1);
        }
    });
    auto exit_code = engine().run();
    smp::cleanup();
    return exit_code;
}


