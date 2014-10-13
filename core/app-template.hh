/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
#ifndef _APP_TEMPLATE_HH
#define _APP_TEMPLATE_HH

#include <boost/program_options.hpp>
#include <fstream>
#include <cstdlib>
#include "core/reactor.hh"

namespace bpo = boost::program_options;

class app_template {
private:
    bpo::options_description _opts;
    boost::optional<bpo::variables_map> _configuration;
public:
    app_template() {
        _opts.add_options()
                ("help", "show help message")
                ;
        _opts.add(reactor::get_options_description());
        _opts.add(smp::get_options_description());
    };

    boost::program_options::options_description_easy_init add_options() {
        return _opts.add_options();
    }

    bpo::variables_map& configuration() { return *_configuration; }

    template<typename Func>
    int run(int ac, char ** av, Func&& func) {
        bpo::variables_map configuration;
        bpo::store(bpo::command_line_parser(ac, av).options(_opts).run(), configuration);
        auto home = std::getenv("HOME");
        if (home) {
            std::ifstream ifs(std::string(home) + "/.config/seastar/seastar.conf");
            if (ifs) {
                bpo::store(bpo::parse_config_file(ifs, _opts), configuration);
            }
        }
        bpo::notify(configuration);
        if (configuration.count("help")) {
            std::cout << _opts << "\n";
            return 1;
        }
        smp::configure(configuration);
        _configuration = {std::move(configuration)};
        engine.when_started().then(func).rescue([] (auto get_ex) {
            try {
                get_ex();
            } catch (std::exception& ex) {
                std::cout << "program failed with uncaught exception: " << ex.what() << "\n";
            engine.exit(1);
            }
            });
        return engine.run();
    };
};

#endif
