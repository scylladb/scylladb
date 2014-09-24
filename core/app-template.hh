/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
#ifndef _APP_TEMPLATE_HH
#define _APP_TEMPLATE_HH

#include <boost/program_options.hpp>
#include "core/reactor.hh"

namespace bpo = boost::program_options;

class app_template {
private:
    bpo::options_description _opts;
public:
    app_template() {
        _opts.add_options()
                ("help", "show help message")
                ;
        _opts.add(reactor::get_options_description());
        _opts.add(smp::get_options_description());
    };

    template<typename Func>
    int run(int ac, char ** av, Func&& func) {
        bpo::variables_map configuration;
        bpo::store(bpo::command_line_parser(ac, av).options(_opts).run(), configuration);
        bpo::notify(configuration);
        if (configuration.count("help")) {
            std::cout << _opts << "\n";
            return 1;
        }
        smp::configure(configuration);
        engine.when_started().then(func);
        engine.run();
        return 0;
    };
};

#endif
