/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
#ifndef _APP_TEMPLATE_HH
#define _APP_TEMPLATE_HH

#include <boost/program_options.hpp>
#include <boost/optional.hpp>
#include <functional>

namespace bpo = boost::program_options;

class app_template {
private:
    bpo::options_description _opts;
    boost::optional<bpo::variables_map> _configuration;
public:
    app_template();
    boost::program_options::options_description_easy_init add_options();
    bpo::variables_map& configuration();
    int run(int ac, char ** av, std::function<void ()>&& func);
};

#endif
