/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */
#ifndef _APP_TEMPLATE_HH
#define _APP_TEMPLATE_HH

#include <boost/program_options.hpp>
#include <boost/optional.hpp>
#include <functional>

class app_template {
private:
    boost::program_options::options_description _opts;
    boost::optional<boost::program_options::variables_map> _configuration;
public:
    app_template();
    boost::program_options::options_description_easy_init add_options();
    boost::program_options::variables_map& configuration();
    int run(int ac, char ** av, std::function<void ()>&& func);
};

#endif
