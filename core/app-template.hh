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
    boost::program_options::positional_options_description _pos_opts;
    boost::optional<boost::program_options::variables_map> _configuration;
public:
    struct positional_option {
        const char* name;
        const boost::program_options::value_semantic* value_semantic;
        const char* help;
        int max_count;
    };
public:
    app_template();
    boost::program_options::options_description_easy_init add_options();
    void add_positional_options(std::initializer_list<positional_option> options);
    boost::program_options::variables_map& configuration();
    int run(int ac, char ** av, std::function<void ()>&& func);
};

#endif
