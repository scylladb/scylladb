/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "api/api.hh"
#include "api/vector_search.hh"
#include <seastar/core/fstream.hh>
#include <seastar/http/api_docs.hh>

namespace api {
using namespace seastar::httpd;

const sstring vector_search_api_filepath = "api/api-doc/vector_search";

void register_vector_search(std::shared_ptr<httpd::api_registry_builder20> rb, http_context& ctx, routes& r) {
    rb->register_function(r, [] (output_stream<char>& os) {
        return open_file_dma(vector_search_api_filepath + ".json", open_flags::ro).then([&os] (file f) mutable {
            return do_with(input_stream<char>(make_file_input_stream(std::move(f))), [&os](input_stream<char>& is) {
                return copy(is, os).then([&is, &os] {
                    return os.write(",\n").then([&is] {
                        return is.close();
                    });
                });
            });
        });
    });
}

void register_vector_search_definitions(std::shared_ptr<httpd::api_registry_builder20> rb, http_context& ctx, routes& r) {
    rb->add_definition(r, [] (output_stream<char>& os) {
        return open_file_dma(vector_search_api_filepath + ".def.json", open_flags::ro).then([&os] (file f) mutable {
            return do_with(input_stream<char>(make_file_input_stream(std::move(f))), [&os](input_stream<char>& is) {
                return copy(is, os).then([&is, &os] {
                    return os.write(",\n").then([&is] {
                        return is.close();
                    });
                });
            });
        });
    });
}

}

