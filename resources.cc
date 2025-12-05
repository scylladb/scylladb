/*
 * Copyright (C) 2025-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/http/short_streams.hh>

#include "resources.hh"
#include "utils/rjson.hh"

#include "seastarx.hh"

namespace rjs = rjson::schema;

namespace resources {

future<std::vector<resource>> load_resource_manifest(const std::filesystem::path& manifest_path) {
    if (!co_await file_accessible(manifest_path.native(), access_flags::exists | access_flags::read)) {
        co_await coroutine::return_exception(std::runtime_error("Cannot access file for reading"));
    }

    auto f = co_await open_file_dma(manifest_path.native(), open_flags::ro);
    auto in = make_file_input_stream(f);

    auto manifest = rjson::parse_and_validate(
            co_await util::read_entire_stream_contiguous(in),
            rjs::array(rjs::object({
                {"name", rjs::scalar::string()},
                {"content-type", rjs::scalar::string()},
                {"file", rjs::scalar::string()},
                })));

    co_await in.close();

    if (!manifest) {
        co_await coroutine::return_exception(std::runtime_error(manifest.error()));
    }

    std::vector<resource> resources;
    for (const auto& res : manifest->GetArray()) {
        resources.emplace_back(resource{
            .name = std::string(rjson::to_string_view(res["name"])),
            .content_type = std::string(rjson::to_string_view(res["content-type"])),
            .compressed = false,
            .content = std::filesystem::path(rjson::to_string_view(res["file"])),
        });
    }

    co_return std::move(resources);
}

} // namespace resources
