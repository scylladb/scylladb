/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <string>
#include <optional>
#include <filesystem>

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <fmt/core.h>

#include "utils/lister.hh"

namespace seastar {
class abort_source;
class data_sink;
class data_source;
class file;

}

namespace db {
class object_storage_endpoint_param;
}

namespace utils {
class upload_progress;
}

using namespace seastar;

class memory_data_sink_buffers;
class abstract_lister;

namespace sstables {

class generation_type;

class object_name {
    std::string _name;
public:
    object_name(const object_name&);
    object_name(object_name&&);
    object_name(std::string_view bucket, std::string_view prefix, std::string_view type);
    object_name(std::string_view bucket, const generation_type&, std::string_view type);
    object_name(std::string_view bucket, std::string_view object);

    std::string_view bucket() const;
    std::string_view object() const;

    const std::string& str() const {
        return _name;
    }
    operator const std::string&() const {
        return str();
    }
    operator std::string() && {
        return std::move(_name);
    }
};

class object_storage_client {
public:
    virtual ~object_storage_client() = default;

    virtual future<> put_object(object_name, ::memory_data_sink_buffers bufs, abort_source* = nullptr) = 0;
    virtual future<> delete_object(object_name) = 0;
    virtual file make_readable_file(object_name, abort_source* = nullptr) = 0;
    virtual data_sink make_data_upload_sink(object_name, std::optional<unsigned> max_parts_per_piece, abort_source* = nullptr) = 0;
    virtual data_sink make_upload_sink(object_name, abort_source* = nullptr) = 0;
    virtual data_source make_download_source(object_name, abort_source* = nullptr) = 0;

    virtual abstract_lister make_object_lister(std::string bucket, std::string prefix, lister::filter_type) = 0;

    virtual future<> upload_file(std::filesystem::path path, object_name, utils::upload_progress& up, seastar::abort_source* = nullptr) = 0;

    virtual future<> update_config(const db::object_storage_endpoint_param&) = 0;

    virtual future<> close() = 0;
};

using shard_client_factory = std::function<shared_ptr<object_storage_client>(std::string)>;

shared_ptr<object_storage_client> make_object_storage_client(const db::object_storage_endpoint_param&, semaphore&, shard_client_factory);

}

template <>
struct fmt::formatter<sstables::object_name> : fmt::formatter<std::string_view> {
    auto format(const sstables::object_name&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
