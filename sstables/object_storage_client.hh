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

class object_storage_client {
public:
    virtual ~object_storage_client() = default;

    virtual future<> put_object(std::string object_name, ::memory_data_sink_buffers bufs, abort_source* = nullptr) = 0;
    virtual future<> delete_object(std::string object_name) = 0;
    virtual file make_readable_file(std::string object_name, abort_source* = nullptr) = 0;
    virtual data_sink make_data_upload_sink(std::string object_name, std::optional<unsigned> max_parts_per_piece, abort_source* = nullptr) = 0;
    virtual data_sink make_upload_sink(std::string object_name, abort_source* = nullptr) = 0;
    virtual data_source make_download_source(std::string object_name, abort_source* = nullptr) = 0;

    virtual abstract_lister make_object_lister(std::string bucket, std::string prefix, lister::filter_type) = 0;

    virtual future<> upload_file(std::filesystem::path path, std::string object_name, utils::upload_progress& up, seastar::abort_source* = nullptr) = 0;

    virtual future<> update_config(const db::object_storage_endpoint_param&) = 0;

    virtual future<> close() = 0;
};

using shard_client_factory = std::function<shared_ptr<object_storage_client>(std::string)>;

shared_ptr<object_storage_client> make_object_storage_client(const db::object_storage_endpoint_param&, semaphore&, shard_client_factory);

}