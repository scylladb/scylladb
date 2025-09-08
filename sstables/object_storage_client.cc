/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <string>
#include <optional>

#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>

#include "db/object_storage_endpoint_param.hh"

#include "object_storage_client.hh"
#include "utils/s3/client.hh"
#include "utils/s3/creds.hh"
#include "utils/memory_data_sink.hh"
#include "utils/lister.hh"

using namespace seastar;
using namespace sstables;

class s3_client_wrapper : public sstables::object_storage_client {
    shared_ptr<s3::client> _client;
    shard_client_factory _cf;
public:
    s3_client_wrapper(const std::string& host, s3::endpoint_config_ptr cfg, semaphore& memory, shard_client_factory cf)
        : _client(s3::client::make(host, cfg, memory, std::bind_front(&s3_client_wrapper::shard_client, this)))
        , _cf(std::move(cf))
    {}
    shared_ptr<s3::client> shard_client(std::string host) const {
        auto lc = _cf(host);
        return lc ? dynamic_pointer_cast<s3_client_wrapper>(lc)->_client : shared_ptr<s3::client>{};
    }

    future<> put_object(std::string object_name, ::memory_data_sink_buffers bufs, abort_source* as) override {
        return _client->put_object(std::move(object_name), std::move(bufs), as);
    }
    future<> delete_object(std::string object_name) override {
        return _client->delete_object(std::move(object_name));
    }
    file make_readable_file(std::string object_name, abort_source* as) override {
        return _client->make_readable_file(std::move(object_name), as);
    }
    data_sink make_data_upload_sink(std::string object_name, std::optional<unsigned> max_parts_per_piece, abort_source* as) override {
        return _client->make_upload_jumbo_sink(std::move(object_name), max_parts_per_piece, as);
    }
    data_sink make_upload_sink(std::string object_name, abort_source* as) override {
        return _client->make_upload_sink(std::move(object_name), as);
    }
    data_source make_download_source(std::string object_name, abort_source* as) override {
        return _client->make_chunked_download_source(std::move(object_name), s3::full_range, as);
    }
    abstract_lister make_object_lister(std::string bucket, std::string prefix, lister::filter_type filter) override {
        return abstract_lister::make<s3::client::bucket_lister>(_client, std::move(bucket), std::move(prefix), std::move(filter));
    }
    future<> upload_file(std::filesystem::path path, std::string object_name, utils::upload_progress& up, seastar::abort_source* as) override {
        return _client->upload_file(std::move(path), std::move(object_name), up, as);
    }
    future<> update_config(const db::object_storage_endpoint_param& ep) override {
        auto s3_cfg = make_lw_shared<s3::endpoint_config>(ep.get_s3_storage().config);
        return _client->update_config(std::move(s3_cfg));
    }
    future<> close() override {
        return _client->close();
    }
};

shared_ptr<object_storage_client> sstables::make_object_storage_client(const db::object_storage_endpoint_param& ep, semaphore& memory, shard_client_factory cf) {
    if (ep.is_s3_storage()) {
        auto& epc = ep.get_s3_storage();
        auto s3_cfg = make_lw_shared<s3::endpoint_config>(epc.config);
        return seastar::make_shared<s3_client_wrapper>(epc.endpoint, std::move(s3_cfg), memory, std::move(cf));
    }
    throw std::invalid_argument("Not implemented");
}
