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
#include <fmt/core.h>

#include "db/object_storage_endpoint_param.hh"

#include "object_storage_client.hh"
#include "generation_type.hh"
#include "utils/s3/client.hh"
#include "utils/s3/creds.hh"
#include "utils/memory_data_sink.hh"
#include "utils/lister.hh"

using namespace seastar;
using namespace sstables;


sstables::object_name::object_name(std::string_view bucket, std::string_view prefix, std::string_view type)
    : _name(fmt::format("/{}/{}/{}", bucket, prefix, type))
{}

sstables::object_name::object_name(std::string_view bucket, const generation_type& gen, std::string_view type) 
    : _name(fmt::format("/{}/{}/{}", bucket, gen, type))
{}
sstables::object_name::object_name(std::string_view bucket, std::string_view object) 
    : _name(fmt::format("/{}/{}", bucket, object))
{}

sstables::object_name::object_name(const object_name&) = default;
sstables::object_name::object_name(object_name&&) = default;

std::string_view sstables::object_name::bucket() const {
    auto i = _name.find_first_of('/', 1);
    return std::string_view(_name).substr(1, i - 1);
}

std::string_view sstables::object_name::object() const {
    auto i = _name.find_first_of('/', 1);
    return std::string_view(_name).substr(i + 1);
}

auto 
fmt::formatter<sstables::object_name>::format(const sstables::object_name& n, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", n.str());
}

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

    future<> put_object(object_name name, ::memory_data_sink_buffers bufs, abort_source* as) override {
        return _client->put_object(name.str(), std::move(bufs), as);
    }
    future<> delete_object(object_name name) override {
        return _client->delete_object(name.str());
    }
    file make_readable_file(object_name name, abort_source* as) override {
        return _client->make_readable_file(name.str(), as);
    }
    data_sink make_data_upload_sink(object_name name, std::optional<unsigned> max_parts_per_piece, abort_source* as) override {
        return _client->make_upload_jumbo_sink(name.str(), max_parts_per_piece, as);
    }
    data_sink make_upload_sink(object_name name, abort_source* as) override {
        return _client->make_upload_sink(name.str(), as);
    }
    data_source make_download_source(object_name name, abort_source* as) override {
        return _client->make_chunked_download_source(name.str(), s3::full_range, as);
    }
    abstract_lister make_object_lister(std::string bucket, std::string prefix, lister::filter_type filter) override {
        return abstract_lister::make<s3::client::bucket_lister>(_client, std::move(bucket), std::move(prefix), std::move(filter));
    }
    future<> upload_file(std::filesystem::path path, object_name name, utils::upload_progress& up, seastar::abort_source* as) override {
        return _client->upload_file(std::move(path), name.str(), up, as);
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
