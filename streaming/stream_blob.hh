/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "message/messaging_service_fwd.hh"
#include <cstdint>
#include <vector>
#include <list>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>
#include <seastar/rpc/rpc_types.hh>
#include "utils/UUID.hh"
#include "dht/i_partitioner.hh"
#include "bytes.hh"
#include "replica/database_fwd.hh"
#include "locator/host_id.hh"
#include "service/topology_guard.hh"
#include "sstables/open_info.hh"

#include <fmt/core.h>
#include <fmt/ostream.h>

namespace streaming {

using file_stream_id = utils::tagged_uuid<struct file_stream_id_tag>;

// - The file_ops::stream_sstables is used to stream a sstable file.
//
// - The file_ops::load_sstables is used to stream a sstable file and
// ask the receiver to load the sstable into the system.
enum class file_ops : uint16_t {
    stream_sstables,
    load_sstables,
};

// For STREAM_BLOB verb
enum class stream_blob_cmd : uint8_t {
    ok,
    error,
    data,
    end_of_stream,
};

class stream_blob_data {
public:
    temporary_buffer<char> buf;
    stream_blob_data() = default;
    stream_blob_data(temporary_buffer<char> b) : buf(std::move(b)) {}
    const char* data() const {
        return buf.get();
    }
    size_t size() const {
        return buf.size();
    }
    bool empty() const {
        return buf.size() == 0;
    }
};

class stream_blob_cmd_data {
public:
    stream_blob_cmd cmd;
    // The optional data contains value when the cmd is stream_blob_cmd::data.
    // When the cmd is set to other values, e.g., stream_blob_cmd::error, the
    // data contains no value.
    std::optional<stream_blob_data> data;
    stream_blob_cmd_data(stream_blob_cmd c) : cmd(c) {}
    stream_blob_cmd_data(stream_blob_cmd c, std::optional<stream_blob_data> d)
        : cmd(c)
        , data(std::move(d))
    {}
    stream_blob_cmd_data(stream_blob_cmd c, stream_blob_data d)
        : cmd(c)
        , data(std::move(d))
    {}

};

class stream_blob_meta {
public:
    file_stream_id ops_id;
    table_id table;
    sstring filename;
    seastar::shard_id dst_shard_id;
    streaming::file_ops fops;
    service::frozen_topology_guard topo_guard;
    std::optional<sstables::sstable_state> sstable_state;
    // We can extend this verb to send arbitrary blob of data
};

enum class store_result {
    ok, failure,
};

using stream_blob_source_fn = noncopyable_function<future<input_stream<char>>(const file_input_stream_options&)>;
using stream_blob_finish_fn = noncopyable_function<future<>(store_result)>;
using output_result = std::tuple<stream_blob_finish_fn, output_stream<char>>;
using stream_blob_create_output_fn = noncopyable_function<future<output_result>(replica::database&, const streaming::stream_blob_meta&)>;

struct stream_blob_info {
    sstring filename;
    streaming::file_ops fops;
    std::optional<sstables::sstable_state> sstable_state;
    stream_blob_source_fn source;

    friend inline std::ostream& operator<<(std::ostream& os, const stream_blob_info& x) {
        return os << x.filename;
    }
};

// The handler for the STREAM_BLOB verb.
seastar::future<> stream_blob_handler(replica::database& db, netw::messaging_service& ms, gms::inet_address from, streaming::stream_blob_meta meta, rpc::sink<streaming::stream_blob_cmd_data> sink, rpc::source<streaming::stream_blob_cmd_data> source);

// Exposed mainly for testing

future<> stream_blob_handler(replica::database& db,
        netw::messaging_service& ms,
        gms::inet_address from,
        streaming::stream_blob_meta meta,
        rpc::sink<streaming::stream_blob_cmd_data> sink,
        rpc::source<streaming::stream_blob_cmd_data> source,
        stream_blob_create_output_fn,
        bool may_inject_errors = false
        );

// For TABLET_STREAM_FILES
class node_and_shard {
public:
    locator::host_id node;
    seastar::shard_id shard;
    friend inline std::ostream& operator<<(std::ostream& os, const node_and_shard& x) {
        return os << x.node << ":" << x.shard;
    }

};

}

template <> struct fmt::formatter<streaming::node_and_shard> : fmt::ostream_formatter {};

namespace streaming {

class stream_files_request {
public:
    file_stream_id ops_id;
    sstring keyspace_name;
    sstring table_name;
    table_id table;
    dht::token_range range;
    std::vector<streaming::node_and_shard> targets;
    service::frozen_topology_guard topo_guard;
};

class stream_files_response {
public:
    size_t stream_bytes = 0;
};

using host2ip_t = std::function<future<gms::inet_address> (locator::host_id)>;

// The handler for the TABLET_STREAM_FILES verb. The receiver of this verb will
// stream sstables files specified by the stream_files_request req.
future<stream_files_response> tablet_stream_files_handler(replica::database& db, netw::messaging_service& ms, streaming::stream_files_request req, host2ip_t host2ip);

// Ask the src node to stream sstables to dst node for table in the given token range using TABLET_STREAM_FILES verb.
future<stream_files_response> tablet_stream_files(const file_stream_id& ops_id, replica::table& table, const dht::token_range& range, const locator::host_id& src, const locator::host_id& dst, seastar::shard_id dst_shard_id, netw::messaging_service& ms, abort_source& as, service::frozen_topology_guard topo_guard);

// Exposed for testability
future<size_t> tablet_stream_files(netw::messaging_service& ms,
    std::list<stream_blob_info> sources,
    std::vector<node_and_shard> targets,
    table_id table,
    file_stream_id ops_id,
    host2ip_t host2ip,
    service::frozen_topology_guard topo_guard,
    bool may_inject_errors = false
    );


future<> mark_tablet_stream_start(file_stream_id);
future<> mark_tablet_stream_done(file_stream_id);

}

template<> struct fmt::formatter<streaming::stream_blob_info>;
