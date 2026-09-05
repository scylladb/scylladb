/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <string>

#include <seastar/core/metrics_registration.hh>

namespace seastar::http {
class client;
}

namespace utils {

// The labels an object storage client's owner puts on its metrics. A client does
// not know whether it is the only one serving its endpoint on this shard, so it
// does not name itself, and a client whose owner supplies no labels reports no
// metrics.
struct object_storage_metrics_labels {
    std::string type;
    std::string endpoint;
    std::string class_name;
};

// The bytes an object storage client has moved to and from objects. The client
// counts them, because only it sees the transfers; its owner reports them.
struct object_storage_bytes {
    uint64_t read = 0;
    uint64_t written = 0;
};

// Registers the http client half of the object_storage metric group: connections,
// per method requests, latencies, retries and the queued requests. The bytes moved
// to and from objects are reported by the owner, which knows what an object
// operation is.
//
// The http client must outlive the object.
class http_client_metrics {
    seastar::metrics::metric_groups _metrics;
public:
    http_client_metrics(const seastar::http::client& http, object_storage_metrics_labels labels);
};

} // namespace utils
