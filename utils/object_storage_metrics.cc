/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>
#include <vector>

#include <seastar/core/format.hh>
#include <seastar/core/metrics.hh>
#include <seastar/http/client.hh>

#include "utils/object_storage_metrics.hh"
#include "utils/log.hh"

static logging::logger oslog("object_storage_metrics");

using namespace seastar;

utils::http_client_metrics::http_client_metrics(const seastar::http::client& http, object_storage_metrics_labels labels) {
    namespace sm = seastar::metrics;
    auto endpoint = labels.endpoint;
    auto type_label = sm::label("type")(std::move(labels.type));
    auto ep_label = sm::label("endpoint")(std::move(labels.endpoint));
    auto sg_label = sm::label("class")(std::move(labels.class_name));
    auto method_label = sm::label("method");
    std::vector<sm::label_instance> label_set{type_label, ep_label, sg_label};

    std::vector<sm::metric_definition> defs;

    defs.emplace_back(sm::make_gauge("nr_connections", [&http] { return http.connections_nr(); },
            sm::description("Total number of connections"), label_set));
    defs.emplace_back(sm::make_gauge("nr_active_connections", [&http] { return http.connections_nr() - http.idle_connections_nr(); },
            sm::description("Total number of connections with running requests"), label_set));
    defs.emplace_back(sm::make_counter("total_new_connections", [&http] { return http.total_new_connections_nr(); },
            sm::description("Total number of new connections created so far"), label_set));
    defs.emplace_back(sm::make_counter("total_read_requests", [&http] { return http.get_stats()[httpd::GET].ops; },
            sm::description("Total number of object read requests"), label_set));
    defs.emplace_back(sm::make_counter("total_write_requests", [&http] { return http.get_stats()[httpd::PUT].ops; },
            sm::description("Total number of object write requests"), label_set));
    defs.emplace_back(sm::make_counter("total_read_latency_sec", [&http] { return http.get_stats()[httpd::GET].latency.count(); },
            sm::description("Total time spent reading data from objects"), label_set));
    defs.emplace_back(sm::make_counter("total_write_latency_sec", [&http] { return http.get_stats()[httpd::PUT].latency.count(); },
            sm::description("Total time spend writing data to objects"), label_set));
    defs.emplace_back(sm::make_counter("integrated_request_queue_length", [&http] { return http.integrated_requests_queued().integral(); },
            sm::description("The number of queued HTTP requests integrated over time (measured in request-seconds)"), label_set));

    // Per HTTP method metrics
    for (size_t i = 0; i < httpd::operation_type::NUM_OPERATION; ++i) {
        auto method = static_cast<httpd::operation_type>(i);
        auto method_name = httpd::type2str(method);
        auto lower_method_name = method_name;
        std::ranges::transform(method_name, lower_method_name.begin(), ::tolower);
        auto method_labels = label_set;
        method_labels.emplace_back(method_label(method_name));
        defs.emplace_back(sm::make_counter(format("total_{}_requests", lower_method_name),
                [&http, method] { return http.get_stats()[method].ops; },
                sm::description(format("Total number of HTTP {} requests", method_name)), method_labels)
                (sm::skip_when_empty::yes));
        defs.emplace_back(sm::make_counter(format("total_{}_latency_sec", lower_method_name),
                [&http, method] { return http.get_stats()[method].latency.count(); },
                sm::description(format("Total time in seconds spent in HTTP {} requests", method_name)), method_labels)
                (sm::skip_when_empty::yes));
        defs.emplace_back(sm::make_counter(format("total_{}_retries", lower_method_name),
                [&http, method] { return http.get_stats()[method].retries; },
                sm::description(format("Total number of HTTP {} retries", method_name)), method_labels)
                (sm::skip_when_empty::yes));
    }

    try {
        _metrics.add_group("object_storage", defs);
    } catch (const seastar::metrics::double_registration& e) {
        // An endpoint dropped from the configuration keeps its client for as long
        // as anything references it, so a client created after the same endpoint
        // is added back can find the labels taken. Reporting metrics is not worth
        // failing whatever operation created the client.
        oslog.warn("Not reporting object storage metrics for {}: {}", endpoint, e.what());
    }
}
