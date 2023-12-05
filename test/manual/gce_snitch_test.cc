/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/// \brief Tests the GoogleCloudSnitch.
/// Uses the dummy GCE meta server that would be listening on 127.0.0.1:80 by default.
/// To change the IP of the dummy server one can use the DUMMY_META_SERVER_IP environment macro.
/// To use the real GCE meta server (from inside the GCE VM) one should define the USE_GCE_META_SERVER environment macro.
///
/// For example:
///
///    Use the real GCE meta server
///    $ USE_GCE_META_SERVER="1" ./gce_snitch_test
///
///    Use the dummy meta server on 127.0.0.8
///    $ sudo DUMMY_META_SERVER_IP="128.0.0.8" ./gce_snitch_test

#include <boost/test/unit_test.hpp>
#include "locator/gce_snitch.hh"
#include <seastar/testing/test_case.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/std-compat.hh>

namespace fs = std::filesystem;

static fs::path test_files_subdir("test/resource/snitch_property_files");
static constexpr const char* DUMMY_META_SERVER_IP = "DUMMY_META_SERVER_IP";
static constexpr const char* USE_GCE_META_SERVER = "USE_GCE_META_SERVER";

class gce_meta_get_handler : public seastar::httpd::handler_base {
    virtual future<std::unique_ptr<seastar::http::reply>> handle(const sstring& path, std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply> rep) override {
        using namespace seastar::httpd;

        if (path == locator::gce_snitch::ZONE_NAME_QUERY_REQ) {
            rep->write_body(sstring("txt"), sstring("projects/431729375847/zones/us-central1-a"));
        } else {
            rep->set_status(http::reply::status_type::bad_request);
        }

        return make_ready_future<std::unique_ptr<http::reply>>(std::move(rep));
    }
};

future<> one_test(const std::string& property_fname, bool exp_result) {
    return seastar::async([property_fname, exp_result] () {
        using namespace locator;

        fs::path fname(test_files_subdir);
        fname /= fs::path(property_fname);

        auto my_address = gms::inet_address("localhost");

        char* meta_url_env = std::getenv(DUMMY_META_SERVER_IP);
        char* use_gce_server = std::getenv(USE_GCE_META_SERVER);
        bool use_dummy_server = true;
        sstring meta_url = "127.0.0.1";

        if (use_gce_server != nullptr) {
            meta_url = locator::gce_snitch::GCE_QUERY_SERVER_ADDR;
            use_dummy_server = false;
        } else if (meta_url_env != nullptr) {
            meta_url = meta_url_env;
        }

        httpd::http_server_control http_server;

        try {
            if (use_dummy_server) {
                http_server.start("dummy_GCE_meta_server").get();
                http_server.set_routes([] (httpd::routes& r) {
                    r.put(seastar::httpd::operation_type::GET, locator::gce_snitch::ZONE_NAME_QUERY_REQ, new gce_meta_get_handler());
                }).get();
                http_server.listen(ipv4_addr(meta_url.c_str(), 80)).get();
            }

            snitch_config cfg;
            cfg.name = "GoogleCloudSnitch";
            cfg.properties_file_name = fname.string();
            cfg.gce_meta_server_url = meta_url;
            cfg.listen_address = my_address;
            cfg.broadcast_address = my_address;
            sharded<snitch_ptr> snitch;
            snitch.start(cfg).get();
            snitch.invoke_on_all(&snitch_ptr::start).get();
            if (!exp_result) {
                BOOST_ERROR("Failed to catch an error in a malformed configuration file");
                snitch.stop().get();
                if (use_dummy_server) {
                    http_server.stop().get();
                }
                return;
            }
            auto cpu0_dc = make_lw_shared<sstring>();
            auto cpu0_rack = make_lw_shared<sstring>();
            auto res = make_lw_shared<bool>(true);

            snitch.invoke_on(0, [cpu0_dc, cpu0_rack, res] (snitch_ptr& inst) {
                *cpu0_dc =inst->get_datacenter();
                *cpu0_rack = inst->get_rack();
            }).get();

            snitch.invoke_on_all([cpu0_dc, cpu0_rack, res] (snitch_ptr& inst) {
                if (*cpu0_dc != inst->get_datacenter() ||
                    *cpu0_rack != inst->get_rack()) {
                    *res = false;
                }
            }).get();

            if (!*res) {
                BOOST_ERROR("Data center or Rack do not match on different shards");
            } else {
                BOOST_CHECK(true);
            }
            snitch.stop().get();
        } catch (std::exception& e) {
            BOOST_CHECK(!exp_result);
        }

        if (use_dummy_server) {
            http_server.stop().get();
        }
    });
}

#define GOSSIPING_TEST_CASE(tag, exp_res) \
SEASTAR_TEST_CASE(tag) { \
    return one_test(#tag".property", exp_res); \
}

////////////////////////////////////////////////////////////////////////////////
GOSSIPING_TEST_CASE(good_1,                    true);
