/*
 * Copyright (C) 2024 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once 

#include <vector>
#include <optional>
#include <chrono>
#include <iosfwd>
#include <string>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>

#include "symmetric_key.hh"

namespace encryption {

class encryption_context;
struct key_info;

class gcp_host {
public:
    class impl;

    template<typename T>
    struct t_credentials_source {
        // Path to credentials JSON file (exported from gcloud console)
        T gcp_credentials_file;
        // Optional service account (email address) to impersonate
        T gcp_impersonate_service_account;
    };

    using credentials_source = t_credentials_source<std::string>;

    struct host_options : public credentials_source {
        std::string gcp_project_id;
        std::string gcp_location;

        // GCP KMS Key to encrypt data keys with. Format: <keychain>/<keyname>
        std::string master_key;

        // tls. if unspeced, use system for https
        // GCP does not (afaik?) allow certificate auth
        // but we keep the option available just in case.
        std::string certfile;
        std::string keyfile;
        std::string truststore;
        std::string priority_string;

        std::optional<std::chrono::milliseconds> key_cache_expiry;
        std::optional<std::chrono::milliseconds> key_cache_refresh;
    };

    using id_type = bytes;

    gcp_host(encryption_context&, const std::string& name, const host_options&);
    gcp_host(encryption_context&, const std::string& name, const std::unordered_map<sstring, sstring>&);
    ~gcp_host();

    future<> init();
    const host_options& options() const;

    struct option_override : public t_credentials_source<std::optional<std::string>> {
        std::optional<std::string> master_key;
    };

    future<std::tuple<shared_ptr<symmetric_key>, id_type>> get_or_create_key(const key_info&, const option_override* = nullptr);
    future<shared_ptr<symmetric_key>> get_key_by_id(const id_type&, const key_info&, const option_override* = nullptr);
private:
    std::unique_ptr<impl> _impl;
};

}
