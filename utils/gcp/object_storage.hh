/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <variant>
#include <string>
#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/net/tls.hh>

#include "utils/rjson.hh"
#include "utils/chunked_vector.hh"

namespace utils::gcp {
    struct google_credentials;
}

namespace utils::gcp::storage {

    /**
     * List info on a named object in a bucket
     */
    struct object_info {
        std::string name;
        std::string content_type;
        uint64_t size;
        uint64_t generation;
        // TODO: what info do we need?
    };

    /**
     * Minimal GCP object storage client
     */
    class client {
        class impl;
        class object_data_sink;
        class object_data_source;
        shared_ptr<impl> _impl;
    public:
        static const std::string DEFAULT_ENDPOINT;

        client(client&&);

        /**
         * @endpoint - typically https://storage.googleapis.com.
         * @credentials - google credentials for accessing the bucket(s). Can be nullopt, but only really useful for mockup testing.
         * @certs - TLS certs (truststore). Optional TLS parameters for connecting to endpoint. Normally not required (default is 
         * using system trust iff endpoint is a https url)
         */
        client(std::string_view endpoint, std::optional<google_credentials> credentials, shared_ptr<seastar::tls::certificate_credentials> certs={});
        ~client();

        /**
         * Creates a named bucket in project and region, using storage_class
         */
        future<> create_bucket(std::string_view project, std::string_view bucket, std::string_view region = {}, std::string_view storage_class = {});
        /**
         * Creates a named bucket in project, using provided metadata json 
         * See https://cloud.google.com/storage/docs/creating-buckets
         */
        future<> create_bucket(std::string_view project, rjson::value meta);
        /**
         * Deletes a bucket. Note: bucket must be empty.
         */
        future<> delete_bucket(std::string_view bucket);

        /**
         * List objects in bucket. Optionally applies the @prefix as filter
         */
        future<utils::chunked_vector<object_info>> list_objects(std::string_view bucket, std::string_view prefix = {});
        /**
         * Deletes a named object from bucket
         */
        future<> delete_object(std::string_view bucket, std::string_view object_name);
        /**
         * Renames a named object in bucket
         */
        future<> rename_object(std::string_view bucket, std::string_view object_name, std::string_view new_name);
        /**
         * Moves a named object from one bucket to a different one using new name
         */
        future<> rename_object(std::string_view bucket, std::string_view object_name, std::string_view new_bucket, std::string_view new_name);
        /**
         * Copies a named object to @new_name
         */
        future<> copy_object(std::string_view bucket, std::string_view object_name, std::string_view to_name);
        /**
         * Copies a named object to @new_bucket and @new_name
         */
        future<> copy_object(std::string_view bucket, std::string_view object_name, std::string_view new_bucket, std::string_view to_name);

        /**
         * Creates a data_sink for uploading data to a given name in bucket.
         *
         * @name - name of object to create/overwrite
         * @metadata - optional metadata to set in the created object.
         * 
         * Note: this will overwrite any existing object of the same name.
         */
        seastar::data_sink create_upload_sink(std::string_view bucket, std::string_view object_name, rjson::value metadata = {}) const;
        /**
         * Creates a data_source for reading from a named object.
         */
        seastar::data_source create_download_source(std::string_view bucket, std::string_view object_name) const;

        /**
         * Destroys resources. Must be called before releasing object
         */
        future<> close();
    };

    class storage_error : public std::runtime_error {
        int _status;
    public:
        storage_error(const std::string&);
        storage_error(int status, const std::string&);
        // TODO: make http::status_type non-nested type, and forward declarable
        int status() const {
            return _status;
        }
    };

    class permission_error : public storage_error {
    public:
        using mybase = storage_error;
        using mybase::mybase;
    };

    class failed_operation : public storage_error {
    public:
        using mybase = storage_error;
        using mybase::mybase;
    };

    class failed_upload_error : public failed_operation {
    public:
        using mybase = failed_operation;
        using mybase::mybase;
    };
}
