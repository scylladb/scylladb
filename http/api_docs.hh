/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef API_DOCS_HH_
#define API_DOCS_HH_
#include "json/json_elements.hh"
#include "json/formatter.hh"
#include "routes.hh"
#include "transformers.hh"
#include <string>

namespace httpd {

struct api_doc : public json::json_base {
    json::json_element<std::string> path;
    json::json_element<std::string> description;

    void register_params() {
        add(&path, "path");
        add(&description, "description");

    }
    api_doc() {
        register_params();
    }
    api_doc(const api_doc & e) {
        register_params();
        path = e.path;
        description = e.description;
    }
    template<class T>
    api_doc& operator=(const T& e) {
        path = e.path;
        description = e.description;
        return *this;
    }
    api_doc& operator=(const api_doc& e) {
        path = e.path;
        description = e.description;
        return *this;
    }
};

struct api_docs : public json::json_base {
    json::json_element<std::string> apiVersion;
    json::json_element<std::string> swaggerVersion;
    json::json_list<api_doc> apis;

    void register_params() {
        add(&apiVersion, "apiVersion");
        add(&swaggerVersion, "swaggerVersion");
        add(&apis, "apis");

    }
    api_docs() {
        apiVersion = "0.0.1";
        swaggerVersion = "1.2";
        register_params();
    }
    api_docs(const api_docs & e) {
        apiVersion = "0.0.1";
        swaggerVersion = "1.2";
        register_params();
    }
    template<class T>
    api_docs& operator=(const T& e) {
        apis = e.apis;
        return *this;
    }
    api_docs& operator=(const api_docs& e) {
        apis = e.apis;
        return *this;
    }
};

class api_registry : public handler_base {
    sstring _base_path;
    sstring _file_directory;
    api_docs _docs;
    routes& _routes;

public:
    api_registry(routes& routes, const sstring& file_directory,
            const sstring& base_path)
            : _base_path(base_path), _file_directory(file_directory), _routes(
                    routes) {
        _routes.put(GET, _base_path, this);
    }
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rep->_content = json::formatter::to_json(_docs);
        rep->done("json");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }

    void reg(const sstring& api, const sstring& description,
            const sstring& alternative_path = "") {
        api_doc doc;
        doc.description = description;
        doc.path = "/" + api;
        _docs.apis.push(doc);
        sstring path =
                (alternative_path == "") ?
                        _file_directory + api + ".json" : alternative_path;
        file_handler* index = new file_handler(path,
                new content_replace("json"));
        _routes.put(GET, _base_path + "/" + api, index);
    }
};

class api_registry_builder {
    sstring _file_directory;
    sstring _base_path;

public:
    static const sstring DEFAULT_DIR;
    static const sstring DEFAULT_PATH;

    api_registry_builder(const sstring& file_directory = DEFAULT_DIR,
            const sstring& base_path = DEFAULT_PATH)
            : _file_directory(file_directory), _base_path(base_path) {
    }

    void set_api_doc(routes& r) {
        new api_registry(r, _file_directory, _base_path);
    }

    void register_function(routes& r, const sstring& api,
            const sstring& description, const sstring& alternative_path = "") {
        auto h = r.get_exact_match(GET, _base_path);
        if (h) {
            // if a handler is found, it was added there by the api_registry_builder
            // with the set_api_doc method, so we know it's the type
            static_cast<api_registry*>(h)->reg(api, description, alternative_path);
        };
    }
};

}

#endif /* API_DOCS_HH_ */
