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

#include "routes.hh"
#include "reply.hh"
#include "exception.hh"

namespace httpd {

using namespace std;

void verify_param(const request& req, const sstring& param) {
    if (req.get_query_param(param) == "") {
        throw missing_param_exception(param);
    }
}

routes::~routes() {
    for (int i = 0; i < NUM_OPERATION; i++) {
        for (auto kv : _map[i]) {
            delete kv.second;
        }
    }
    for (int i = 0; i < NUM_OPERATION; i++) {
        for (auto r : _rules[i]) {
            delete r;
        }
    }

}

future<std::unique_ptr<reply> > routes::handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    handler_base* handler = get_handler(str2type(req->_method),
            normalize_url(path), req->param);
    if (handler != nullptr) {
        try {
            for (auto& i : handler->_mandatory_param) {
                verify_param(*req.get(), i);
            }
            auto r =  handler->handle(path, std::move(req), std::move(rep));
            return r;
        } catch (const redirect_exception& _e) {
            rep.reset(new reply());
            rep->add_header("Location", _e.url).set_status(_e.status()).done(
                    "json");

        } catch (const base_exception& _e) {
            rep.reset(new reply());
            json_exception e(_e);
            rep->set_status(_e.status(), e.to_json()).done("json");
        } catch (exception& _e) {
            rep.reset(new reply());
            json_exception e(_e);
            cerr << "exception was caught for " << path << ": " << _e.what()
                    << endl;
            rep->set_status(reply::status_type::internal_server_error,
                    e.to_json()).done("json");
        }
    } else {
        rep.reset(new reply());
        json_exception ex(not_found_exception("Not found"));
        rep->set_status(reply::status_type::not_found, ex.to_json()).done(
                "json");
    }
    cerr << "Failed with " << path << " " << rep->_content << endl;
    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
}

sstring routes::normalize_url(const sstring& url) {
    if (url.length() < 2 || url.at(url.length() - 1) != '/') {
        return url;
    }
    return url.substr(0, url.length() - 1);
}

handler_base* routes::get_handler(operation_type type, const sstring& url,
        parameters& params) {
    handler_base* handler = get_exact_match(type, url);
    if (handler != nullptr) {
        return handler;
    }

    for (auto rule = _rules[type].cbegin(); rule != _rules[type].cend();
            ++rule) {
        handler = (*rule)->get(url, params);
        if (handler != nullptr) {
            return handler;
        }
        params.clear();
    }
    return nullptr;
}

routes& routes::add(operation_type type, const url& url,
        handler_base* handler) {
    match_rule* rule = new match_rule(handler);
    rule->add_str(url._path);
    if (url._param != "") {
        rule->add_param(url._param, true);
    }
    return add(rule, type);
}

}
