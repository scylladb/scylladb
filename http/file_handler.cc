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

#include "file_handler.hh"
#include <algorithm>
#include <iostream>
#include "core/reactor.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include "core/app-template.hh"
#include "exception.hh"

namespace httpd {

directory_handler::directory_handler(const sstring& doc_root,
        file_transformer* transformer)
        : file_interaction_handler(transformer), doc_root(doc_root) {
}

future<std::unique_ptr<reply>> directory_handler::handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    sstring full_path = doc_root + req->param["path"];
    auto h = this;
    return engine().file_type(full_path).then(
            [h, full_path, req = std::move(req), rep = std::move(rep)](auto val) mutable {
                if (val) {
                    if (val.value() == directory_entry_type::directory) {
                        if (h->redirect_if_needed(*req.get(), *rep.get())) {
                            return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                        }
                        full_path += "/index.html";
                    }
                    return h->read(full_path, std::move(req), std::move(rep));
                }
                rep->set_status(reply::status_type::not_found).done();
                return make_ready_future<std::unique_ptr<reply>>(std::move(rep));

            });
}

file_interaction_handler::~file_interaction_handler() {
    delete transformer;
}

sstring file_interaction_handler::get_extension(const sstring& file) {
    size_t last_slash_pos = file.find_last_of('/');
    size_t last_dot_pos = file.find_last_of('.');
    sstring extension;
    if (last_dot_pos != sstring::npos && last_dot_pos > last_slash_pos) {
        extension = file.substr(last_dot_pos + 1);
    }
    return extension;
}

struct reader {
    reader(file f, std::unique_ptr<reply> rep)
            : is(
                    make_file_input_stream(make_lw_shared<file>(std::move(f)),
                            0, 4096)), _rep(std::move(rep)) {
    }
    input_stream<char> is;
    std::unique_ptr<reply> _rep;

    // for input_stream::consume():
    template<typename Done>
    void operator()(temporary_buffer<char> data, Done&& done) {
        if (data.empty()) {
            done(std::move(data));
            _rep->done();
        } else {
            _rep->_content.append(data.get(), data.size());
        }
    }
};

future<std::unique_ptr<reply>> file_interaction_handler::read(
        const sstring& file_name, std::unique_ptr<request> req,
        std::unique_ptr<reply> rep) {
    sstring extension = get_extension(file_name);
    rep->set_content_type(extension);
    return engine().open_file_dma(file_name, open_flags::ro).then(
            [rep = std::move(rep)](file f) mutable {
                std::shared_ptr<reader> r = std::make_shared<reader>(std::move(f), std::move(rep));

                return r->is.consume(*r).then([r]() {
                            r->_rep->done();
                            return make_ready_future<std::unique_ptr<reply>>(std::move(r->_rep));
                        });
            });
}

bool file_interaction_handler::redirect_if_needed(const request& req,
        reply& rep) const {
    if (req._url.length() == 0 || req._url.back() != '/') {
        rep.set_status(reply::status_type::moved_permanently);
        rep._headers["Location"] = req.get_url() + "/";
        rep.done();
        return true;
    }
    return false;
}

future<std::unique_ptr<reply>> file_handler::handle(const sstring& path,
        std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
    if (force_path && redirect_if_needed(*req.get(), *rep.get())) {
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
    return read(file, std::move(req), std::move(rep));
}

}
