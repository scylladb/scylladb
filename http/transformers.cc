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

#include <boost/algorithm/string/replace.hpp>
#include "transformers.hh"
#include <experimental/string_view>

namespace httpd {

using namespace std;

void content_replace::transform(sstring& content, const request& req,
        const sstring& extension) {
    sstring host = req.get_header("Host");
    if (host == "" || (this->extension != "" && extension != this->extension)) {
        return;
    }
    boost::replace_all(content, "{{Host}}", host);
    boost::replace_all(content, "{{Protocol}}", req.get_protocol_name());
}

}
