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

#ifndef TRANSFORMERS_HH_
#define TRANSFORMERS_HH_

#include "handlers.hh"
#include "file_handler.hh"

namespace httpd {

/**
 * content_replace replaces variable in a file with a dynamic value.
 * It would take the host from request and will replace the variable
 * in a file
 *
 * The replacement can be restricted to an extension.
 *
 * We are currently support only one file type for replacement.
 * It could be extend if we will need it
 *
 */
class content_replace : public file_transformer {
public:
    virtual void transform(sstring& content, const request& req,
            const sstring& extension) override;
    /**
     * the constructor get the file extension the replace would work on.
     * @param extension file extension, when not set all files extension
     */
    explicit content_replace(const sstring& extension = "")
            : extension(extension) {
    }
private:
    sstring extension;
};

}
#endif /* TRANSFORMERS_HH_ */
