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

#ifndef HTTP_FILE_HANDLER_HH_
#define HTTP_FILE_HANDLER_HH_

#include "handlers.hh"

namespace httpd {
/**
 * This is a base class for file transformer.
 *
 * File transformer adds the ability to modify a file content before returning
 * the results.
 *
 * The transformer decides according to the file extension if transforming is
 * needed.
 */
class file_transformer {
public:
    /**
     * Any file transformer should implement this method.
     * @param content the content to transform
     * @param req the request
     * @param extension the file extension originating the content
     */
    virtual void transform(sstring& content, const request& req,
            const sstring& extension) = 0;

    virtual ~file_transformer() = default;
};

/**
 * A base class for handlers that interact with files.
 * directory and file handlers both share some common logic
 * with regards to file handling.
 * they both needs to read a file from the disk, optionally transform it,
 * and return the result or page not found on error
 */
class file_interaction_handler : public handler_base {
public:
    file_interaction_handler(file_transformer* p = nullptr)
            : transformer(p) {

    }

    ~file_interaction_handler();

    /**
     * Allows setting a transformer to be used with the files returned.
     * @param t the file transformer to use
     * @return this
     */
    file_interaction_handler* set_transformer(file_transformer* t) {
        transformer = t;
        return this;
    }

    /**
     * if the url ends without a slash redirect
     * @param req the request
     * @param rep the reply
     * @return true on redirect
     */
    bool redirect_if_needed(const request& req, reply& rep) const;

    /**
     * A helper method that returns the file extension.
     * @param file the file to check
     * @return the file extension
     */
    static sstring get_extension(const sstring& file);

protected:

    /**
     * read a file from the disk and return it in the replay.
     * @param file the full path to a file on the disk
     * @param req the reuest
     * @param rep the reply
     */
    future<std::unique_ptr<reply> > read(const sstring& file,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep);
    file_transformer* transformer;
};

/**
 * The directory handler get a disk path in the
 * constructor.
 * and expect a path parameter in the handle method.
 * it would concatenate the two and return the file
 * e.g. if the path is /usr/mgmt/public in the path
 * parameter is index.html
 * handle will return the content of /usr/mgmt/public/index.html
 */
class directory_handler : public file_interaction_handler {
public:

    /**
     * The directory handler map a base path and a path parameter to a file
     * @param doc_root the root directory to search the file from.
     * For example if the root is '/usr/mgmt/public' and the path parameter
     * will be '/css/style.css' the file wil be /usr/mgmt/public/css/style.css'
     */
    explicit directory_handler(const sstring& doc_root,
            file_transformer* transformer = nullptr);

    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;

private:
    sstring doc_root;
};

/**
 * The file handler get a path to a file on the disk
 * in the constructor.
 * it will always return the content of the file.
 */
class file_handler : public file_interaction_handler {
public:

    /**
     * The file handler map a file to a url
     * @param file the full path to the file on the disk
     */
    explicit file_handler(const sstring& file, file_transformer* transformer =
            nullptr, bool force_path = true)
            : file_interaction_handler(transformer), file(file), force_path(
                    force_path) {
    }

    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override;

private:
    sstring file;
    bool force_path;
};

}

#endif /* HTTP_FILE_HANDLER_HH_ */
