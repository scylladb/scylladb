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
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

// Demonstration of file_input_stream.  Don't expect stellar performance
// since no read-ahead or caching is done yet.

#include "core/fstream.hh"
#include "core/app-template.hh"
#include "core/shared_ptr.hh"
#include "core/reactor.hh"
#include <algorithm>

struct reader {
    reader(file f) : is(make_file_input_stream(
            make_lw_shared<file>(std::move(f)), 0, 4096)) {}
    input_stream<char> is;
    size_t count = 0;

    // for input_stream::consume():
    template <typename Done>
    void operator()(temporary_buffer<char> data, Done&& done) {
        if (data.empty()) {
            done(std::move(data));
        } else {
            count += std::count(data.begin(), data.end(), '\n');
            // FIXME: last line without \n?
        }
    }
};

int main(int ac, char** av) {
    app_template app;
    namespace bpo = boost::program_options;
    app.add_positional_options({
        { "file", bpo::value<std::string>(), "File to process", 1 },
    });
    app.run(ac, av, [&app] {
        auto fname = app.configuration()["file"].as<std::string>();
        engine().open_file_dma(fname, open_flags::ro).then([] (file f) {
            auto r = make_shared<reader>(std::move(f));
            r->is.consume(*r).then([r] {
               print("%d lines\n", r->count);
            });
        }).then_wrapped([] (future<> f) {
            try {
                f.get();
                engine().exit(0);
            } catch (std::exception& ex) {
                std::cout << ex.what() << "\n";
                engine().exit(1);
            } catch (...) {
                std::cout << "unknown exception\n";
                engine().exit(0);
            }
        });
    });
}

