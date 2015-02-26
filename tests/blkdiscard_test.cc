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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <algorithm>
#include "core/app-template.hh"
#include "core/future-util.hh"

namespace bpo = boost::program_options;

struct file_test {
    file_test(file&& f) : f(std::move(f)) {}
    file f;
    semaphore sem = { 0 };
};

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("dev", bpo::value<std::string>(), "e.g. --dev /dev/sdb")
        ;

    return app.run(ac, av, [&app] {
        static constexpr auto max = 10000;
        auto&& config = app.configuration();
        auto filepath = config["dev"].as<std::string>();

        engine().open_file_dma(filepath, open_flags::rw | open_flags::create).then([] (file f) {
            auto ft = new file_test{std::move(f)};

            ft->f.stat().then([ft] (struct stat st) mutable {
                assert(S_ISBLK(st.st_mode));
                auto offset = 0;
                auto length = max * 4096;
                ft->f.discard(offset, length).then([ft] () mutable {
                    ft->sem.signal();
                });
            });

            ft->sem.wait().then([ft] () mutable {
                return ft->f.flush();
            }).then([ft] () mutable {
                std::cout << "done\n";
                delete ft;
                engine().exit(0);
            });
        });
    });
}
