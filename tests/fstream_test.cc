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

#include <algorithm>
#include <iostream>
#include "core/reactor.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"
#include "core/app-template.hh"

struct writer {
    output_stream<char> out;
    writer(file f) : out(make_file_output_stream(
            make_lw_shared<file>(std::move(f)))) {}
};

struct reader {
    input_stream<char> in;
    reader(file f) : in(make_file_input_stream(
            make_lw_shared<file>(std::move(f)))) {}
};

int main(int ac, char** av) {
    app_template app;

    return app.run(ac, av, [&app] {
        engine().open_file_dma("testfile.tmp",
                open_flags::rw | open_flags::create | open_flags::truncate).then([] (file f) {
            auto w = make_shared<writer>(std::move(f));
            auto buf = static_cast<char*>(::malloc(4096));
            memset(buf, 0, 4096);
            buf[0] = '[';
            buf[1] = 'A';
            buf[4095] = ']';
            w->out.write(buf, 4096).then([buf, w] {
                ::free(buf);
                return w->out.flush();
            }).then([w] {
                auto buf = static_cast<char*>(::malloc(8192));
                memset(buf, 0, 8192);
                buf[0] = '[';
                buf[1] = 'B';
                buf[8191] = ']';
                return w->out.write(buf, 8192).then([buf, w] {
                    ::free(buf);
                    return w->out.flush();
                });
            }).then([] {
                return engine().open_file_dma("testfile.tmp", open_flags::ro);
            }).then([] (file f) {
                /*  file content after running the above:
                 * 00000000  5b 41 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |[A..............|
                 * 00000010  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
                 * *
                 * 00000ff0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 5d  |...............]|
                 * 00001000  5b 42 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |[B..............|
                 * 00001010  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
                 * *
                 * 00002ff0  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 5d  |...............]|
                 * 00003000
                 */
                auto r = make_shared<reader>(std::move(f));
                return r->in.read_exactly(4096 + 8192).then([r] (temporary_buffer<char> buf) {
                    auto p = buf.get();
                    auto first_test = p[0] == '[' && p[1] == 'A' && p[4095] == ']';
                    auto second_test = p[4096] == '[' && p[4096 + 1] == 'B' && p[4096 + 8191] == ']';
                    auto ret = first_test && second_test ? 0 : -1;
                    return make_ready_future<int>(ret);
                });
            }).then([] (int exit_status) {
                std::cout << "done: " << exit_status << "\n";
                engine().exit(exit_status);
            });
        });
    });
}
