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
#include "core/reactor.hh"
#include "core/app-template.hh"

struct file_test {
    file_test(file&& f) : f(std::move(f)) {}
    file f;
    semaphore sem = { 0 };
    semaphore par = { 1000 };
};

int main(int ac, char** av) {
    app_template app;

    return app.run(ac, av, [&app] {
        static constexpr auto max = 10000;
        engine().open_file_dma("testfile.tmp", open_flags::rw | open_flags::create).then([] (file f) {
            auto ft = new file_test{std::move(f)};
            for (size_t i = 0; i < max; ++i) {
                ft->par.wait().then([ft, i] {
                    auto wbuf = allocate_aligned_buffer<unsigned char>(4096, 4096);
                    std::fill(wbuf.get(), wbuf.get() + 4096, i);
                    auto wb = wbuf.get();
                    ft->f.dma_write(i * 4096, wb, 4096).then(
                            [ft, i, wbuf = std::move(wbuf)] (size_t ret) mutable {
                        assert(ret == 4096);
                        auto rbuf = allocate_aligned_buffer<unsigned char>(4096, 4096);
                        auto rb = rbuf.get();
                        ft->f.dma_read(i * 4096, rb, 4096).then(
                                [ft, i, rbuf = std::move(rbuf), wbuf = std::move(wbuf)] (size_t ret) mutable {
                            assert(ret == 4096);
                            bool eq = std::equal(rbuf.get(), rbuf.get() + 4096, wbuf.get());
                            assert(eq);
                            ft->sem.signal(1);
                            ft->par.signal();
                        });
                    });
                });
            }
            ft->sem.wait(max).then([ft] () mutable {
                return ft->f.flush();
            }).then([ft] () mutable {
                std::cout << "done\n";
                delete ft;
                engine().exit(0);
            });
        });
    });
}


