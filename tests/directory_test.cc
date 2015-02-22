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


#include "core/reactor.hh"
#include "core/app-template.hh"
#include "core/print.hh"
#include "core/shared_ptr.hh"

int main(int ac, char** av) {
    class lister {
        file _f;
        subscription<directory_entry> _listing;
    public:
        lister(file f)
                : _f(std::move(f))
                , _listing(_f.list_directory([this] (directory_entry de) { return report(de); })) {
        }
        future<> done() { return _listing.done(); }
    private:
        future<> report(directory_entry de) {
            print("%s\n", de.name);
            return make_ready_future<>();
        }
    };
    return app_template().run(ac, av, [] {
        return engine().open_directory(".").then([] (file f) {
            auto l = make_lw_shared<lister>(std::move(f));
            return l->done().then([l] {
                // ugly thing to keep *l alive
                engine().exit(0);
            });
        });
    });
}
