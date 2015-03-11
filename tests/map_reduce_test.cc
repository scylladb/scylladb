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

#include "core/app-template.hh"
#include "core/distributed.hh"
#include "core/future-util.hh"

struct X {
	sstring echo(sstring arg) {
		return arg;
	}
	future<> stop() { return make_ready_future<>(); }
};

future<> test_that_each_core_gets_the_arguments() {
	auto x = make_shared<distributed<X>>();
	return x->start().then([x] {
		return x->map_reduce([] (sstring msg){
			if (msg != "hello") {
				throw std::runtime_error("wrong message");
			}
		}, &X::echo, sstring("hello"));
	}).finally([x] {
		return x->stop();
	}).finally([x]{});
}

int main(int argc, char** argv) {
	app_template app;
	return app.run(argc, argv, [] {
		test_that_each_core_gets_the_arguments().then([] {
			return engine().exit(0);
		}).or_terminate();
	});
}
