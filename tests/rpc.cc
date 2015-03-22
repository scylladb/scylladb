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
#include <cmath>
#include "core/reactor.hh"
#include "core/app-template.hh"
#include "rpc/rpc.hh"

struct serializer {
    template<typename T>
    inline auto operator()(output_stream<char>& out, T&& v, std::enable_if_t<std::is_pod<std::remove_reference_t<T>>::value, void*> = nullptr) {
        return out.write(reinterpret_cast<const char*>(&v), sizeof(T));
    }

    template<typename T>
    inline auto operator()(output_stream<char>& out, T&& v, std::enable_if_t<!std::is_pod<std::remove_reference_t<T>>::value, void*> = nullptr) {
        return v.serialize(out);
    }

    inline auto operator()(output_stream<char>& out, sstring& v) {
        auto l = std::make_unique<size_t>(v.size());
        auto f = operator()(out, *l);
        return f.then([v = std::forward<sstring>(v), &out, l = std::move(l)] {
            return out.write(v.c_str(), v.size());
        });
    }

    template<typename T>
    inline auto operator()(input_stream<char>& in, T& v, std::enable_if_t<std::is_pod<T>::value, void*> = nullptr) {
        return in.read_exactly(sizeof(v)).then([&v] (temporary_buffer<char> buf) mutable {
            if (buf.size() == 0) {
                throw rpc::closed_error();
            }
            v = *reinterpret_cast<const T*>(buf.get());
        });
    }

    template<typename T>
    inline auto operator()(input_stream<char>& in, T& v, std::enable_if_t<!std::is_pod<T>::value, void*> = nullptr) {
        return v.deserialize(in);
    }

    inline auto operator()(input_stream<char>& in, sstring& v) {
        return in.read_exactly(sizeof(size_t)).then([] (temporary_buffer<char> buf) {
            if (buf.size() == 0) {
                throw rpc::closed_error();
            }
            return make_ready_future<size_t>(*reinterpret_cast<const size_t*>(buf.get()));
        }).then([&in] (size_t l) {
            return in.read_exactly(l);
        }).then([&v] (temporary_buffer<char> buf) mutable {
            if (buf.size() == 0) {
                throw rpc::closed_error();
            }
            v = sstring(buf.get(), buf.size());
        });
    }
};

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
                    ("port", bpo::value<uint16_t>()->default_value(10000), "RPC server port")
                    ("server", bpo::value<std::string>(), "Server address");
    std::cout << "start ";
    rpc::protocol<serializer> myrpc(serializer{});
    static std::unique_ptr<rpc::protocol<serializer>::server> server;
    static std::unique_ptr<rpc::protocol<serializer>::client> client;
    static double x = 30.0;

    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        auto test1 = myrpc.register_handler(1, [x = 0](int i) mutable { print("test1 count %d got %d\n", ++x, i); });
        auto test2 = myrpc.register_handler(2, [](int a, int b){ print("test2 got %d %d\n", a, b); return make_ready_future<int>(a+b); });
        auto test3 = myrpc.register_handler(3, [](double x){ print("test3 got %f\n", x); return sin(x); });
        auto test4 = myrpc.register_handler(4, [](){ print("test4 throw!\n"); throw std::runtime_error("exception!"); });
        auto test5 = myrpc.register_handler(5, [](){ print("test5 no wait\n"); return rpc::no_wait; });
        auto test6 = myrpc.register_handler(6, [](const rpc::client_info& info, int x){ print("test6 client %s, %d\n", inet_ntoa(info.addr.as_posix_sockaddr_in().sin_addr), x); });
        if (config.count("server")) {
            std::cout << "client" << std::endl;
            auto test7 = myrpc.make_client<long (long a, long b)>(7);

            client = std::make_unique<rpc::protocol<serializer>::client>(myrpc, ipv4_addr{config["server"].as<std::string>()});

            auto f = make_ready_future<>();
            for (auto i = 0; i < 100; i++) {
                print("iteration=%d\n", i);
                test1(*client, 5).then([] (){ print("test1 ended\n");});
                test2(*client, 1, 2).then([] (int r) { print("test2 got %d\n", r); });
                test3(*client, x).then([](double x) { print("sin=%f\n", x); });
                test4(*client).then_wrapped([](future<> f) {
                    try {
                        f.get();
                        printf("test4 your should not see this!");
                    } catch (std::runtime_error& x){
                        print("test4 %s\n", x.what());
                    }
                });
                test5(*client).then([] { print("test5 no wait ended\n"); });
                test6(*client, 1).then([] { print("test6 ended\n"); });
                f = test7(*client, 5, 6).then([] (long r) { print("test7 got %ld\n", r); });
            }
            f.finally([] {
                    engine().exit(0);
            });
        } else {
            std::cout << "server on port " << port << std::endl;
            myrpc.register_handler(7, [](long a, long b) mutable {
                auto p = make_lw_shared<promise<>>();
                auto t = make_lw_shared<timer<>>();
                print("test7 got %ld %ld\n", a, b);
                auto f = p->get_future().then([a, b, t] {
                    print("test7 calc res\n");
                    return a - b;
                });
                t->set_callback([p = std::move(p)] () mutable { p->set_value(); });
                using namespace std::chrono_literals;
                t->arm(1s);
                return f;
            });
            server = std::make_unique<rpc::protocol<serializer>::server>(myrpc, ipv4_addr{port});
        }
    });

}
