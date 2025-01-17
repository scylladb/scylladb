/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/cql_test_env.hh"
#include "streaming/stream_blob.hh"
#include "message/messaging_service.hh"
#include "test/lib/log.hh"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <cstdio>
#include <sstream>
#include <cryptopp/sha.h>

future<sstring> generate_file_hash(sstring filename) {
    auto f = co_await seastar::open_file_dma(filename, seastar::open_flags::ro);
    auto in = seastar::make_file_input_stream(std::move(f));
    CryptoPP::SHA256 hash;
    unsigned char digest[CryptoPP::SHA256::DIGESTSIZE];
    std::stringstream ss;
    while (true) {
        auto buf = co_await in.read();
        if (buf.empty()) {
            break;
        }
        hash.Update((const unsigned char*)buf.get(), buf.size());
    }
    co_await in.close();
    hash.Final(digest);
    for (int i = 0; i < CryptoPP::SHA256::DIGESTSIZE; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << (int)digest[i];
    }
    co_return ss.str();
}

sstring generate_random_filename() {
    char filename[L_tmpnam];
    std::tmpnam(filename);
    return filename;
}

future<> write_random_content_to_file(const sstring& filename, size_t content_size = 1024) {
    auto f = co_await open_file_dma(filename, open_flags::rw | open_flags::create);
    auto ostream = co_await make_file_output_stream(std::move(f));
    srand(time(nullptr));
    for (size_t i = 0; i < content_size; ++i) {
        char c = rand() % 256;
        co_await ostream.write(&c, 1);
    }
    co_await ostream.close();
}

using namespace streaming;

static future<bool>
do_test_file_stream(replica::database& db, netw::messaging_service& ms, std::vector<sstring> filelist, const std::string& suffix, bool inject_error, bool unsupported_file_ops = false) {
    bool ret = false;
    bool verb_register = false;
    auto ops_id = file_stream_id::create_random_id();
    auto& global_db = db.container();
    auto& global_ms = ms.container();
    int n_retries = 0;

    do {
        try {
            if (!verb_register) {
                co_await smp::invoke_on_all([&] {
                    return global_ms.local().register_stream_blob([&](const rpc::client_info& cinfo, streaming::stream_blob_meta meta, rpc::source<streaming::stream_blob_cmd_data> source) {
                        auto from = netw::messaging_service::get_source(cinfo).addr;
                        auto sink = global_ms.local().make_sink_for_stream_blob(source);
                        (void)stream_blob_handler(global_db.local(), global_ms.local(), from, meta, sink, source, [&suffix](auto&, const streaming::stream_blob_meta& meta) -> future<output_result> {
                            auto path = meta.filename + suffix;
                            auto f = co_await open_file_dma(path, open_flags::wo|open_flags::create);
                            auto out = co_await make_file_output_stream(std::move(f));
                            co_return output_result{
                                [path = std::move(path)](store_result res) -> future<> {
                                    if (res != store_result::ok) {
                                        co_await remove_file(path);
                                    }
                                },
                                std::move(out)
                            };
                        }, inject_error).handle_exception([sink, source, ms = global_ms.local().shared_from_this()] (std::exception_ptr eptr) {
                            testlog.warn("Failed to run stream blob handler: {}", eptr);
                        });
                        return make_ready_future<rpc::sink<streaming::stream_blob_cmd_data>>(sink);
                    });
                });
            }
            verb_register = true;
            auto table = table_id::create_random_id();
            auto files = std::list<stream_blob_info>();
            auto hostid = db.get_token_metadata().get_my_id();
            seastar::shard_id dst_shard_id = 0;
            co_await mark_tablet_stream_start(ops_id);
            auto targets = std::vector<node_and_shard>{node_and_shard{hostid, dst_shard_id}};
            for (const auto& filename : filelist) {
                auto fops = file_ops::stream_sstables;
                fops = unsupported_file_ops ? file_ops(0xff55) : fops;
                auto file = co_await open_file_dma(filename, open_flags::ro);
                auto& info = files.emplace_back();
                info.filename = filename;
                info.fops = fops;
                info.source = [file = std::move(file)](const file_input_stream_options& foptions) mutable -> future<input_stream<char>> {
                    co_return make_file_input_stream(std::move(file), foptions);
                };
            }
            auto host2ip = [&global_db] (locator::host_id id) -> future<gms::inet_address> {
                co_return global_db.local().get_token_metadata().get_topology().my_address();
            };
            size_t stream_bytes = co_await tablet_stream_files(ms, std::move(files), targets, table, ops_id, host2ip, service::null_topology_guard, inject_error);
            co_await mark_tablet_stream_done(ops_id);
            testlog.info("do_test_file_stream[{}] status=ok files={} stream_bytes={}", ops_id, filelist.size(), stream_bytes);
            ret = true;
        } catch (seastar::rpc::stream_closed&) {
            testlog.warn("do_test_file_stream[{}] status=fail error={} retry={}", ops_id, std::current_exception(), n_retries++);
            if (n_retries < 3) {
                testlog.info("Retrying send");
                continue;
            }
        } catch (...) {
            testlog.warn("do_test_file_stream[{}] status=fail error={}", ops_id, std::current_exception());
        }
    } while (false);

    if (verb_register) {
        co_await smp::invoke_on_all([&global_ms] {
            return global_ms.local().unregister_stream_blob();
        });
    }
    co_return ret;
}

void do_test_file_stream(bool inject_error) {
    cql_test_config cfg;
    cfg.ms_listen = true;
    std::vector<sstring> files;
    std::vector<sstring> files_rx;
    std::vector<sstring> hash_tx;
    std::vector<sstring> hash_rx;
    size_t nr_files = 10;
    size_t file_size = 0;
    static const std::string suffix = ".rx";

    while (files.size() != nr_files) {
        auto name = generate_random_filename();
        files.push_back(name);
        files_rx.push_back(name + suffix);
    }

    size_t base_size = 1024;

#ifdef SEASTAR_DEBUG
    base_size = 1;
#endif

    for (auto& file : files) {
        if (file_size == 0) {
            file_size = 1 * 1024 * base_size;
        } else {
            file_size = (rand() % 10) * 1024 * base_size + rand() % base_size;
        }
        file_size = std::max(size_t(1), file_size);
        testlog.info("file_tx={} file_size={}", file, file_size);
        write_random_content_to_file(file, file_size).get();
    }

    do_with_cql_env_thread([files, inject_error] (auto& e) {
        do_test_file_stream(e.local_db(), e.get_messaging_service().local(), files, suffix, inject_error).get();
    }, cfg).get();

    bool cleanup = true;
    for (auto& file : files) {
        auto hash = generate_file_hash(file).get();
        testlog.info("file_tx={}    hash={}", file, hash);
        hash_tx.push_back(hash);
        if (cleanup) {
            seastar::remove_file(file).get();
        }
    }
    for (auto& file : files_rx) {
        sstring hash = "SKIP";
        try {
            hash = generate_file_hash(file).get();
            if (cleanup) {
                seastar::remove_file(file).get();
            }
        } catch (...) {
            if (!inject_error) {
                throw;
            }
        }
        hash_rx.push_back(hash);
        testlog.info("file_rx={} hash={}", file, hash);
    }

    BOOST_REQUIRE(hash_tx.size() == hash_rx.size());
    for (size_t i = 0; i < hash_tx.size(); i++) {
        testlog.info("Check tx_hash={} rx_hash={}", hash_tx[i], hash_rx[i]);
        if (inject_error) {
            BOOST_REQUIRE(hash_tx[i] == hash_rx[i] || sstring("SKIP") == hash_rx[i]);
        } else {
            BOOST_REQUIRE(hash_tx[i] == hash_rx[i]);
        }
    }
}

void do_test_unsupported_file_ops() {
    bool inject_error = false;
    bool unsupported_file_ops = true;

    cql_test_config cfg;
    cfg.ms_listen = true;
    std::vector<sstring> files;
    size_t nr_files = 2;
    size_t file_size = 1024;

    while (files.size() != nr_files) {
        auto name = generate_random_filename();
        files.push_back(name);
    }

    for (auto& file : files) {
        testlog.info("file_tx={} file_size={}", file, file_size);
        write_random_content_to_file(file, file_size).get();
    }

    do_with_cql_env_thread([files, inject_error, unsupported_file_ops] (auto& e) {
        auto ok = do_test_file_stream(e.local_db(), e.get_messaging_service().local(), files, "", inject_error, unsupported_file_ops).get();
        // Stream with a unsupported file ops should fail
        BOOST_REQUIRE(ok == false);
    }, cfg).get();

    for (auto& file : files) {
        seastar::remove_file(file).get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_file_stream) {
    bool inject_error = false;
    do_test_file_stream(inject_error);
}

SEASTAR_THREAD_TEST_CASE(test_file_stream_inject_error) {
    bool inject_error = true;
    do_test_file_stream(inject_error);
}

SEASTAR_THREAD_TEST_CASE(test_unsupported_file_ops) {
    do_test_unsupported_file_ops();
}
