/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/gcp/object_storage.hh"
#include "utils/gcp/gcp_credentials.hh"

#include <ranges>
#include <map>
#include <sstream>
#include <unordered_set>
#include <filesystem>
#include <boost/test/unit_test.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/file.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/http/client.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/util/log.hh>

#include "ent/encryption/symmetric_key.hh"
#include "ent/encryption/encrypted_file_impl.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/tmpdir.hh"
#include "test/lib/gcs_fixture.hh"
#include "db/object_storage_endpoint_param.hh"
#include "sstables/object_storage_client.hh"
#include "test/lib/test_utils.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "utils/io-wrappers.hh"
#include "utils/http.hh"
#include "utils/error_injection.hh"
#include "utils/rjson.hh"

#include <seastar/core/metrics_api.hh>
#include <seastar/testing/test_fixture.hh>

using namespace std::string_view_literals;
using namespace std::chrono_literals;
using namespace utils::gcp;

/*
    Simple test of GCP object storage provider. Uses either real or local, fake, endpoint.

    Note: the above text blobs are service account credentials, including private keys. 
    _Never_ give any real priviledges to these accounts, as we are obviously exposing them here.

    User1 is assumed to have permissions to read/write the bucket
    User2 is assumed to only have permissions to read the bucket, but permission to 
    impersonate User1.

    Note: fake gcp storage does not have any credentials or permissions, so
    for testing with such, leave them unset to skip those tests.

    This test is parameterized with env vars:
    * ENABLE_GCP_STORAGE_TEST - set to non-zero (1/true) to run
    * GCP_STORAGE_ENDPOINT - set to endpoint host. default is https://storage.googleapis.com
    * GCP_STORAGE_PROJECT - project in which to create bucket (if not specified)
    * GCP_STORAGE_USER_1_CREDENTIALS - set to credentials file for user1
    * GCP_STORAGE_USER_2_CREDENTIALS - set to credentials file for user2
    * GCP_STORAGE_BUCKET - set to test bucket
*/

static auto check_gcp_storage_test_enabled() {
    return tests::check_run_test_decorator("ENABLE_GCP_STORAGE_TEST", true);
}

static future<> write_object_of_size(data_sink& sink
                                      , size_t dest_size
                                      , std::vector<temporary_buffer<char>>* buffer_store = nullptr
                                      , std::optional<size_t> specific_buffer_size = std::nullopt
                                      , bool can_share = true
                                    ) 
{
    size_t done = 0;
    while (done < dest_size) {
        auto rem = dest_size - done;
        auto len = std::min(rem, specific_buffer_size.value_or(tests::random::get_int(size_t(1), size_t(4*1024*1024))));
        auto rnd = tests::random::get_bytes(len);
        temporary_buffer<char> buf(reinterpret_cast<char*>(rnd.data()), rnd.size());
        if (buffer_store) {
            // maybe don't use share. if sink is encrypted, it will do in-place transforms
            buffer_store->emplace_back(can_share 
                ? buf.share()
                : temporary_buffer<char>(buf.get(), len)
            );
        }
        co_await sink.put(std::move(buf));
        done += len;
    }
}

static future<> create_object_of_size(storage::client& c
                                      , std::string_view bucket
                                      , std::string_view name
                                      , size_t dest_size
                                      , std::vector<temporary_buffer<char>>* buffer_store = nullptr
                                      , std::optional<size_t> specific_buffer_size = std::nullopt
                                    ) 
{
    auto sink = c.create_upload_sink(bucket, name);
    co_await write_object_of_size(sink, dest_size, buffer_store, specific_buffer_size);
    co_await sink.flush();
    co_await sink.close();
}

static future<> compare_stream_data(seastar::input_stream<char>& is1, seastar::input_stream<char>& is2, size_t total) {
    uint64_t read = 0;
    while (!is1.eof()) {
        auto buf = co_await is1.read();
        if (buf.empty()) {
            break;
        }
        auto buf2 = co_await is2.read_exactly(buf.size());
        BOOST_REQUIRE_EQUAL(buf, buf2);
        read += buf.size();
    }

    BOOST_REQUIRE_EQUAL(read, total);
}

static size_t total_size(const std::vector<temporary_buffer<char>>& bufs) {
    auto total = std::accumulate(bufs.begin(), bufs.end(), size_t{}, [](size_t s, auto& buf) {
        return s + buf.size();
    });
    return total;
}

static std::tuple<seastar::input_stream<char>, size_t> stream_from_buffers(std::vector<temporary_buffer<char>>&& bufs) {
    auto total = total_size(bufs);
    auto is = seastar::input_stream<char>(create_memory_source(std::move(bufs)));
    return std::make_tuple(std::move(is), total);
}

static future<> compare_object_data(const local_gcs_wrapper& env, std::string_view object_name, std::vector<temporary_buffer<char>>&& bufs) {
    auto& c = env.client();
    auto source = c.create_download_source(env.bucket, object_name);
    auto is1 = seastar::input_stream<char>(std::move(source));
    auto [is2, total] = stream_from_buffers(std::move(bufs));

    co_await compare_stream_data(is1, is2, total);
}

using namespace std::string_literals;
static constexpr auto prefix = "bork/ninja/"s;

// #28398 include a prefix in all names. 
static std::string make_name() {
    return fmt::format("{}{}", prefix, utils::UUID_gen::get_time_UUID());
}

using fault_counts = std::map<sstring, unsigned>;

// Talk to the control path of the validator sitting in front of the mock, and return
// the faults still armed afterwards. Only reachable when the validator is running -
// see local_gcs_wrapper::validating_uploads().
// See test/pylib/gcs_upload_validator.py for the fault names.
static future<fault_counts> control(const local_gcs_wrapper& env, std::vector<std::pair<sstring, sstring>> params) {
    auto url = utils::http::parse_simple_url(env.endpoint);
    auto cln = seastar::http::client(socket_address(net::inet_address(url.host), url.port));

    fault_counts left;
    std::exception_ptr ex;
    try {
        auto req = seastar::http::request::make("PUT", url.host, "/__inject");
        req._headers["Content-Length"] = "0";
        for (auto& [name, value] : params) {
            req.set_query_param(name, value);
        }
        co_await cln.make_request(std::move(req), [&left](const seastar::http::reply&, seastar::input_stream<char>&& in) -> future<> {
            auto body = std::move(in);
            auto text = co_await util::read_entire_stream_contiguous(body);
            auto counts = rjson::parse(std::string_view(text));
            for (auto it = counts.MemberBegin(); it != counts.MemberEnd(); ++it) {
                left.emplace(sstring(rjson::to_string_view(it->name)), it->value.GetUint());
            }
        }, seastar::http::reply::status_type::ok);
    } catch (...) {
        ex = std::current_exception();
    }
    co_await cln.close();
    if (ex) {
        std::rethrow_exception(ex);
    }
    co_return left;
}

static future<> inject(const local_gcs_wrapper& env, std::string_view fault, unsigned count) {
    co_await control(env, {{sstring(fault), std::to_string(count)}});
}

// What is still armed. A fault the test expected to fire must be back to zero: an
// injection that never took effect otherwise leaves the test green over an upload it
// did not fault at all.
static future<fault_counts> faults_left(const local_gcs_wrapper& env) {
    return control(env, {});
}

// Faults are process-global in the validator and it serves the whole suite, so a test
// that arms more than it consumes has to take the rest back down.
static future<> disarm_faults(const local_gcs_wrapper& env) {
    co_await control(env, {{"reset", "1"}});
}

// The faults are armed over the validator's control path, so there is nothing to
// exercise when uploads are not going through it: GCP_STORAGE_SKIP_UPLOAD_VALIDATOR
// is set, or the test is pointed at a real GCS endpoint.
static bool needs_upload_validator(const local_gcs_wrapper& env) {
    if (env.validating_uploads()) {
        return true;
    }
    BOOST_TEST_MESSAGE("Skipping: uploads are not going through the validator");
    return false;
}

static future<> test_read_write_helper(const local_gcs_wrapper& env, size_t dest_size, std::optional<size_t> specific_buffer_size = std::nullopt) {
    auto& c = env.client();
    auto name = make_name();
    std::vector<temporary_buffer<char>> written;

    // ensure we remove the object
    env.objects_to_delete.emplace_back(name);
    co_await create_object_of_size(c, env.bucket, name, dest_size, &written, specific_buffer_size);
    co_await compare_object_data(env, name, std::move(written));
}

// Sums an object_storage metric across the metrics registered for the GCS backend.
static uint64_t gs_metric(std::string_view name) {
    auto all_metrics = seastar::metrics::impl::get_values();
    const auto& all_metadata = *all_metrics->metadata;
    auto family_name = seastar::sstring(fmt::format("object_storage_{}", name));
    auto family = std::ranges::find_if(all_metadata, [&](const auto& x) { return x.mf.name == family_name; });
    BOOST_REQUIRE(family != all_metadata.end());
    const auto& values = all_metrics->values[std::distance(all_metadata.begin(), family)];
    uint64_t total = 0;
    for (size_t i = 0; i < family->metrics.size(); ++i) {
        const auto& labels = family->metrics[i].labels();
        auto type = labels.find("type");
        BOOST_REQUIRE(type != labels.end());
        if (type->second.value() == "gs") {
            total += values[i].ui();
        }
    }
    return total;
}

BOOST_AUTO_TEST_SUITE(gcs_tests, *seastar::testing::async_fixture<gcs_fixture>())

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_small_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 8*4);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_large_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 32*1024*1024 + 357 + 1022*67);
}

// SCYLLADB-3889: a zero-length object must finalize the resumable upload with
// "bytes */0". Emitting "bytes 0-0/0" claims a byte that cannot exist in a
// zero-byte object and real GCS rejects it with 400. Zero-length objects are
// not exotic here -- every object-storage sstable starts by writing an empty
// refs/nodes/<host_id>/<gen> marker, so this breaks every memtable flush.
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_empty_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 0);
}

// SCYLLADB-3889: same malformed range, reached from the other direction. The
// sink only uploads whole multiples of 256k while the stream is open, so an
// object whose size is an exact multiple of the chunk size leaves nothing
// buffered at close() and finalizes with a zero-length chunk at a non-zero
// offset ("bytes N-N/N").
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_chunk_aligned_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 8*1024*1024, 256*1024);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_small_object_64kbuf, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 618480, 64*1024);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_create_large_object_64kbuf, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await test_read_write_helper(*this, 32*1024*1024 + 357 + 1022*67, 64*1024);
}

// A 308 with no Range header does not describe the chunk, so the client asks the session
// where it stands; here it holds nothing, and the chunk goes again. Treating the reply as
// an acknowledgement instead skips those bytes and the object ends up short of what was
// written.
//
// https://scylladb.atlassian.net/browse/SCYLLADB-4027
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_resend_unacknowledged_chunk, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    // Big enough that the flush in create_object_of_size() writes a whole 256k chunk
    // and leaves a tail for the final one - only a non-final chunk can be answered
    // with a 308.
    constexpr size_t dest_size = 300*1024;

    if (!needs_upload_validator(*this)) {
        co_return;
    }

    co_await inject(*this, "unacknowledged_chunks", 1);

    auto name = make_name();
    objects_to_delete.emplace_back(name);

    std::vector<temporary_buffer<char>> written;
    co_await create_object_of_size(client(), bucket, name, dest_size, &written);
    co_await compare_object_data(*this, name, std::move(written));

    // without this the upload simply succeeds and the comparison passes, so the test
    // would report nothing whether the 308 was ever answered or not
    auto left = co_await faults_left(*this);
    BOOST_REQUIRE_EQUAL(left["unacknowledged_chunks"], 0u);
}

// Cancelling a failed upload is best effort, and its reply - 499 when it works, an
// error when it does not - must never take the place of the reason the upload failed.
//
// https://scylladb.atlassian.net/browse/SCYLLADB-4027
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_failed_upload_error_survives_cancel, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    if (!needs_upload_validator(*this)) {
        co_return;
    }

    auto name = make_name();
    objects_to_delete.emplace_back(name);

    co_await inject(*this, "failed_chunks", 1);
    // more cancel failures than the sink can issue, so none of them is the last word
    co_await inject(*this, "failed_cancels", 8);

    std::string what;
    try {
        co_await create_object_of_size(client(), bucket, name, 300*1024);
        BOOST_FAIL("upload of a rejected chunk should have thrown");
    } catch (const storage_io_error& e) {
        what = e.what();
    }

    auto left = co_await faults_left(*this);
    co_await disarm_faults(*this);

    BOOST_TEST_MESSAGE(fmt::format("upload failed with: {}", what));
    // the chunk's own 400, not 403 from the cancel and not 499 from a cancel that worked
    BOOST_REQUIRE(what.contains("400"));
    BOOST_REQUIRE(!what.contains("403"));
    BOOST_REQUIRE(!what.contains("499"));
    BOOST_REQUIRE_EQUAL(left["failed_chunks"], 0u);
}

// The other half of the same rule: 499 is how GCS reports a cancel that worked, so the
// cancel must be treated as done rather than failed. Only the validator can produce it -
// fake-gcs-server answers the DELETE with a 2xx.
//
// The caller cannot see the difference: close() swallows whatever the cancel raises, so
// the chunk's 400 comes out either way. What does change is the log - a recognized 499
// reaches "Upload ... removed", an unrecognized one throws and is reported as a cancel
// that failed - so that is what this asserts. Verified to fail with the 499 handling
// reverted: the debug line is absent and the warning is present.
//
// https://scylladb.atlassian.net/browse/SCYLLADB-4027
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_cancelled_upload_is_not_an_error, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    if (!needs_upload_validator(*this)) {
        co_return;
    }

    auto name = make_name();
    objects_to_delete.emplace_back(name);

    co_await inject(*this, "failed_chunks", 1);
    co_await inject(*this, "cancelled_uploads", 1);

    // the "removed" line is a debug one
    auto prev_level = logging::logger_registry().get_logger_level("gcp_storage");
    logging::logger_registry().set_logger_level("gcp_storage", logging::log_level::debug);

    std::ostringstream captured;
    seastar::logger::set_ostream(captured);

    std::string what;
    try {
        co_await create_object_of_size(client(), bucket, name, 300*1024);
    } catch (const storage_io_error& e) {
        what = e.what();
    }

    seastar::logger::set_ostream(std::cerr);
    logging::logger_registry().set_logger_level("gcp_storage", prev_level);

    auto log = captured.str();
    auto left = co_await faults_left(*this);
    co_await disarm_faults(*this);

    BOOST_TEST_MESSAGE(fmt::format("upload failed with: {}", what));
    BOOST_REQUIRE(!what.empty());
    // the chunk's own 400 still reaches the caller, not the 499 from the cancel
    BOOST_REQUIRE(what.contains("400"));
    BOOST_REQUIRE(!what.contains("499"));
    // the cancel was accepted...
    BOOST_REQUIRE(log.contains(fmt::format("Upload of {}:{} removed", bucket, name)));
    // ...and never reported as one that failed
    BOOST_REQUIRE(!log.contains("Could not cancel upload"));
    BOOST_REQUIRE_EQUAL(left["failed_chunks"], 0u);
    BOOST_REQUIRE_EQUAL(left["cancelled_uploads"], 0u);
}

// A server that keeps reporting no progress must fail the upload rather than spin. The
// bound covers the whole retry loop, so it also catches the partial-progress path, where
// a non-final chunk rounded down to a 256k boundary can advance by nothing.
//
// https://scylladb.atlassian.net/browse/SCYLLADB-4027
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_stalled_upload_gives_up, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    if (!needs_upload_validator(*this)) {
        co_return;
    }

    auto name = make_name();
    objects_to_delete.emplace_back(name);

    // more than the loop will attempt, so it is the bound that ends the upload
    co_await inject(*this, "unacknowledged_chunks", 12);

    std::string what;
    try {
        co_await create_object_of_size(client(), bucket, name, 300*1024);
    } catch (const std::exception& e) {
        what = e.what();
    }

    co_await disarm_faults(*this);

    BOOST_TEST_MESSAGE(fmt::format("upload failed with: {}", what));
    BOOST_REQUIRE(what.contains("made no progress at offset 0"));
}

// A session reporting fewer bytes than the chunk already sent has lost data the sink no
// longer holds - maybe_do_upload() released those buffers - so no amount of resending can
// make the upload contiguous again. Fail at once, naming both numbers.
//
// https://scylladb.atlassian.net/browse/SCYLLADB-4027
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_upload_fails_when_session_falls_behind, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    if (!needs_upload_validator(*this)) {
        co_return;
    }

    auto name = make_name();
    objects_to_delete.emplace_back(name);

    // Uploads are serialized, and a chunk goes out once 8M has accumulated, so a 9M
    // object sends one at 0 and one at 8M. Drop the second and answer the query that
    // follows with a session that is behind it.
    constexpr size_t second_chunk = 8*1024*1024;
    co_await control(*this, {{"unacknowledged_at", std::to_string(second_chunk)}});
    co_await control(*this, {{"status_answers", std::to_string(second_chunk / 2)}});

    std::string what;
    try {
        co_await create_object_of_size(client(), bucket, name, 9*1024*1024, nullptr, 256*1024);
    } catch (const std::exception& e) {
        what = e.what();
    }

    auto left = co_await faults_left(*this);
    co_await disarm_faults(*this);

    BOOST_TEST_MESSAGE(fmt::format("upload failed with: {}", what));
    BOOST_REQUIRE(what.contains(fmt::format("session holds {} bytes", second_chunk / 2)));
    BOOST_REQUIRE(what.contains(fmt::format("behind the chunk at offset {}", second_chunk)));
    // both the drop and the scripted answer were used
    BOOST_REQUIRE_EQUAL(left["unacknowledged_at"], 0u);
    BOOST_REQUIRE_EQUAL(left["status_answers"], 0u);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_list_objects, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    std::unordered_map<std::string, uint64_t> names;
    for (size_t i = 0; i < 50; ++i) {
        auto name = make_name();
        auto size = tests::random::get_int(size_t(1), size_t(2*1024*1024));
        env.objects_to_delete.emplace_back(name);
        co_await create_object_of_size(c, env.bucket, name, size);
        names.emplace(name, size);
    }


    for (size_t page_size = 4;; page_size *= 2) {
        utils::gcp::storage::bucket_paging paging{page_size};
        size_t n_found = 0;

        for (;;) {
            auto infos = co_await c.list_objects(env.bucket, "", paging);

            for (auto& info : infos) {
                auto i = names.find(info.name);
                if (i != names.end()) {
                    BOOST_REQUIRE_EQUAL(info.size, i->second);
                    ++n_found;
                }
            }
            if (infos.empty()) {
                break;
            }
        }

        BOOST_REQUIRE_EQUAL(n_found, names.size());

        if (page_size >= names.size()) {
            break;
        }
    }
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_delete_object, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    auto name = make_name();
    env.objects_to_delete.emplace_back(name);
    co_await create_object_of_size(c, env.bucket, name, 128);
    {
        // validate object was created.
        auto infos = co_await c.list_objects(env.bucket, name);
        BOOST_REQUIRE(std::find_if(infos.begin(), infos.end(), [&](auto& info) {
            return info.name == name;
        }) != infos.end());
    }

    co_await c.delete_object(env.bucket, name);

    auto infos = co_await c.list_objects(env.bucket, name);
    BOOST_REQUIRE(infos.empty());
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_skip_read, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    auto name = make_name();
    std::vector<temporary_buffer<char>> bufs;
    constexpr size_t file_size = 12*1024*1024 + 384*7 + 31;

    co_await create_object_of_size(c, env.bucket, name, 12*1024*1024, &bufs);
    for (size_t i = 0; i < 20; ++i) {
        auto source = c.create_download_source(env.bucket, name);
        auto copy = bufs | std::views::transform([](auto& buf) { return buf.share(); });
        auto is1 = seastar::input_stream<char>(std::move(source));
        auto is2 = seastar::input_stream<char>(create_memory_source(std::vector<temporary_buffer<char>>(copy.begin(), copy.end())));

        std::exception_ptr p;
        try {

            size_t pos = 0; 
            while (pos < file_size) {
                auto rem = file_size - pos;
                auto skip = tests::random::get_int(std::min(rem, size_t(100)), rem);
                auto read = std::min(rem - skip, size_t(tests::random::get_int(31, 2048)));

                // Both streams may throw "premature end of stream" when
                // skipping past EOF. Verify they agree and exit the loop.
                auto is_premature_eof = [] (seastar::future<>&& f) {
                    try {
                        f.get();
                        return false;
                    } catch (const std::runtime_error& e) {
                        if (std::string_view(e.what()).find("premature end of stream") != std::string_view::npos) {
                            return true;
                        }
                        throw;
                    }
                };
                bool is1_eof = co_await is1.skip(skip).then_wrapped(is_premature_eof);
                bool is2_eof = co_await is2.skip(skip).then_wrapped(is_premature_eof);
                BOOST_REQUIRE_EQUAL(is1_eof, is2_eof);
                if (is1_eof) {
                    break;
                }

                auto b1 = co_await is1.read_exactly(read);
                auto b2 = co_await is2.read_exactly(read);

                BOOST_REQUIRE_EQUAL(b1.size(), b2.size());
                if (b1 != b2) {
                    BOOST_TEST_MESSAGE(fmt::format("diff at {} ({} bytes)", pos + skip, read));
                    auto i = std::mismatch(b1.begin(), b1.end(), b2.begin());
                    BOOST_TEST_MESSAGE(fmt::format("offset {}", std::distance(b1.begin(), i.first)));
                }
                BOOST_REQUIRE_EQUAL(b1, b2);
                pos += (skip + read);
            }
        } catch (...) {
            p = std::current_exception();
        }
        co_await is1.close();
        co_await is2.close();
        if (p) {
            std::rethrow_exception(p);
        }
    }
}

SEASTAR_FIXTURE_TEST_CASE(test_merge_objects, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    std::vector<temporary_buffer<char>> bufs;
    std::vector<std::string> names;

    size_t total = 0; 
    for (size_t i = 0; i < 32; ++i) {
        auto name = make_name();
        auto size = tests::random::get_int(size_t(1), size_t(2*1024*1024));
        env.objects_to_delete.emplace_back(name);
        co_await create_object_of_size(c, env.bucket, name, size, &bufs);
        names.emplace_back(name);
        total += size;
    }

    auto name = make_name();
    env.objects_to_delete.emplace_back(name);

    auto info = co_await c.merge_objects(env.bucket, name, names);

    BOOST_REQUIRE_EQUAL(info.name, name);
    BOOST_REQUIRE_EQUAL(info.size, total);

    co_await compare_object_data(env, name, std::move(bufs));
}


SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_large_object_iov, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& c = client();
    auto name = make_name();
    std::vector<temporary_buffer<char>> written;

    auto dest_size = 32*1024*1024 + 357 + 1022*67;

    // ensure we remove the object
    objects_to_delete.emplace_back(name);
    co_await create_object_of_size(c, bucket, name, dest_size, &written);
    auto source = c.create_download_source(bucket, name);
    auto f = create_file_for_seekable_source(std::move(source));
    auto [is2, total] = stream_from_buffers(std::move(std::move(written)));
    std::vector<temporary_buffer<char>> bufs;
    std::vector<iovec> vecs;
    for (size_t i = 0; i < total; ) {
        auto n = std::min(total - i, size_t(8192));
        bufs.emplace_back(n);
        vecs.emplace_back(iovec{ .iov_base = bufs.back().get_write(), .iov_len = n });
        i += n;
    }
    auto read = co_await f.dma_read(0, std::move(vecs));
    BOOST_REQUIRE_EQUAL(read, total);
    auto [is1, total2] = stream_from_buffers(std::move(std::move(bufs)));
    BOOST_REQUIRE_EQUAL(total, total2);
    co_await compare_stream_data(is1, is2, total);

}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_in_parallel, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& c = client();
    auto name = make_name();
    std::vector<temporary_buffer<char>> written;

    // just a random number reasonably large size that is not a
    // even sector size or anything. 32mb + change
    auto dest_size = 32*1024*1024 + 357 + 1022*67 + 23324;

    // ensure we remove the object
    objects_to_delete.emplace_back(name);
    co_await create_object_of_size(c, bucket, name, dest_size, &written);
    auto source = c.create_download_source(bucket, name);
    auto f = create_file_for_seekable_source(std::move(source));
    auto total = total_size(written);

    utils::get_local_injector().enable("gcp_storage_stall_requests", true);
    auto def = defer([]() noexcept {
        utils::get_local_injector().disable("gcp_storage_stall_requests");
    });

    auto read_file = [&]() -> future<std::tuple<seastar::input_stream<char>, size_t>> {
        std::vector<temporary_buffer<char>> bufs;
        for (size_t i = 0; i < total; ) {
            auto n = std::min(total - i, size_t(64*1024));
            auto buf = co_await f.dma_read_bulk<char>(i, n);
            i += buf.size();
            bufs.emplace_back(std::move(buf));
        }
        co_return stream_from_buffers(std::move(std::move(bufs)));
    };

    auto fut = read_file();
    BOOST_REQUIRE(!fut.available());
    auto [is1, t1] = co_await read_file();
    auto [is2, t2] = co_await std::move(fut);

    co_await f.close();

    auto written2 = written | std::views::transform([](auto& buf) {
        return buf.share();
    }) | std::ranges::to<std::vector>();

    {
        BOOST_TEST_MESSAGE("Check stream 1");
        auto [is_ref, total] = stream_from_buffers(std::move(written));
        BOOST_REQUIRE_EQUAL(t1, total);
        co_await compare_stream_data(is1, is_ref, total);
    }
    {
        BOOST_TEST_MESSAGE("Check stream 2");
        auto [is_ref, total] = stream_from_buffers(std::move(written2));
        BOOST_REQUIRE_EQUAL(t2, total);
        co_await compare_stream_data(is2, is_ref, total);
    }
}

static future<> chunked_read_helper(const local_gcs_wrapper& w, bool do_ranged, bool encrypt, bool do_additional_skip) {
    auto& c = w.client();
    auto name = make_name();
    std::vector<temporary_buffer<char>> written;

    auto dest_size = 56*1024*1024 + 357 + 1022*67;

    // ensure we remove the object
    w.objects_to_delete.emplace_back(name);

    auto k = make_shared<encryption::symmetric_key>(encryption::key_info{ "AES", 128 });

    {
        data_sink sink = c.create_upload_sink(w.bucket, name);
        if (encrypt) {
            sink = data_sink(encryption::make_encrypted_sink(std::move(sink), k));
        }
        co_await write_object_of_size(sink, dest_size, &written, std::nullopt, !encrypt);
        co_await sink.flush();
        co_await sink.close();
    }

    auto [is_ref, total] = stream_from_buffers(std::move(std::move(written)));

    auto chunk_size = 64 * 1024;
    auto start = do_ranged ? 48*1024*1024 + 6 * chunk_size : 0;
    auto len = dest_size - start;
    auto skip = do_additional_skip ? chunk_size : 0;

    co_await is_ref.skip(start + skip);

    data_source source = c.create_download_source(w.bucket, name);
    if (encrypt) {
        source = data_source(encryption::make_encrypted_source(std::move(source), k));
    }
    if (do_ranged) {
        source = create_ranged_source(std::move(source), start, len);
    }

    input_stream<char> is(std::move(source));

    if (skip) {
        co_await is.skip(chunk_size);
    }

    for (auto rem = len - skip; rem > 0;) {
        auto chunk = std::min(rem, chunk_size);

        auto buf1 = co_await is_ref.read_exactly(chunk_size);
        auto buf2 = co_await is.read_exactly(chunk_size);

        BOOST_REQUIRE_EQUAL(buf1.size(), chunk);
        BOOST_REQUIRE_EQUAL(buf2.size(), chunk);

        BOOST_REQUIRE_EQUAL(buf1, buf2);

        rem -= chunk;
    }
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_chunked_partial, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await chunked_read_helper(*this, true, false, false);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_chunked_partial_with_skip, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await chunked_read_helper(*this, true, false, true);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_chunked_fully_encrypted, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await chunked_read_helper(*this, false, true, false);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_chunked_partial_encrypted, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await chunked_read_helper(*this, true, true, false);
}

SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_read_chunked_partial_encrypted_with_skip, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    co_await chunked_read_helper(*this, true, true, true);
}

// The client reports its http metrics only under labels an owner supplies. In
// production that owner is the sstables object storage client, which also reports
// the bytes; the fixture's client is created directly, so the test names it.
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_metrics, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto& c = env.client();
    c.register_metrics({.type = "gs", .endpoint = "test", .class_name = "all"});

    auto puts = gs_metric("total_put_requests");
    auto gets = gs_metric("total_get_requests");
    auto bytes_before = c.bytes();

    constexpr size_t object_size = 1024 * 1024;
    co_await test_read_write_helper(env, object_size);

    BOOST_REQUIRE_GT(gs_metric("total_put_requests"), puts);
    BOOST_REQUIRE_GT(gs_metric("total_get_requests"), gets);
    auto bytes_after = c.bytes();
    BOOST_REQUIRE_GE(bytes_after.written - bytes_before.written, object_size);
    BOOST_REQUIRE_GE(bytes_after.read - bytes_before.read, object_size);
}

// A config update replaces the GCS client, and its counters start at zero, so
// the wrapper carries what the replaced client moved. The replaced client is
// closed in the background, and until that close completes the reported total
// must not fall below what it already reported.
SEASTAR_FIXTURE_TEST_CASE(test_gcp_storage_metrics_across_config_update, local_gcs_wrapper, *check_gcp_storage_test_enabled()) {
    auto& env = *this;
    auto ep = db::object_storage_endpoint_param(db::object_storage_endpoint_param::gs_storage{
        .endpoint = env.endpoint,
        .credentials_file = "none",
    });

    seastar::semaphore memory{16 * 1024 * 1024};
    auto client = sstables::make_object_storage_client(ep, memory, [](std::string) {
        return seastar::shared_ptr<sstables::object_storage_client>{};
    });

    auto name = make_name();
    env.objects_to_delete.emplace_back(name);
    constexpr size_t object_size = 1024 * 1024;
    auto out = seastar::output_stream<char>(client->make_upload_sink(sstables::object_name(env.bucket, name), {}));
    co_await out.write(seastar::temporary_buffer<char>(object_size).share());
    co_await out.flush();
    co_await out.close();

    auto written = gs_metric("total_write_bytes");
    BOOST_REQUIRE_GE(written, object_size);

    // Hold a request in flight: the client's gate, which its close() waits on,
    // cannot be closed while it runs, so the replaced client stays alive and
    // the window this test is about stays open.
    utils::get_local_injector().enable("gcp_storage_stall_requests", true);
    auto stop_stalling = defer([]() noexcept {
        utils::get_local_injector().disable("gcp_storage_stall_requests");
    });
    auto stalled = client->object_exists(sstables::object_name(env.bucket, name));
    BOOST_REQUIRE(!stalled.available());

    client->update_config_sync(ep);

    // Sampled before the replaced client has drained, which is what makes this
    // observable: nothing has yielded since update_config_sync() returned.
    BOOST_REQUIRE_GE(gs_metric("total_write_bytes"), written);

    BOOST_REQUIRE(co_await std::move(stalled));
    BOOST_REQUIRE_GE(gs_metric("total_write_bytes"), written);

    co_await client->close();
}

BOOST_AUTO_TEST_SUITE_END()
