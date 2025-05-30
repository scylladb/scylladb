/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bucket_lister.hh"

#include "utils/log.hh"
#include <seastar/core/pipe.hh>
#include <seastar/http/request.hh>
#include <seastar/util/short_streams.hh>

#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif

namespace s3 {

extern logging::logger s3l;

client::bucket_lister::bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix, size_t objects_per_page, size_t entries_batch)
    : bucket_lister(std::move(client), std::move(bucket), std::move(prefix),
            [] (const fs::path& parent_dir, const directory_entry& entry) { return true; },
            objects_per_page, entries_batch)
{}

client::bucket_lister::bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix, lister::filter_type filter, size_t objects_per_page, size_t entries_batch)
    : _client(std::move(client))
    , _bucket(std::move(bucket))
    , _prefix(std::move(prefix))
    , _max_keys(format("{}", objects_per_page))
    , _filter(std::move(filter))
    , _queue(entries_batch)
{
}

static std::pair<std::vector<sstring>, sstring> parse_list_of_objects(sstring body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse list-objects-v2 response: {}", e.what());
        throw std::runtime_error("cannot parse objects list response");
    }

    std::vector<sstring> names;
    auto root_node = doc->first_node("ListBucketResult");
    for (auto contents = root_node->first_node("Contents"); contents; contents = contents->next_sibling()) {
        auto key = contents->first_node("Key");
        names.push_back(key->value());
    }

    sstring continuation_token;
    auto is_truncated = root_node->first_node("IsTruncated");
    if (is_truncated && std::string_view(is_truncated->value()) == "true") {
        auto continuation = root_node->first_node("NextContinuationToken");
        if (!continuation) {
            throw std::runtime_error("no continuation token in truncated list of objects");
        }
        continuation_token = continuation->value();
    }

    return {std::move(names), std::move(continuation_token)};
}

future<> client::bucket_lister::start_listing() {
    // This is the implementation of paged ListObjectsV2 API call
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
    sstring continuation_token;
    do {
        s3l.trace("GET /?list-type=2 (prefix={})", _prefix);
        auto req = http::request::make("GET", _client->_host, format("/{}", _bucket));
        req.query_parameters.emplace("list-type", "2");
        req.query_parameters.emplace("max-keys", _max_keys);
        if (!continuation_token.empty()) {
            req.query_parameters.emplace("continuation-token", std::exchange(continuation_token, ""));
        }
        if (!_prefix.empty()) {
            req.query_parameters.emplace("prefix", _prefix);
        }

        std::vector<sstring> names;
        try {
            co_await _client->make_request(std::move(req),
                [&names, &continuation_token] (const http::reply& reply, input_stream<char>&& in) mutable -> future<> {
                    auto input = std::move(in);
                    auto body = co_await util::read_entire_stream_contiguous(input);
                    auto list = parse_list_of_objects(std::move(body));
                    names = std::move(list.first);
                    continuation_token = std::move(list.second);
                }, http::reply::status_type::ok);
        } catch (...) {
            _queue.abort(std::current_exception());
            co_return;
        }

        fs::path dir(_prefix);
        for (auto&& o : names) {
            directory_entry ent{o.substr(_prefix.size())};
            if (!_filter(dir, ent)) {
                continue;
            }
            co_await _queue.push_eventually(std::move(ent));
        }
    } while (!continuation_token.empty());
    co_await _queue.push_eventually(std::nullopt);
}

future<std::optional<directory_entry>> client::bucket_lister::get() {
    if (!_opt_done_fut) {
        _opt_done_fut = start_listing();
    }

    std::exception_ptr ex;
    try {
        auto ret = co_await _queue.pop_eventually();
        if (ret) {
            co_return ret;
        }
    } catch (...) {
        ex = std::current_exception();
    }
    co_await close();
    if (ex) {
        co_return coroutine::exception(std::move(ex));
    }
    co_return std::nullopt;
}

future<> client::bucket_lister::close() noexcept {
    if (_opt_done_fut) {
        _queue.abort(std::make_exception_ptr(broken_pipe_exception()));
        try {
            co_await std::exchange(_opt_done_fut, std::make_optional<future<>>(make_ready_future<>())).value();
        } catch (...) {
            // ignore all errors
        }
    }
}
} // s3