/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "mutation_writer/token_group_based_splitting_writer.hh"

#include <seastar/core/shared_mutex.hh>
#include <seastar/core/on_internal_error.hh>

#include "mutation_writer/feed_writers.hh"

namespace mutation_writer {

static logging::logger logger("token_group_based_splitting_mutation_writer");

class token_group_based_splitting_mutation_writer {
    schema_ptr _schema;
    reader_permit _permit;
    classify_by_token_group _classify;
    reader_consumer_v2 _consumer;
    token_group_id _current_group_id = 0;
    std::optional<bucket_writer_v2> _current_writer;
private:
    future<> write(mutation_fragment_v2&& mf) {
        return _current_writer->consume(std::move(mf));
    }

    inline void allocate_new_writer_if_needed() {
        if (!_current_writer) [[unlikely]] {
            _current_writer = bucket_writer_v2(_schema, _permit, _consumer);
        }
    }

    future<> maybe_switch_to_new_writer(dht::token t) {
        auto prev_group_id = _current_group_id;
        _current_group_id = _classify(t);

        if (_current_group_id < prev_group_id) [[unlikely]] {
            on_internal_error(logger, format("Token group id cannot go backwards, current={}, previous={}", _current_group_id, prev_group_id));
        }

        if (_current_writer && _current_group_id > prev_group_id) [[unlikely]] {
            _current_writer->consume_end_of_stream();
            return _current_writer->close().then([this] {
                _current_writer = std::nullopt;
                allocate_new_writer_if_needed();
            });
        }
        allocate_new_writer_if_needed();
        return make_ready_future<>();
    }
public:
    token_group_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, classify_by_token_group classify, reader_consumer_v2 consumer)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _classify(std::move(classify))
        , _consumer(std::move(consumer))
    {}

    future<> consume(partition_start&& ps) {
        return maybe_switch_to_new_writer(ps.key().token()).then([this, ps = std::move(ps)] () mutable {
            return write(mutation_fragment_v2(*_schema, _permit, std::move(ps)));
        });
    }

    future<> consume(static_row&& sr) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone_change&& rt) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write(mutation_fragment_v2(*_schema, _permit, std::move(pe)));
    }

    void consume_end_of_stream() {
        if (_current_writer) {
            _current_writer->consume_end_of_stream();
        }
    }
    void abort(std::exception_ptr ep) {
        if (_current_writer) {
            _current_writer->abort(ep);
        }
    }
    future<> close() noexcept {
        return _current_writer ? _current_writer->close() : make_ready_future<>();
    }
};

future<> segregate_by_token_group(mutation_reader producer, classify_by_token_group classify, reader_consumer_v2 consumer) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        token_group_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(classify), std::move(consumer)));
}
} // namespace mutation_writer
