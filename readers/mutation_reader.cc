/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/util/lazy.hh>

#include "readers/mutation_reader.hh"
#include "mutation/mutation_rebuilder.hh"
#include "schema_upgrader.hh"

logging::logger mrlog("mutation_reader");

static size_t compute_buffer_size(const schema& s, const mutation_reader::tracked_buffer& buffer)
{
    return boost::accumulate(
        buffer
        | boost::adaptors::transformed([] (const mutation_fragment_v2& mf) {
            return mf.memory_usage();
        }), size_t(0)
    );
}

mutation_reader& mutation_reader::operator=(mutation_reader&& o) noexcept {
    if (_impl && _impl->is_close_required()) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(mrlog, format("{} [{}]: permit {}: was not closed before overwritten by move-assign", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
    _impl = std::move(o._impl);
    return *this;
}

mutation_reader::~mutation_reader() {
    if (_impl && _impl->is_close_required()) {
        impl* ip = _impl.get();
        // Abort to enforce calling close() before readers are closed
        // to prevent leaks and potential use-after-free due to background
        // tasks left behind.
        on_internal_error_noexcept(mrlog, format("{} [{}]: permit {}: was not closed before destruction", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description()));
        abort();
    }
}

void mutation_reader::impl::clear_buffer_to_next_partition() {
    auto next_partition_start = std::find_if(_buffer.begin(), _buffer.end(), [] (const mutation_fragment_v2& mf) {
        return mf.is_partition_start();
    });
    _buffer.erase(_buffer.begin(), next_partition_start);

    _buffer_size = compute_buffer_size(*_schema, _buffer);
}

template<typename Source>
future<bool> mutation_reader::impl::fill_buffer_from(Source& source) {
    if (source.is_buffer_empty()) {
        if (source.is_end_of_stream()) {
            return make_ready_future<bool>(true);
        }
        return source.fill_buffer().then([this, &source] {
            return fill_buffer_from(source);
        });
    } else {
        while (!source.is_buffer_empty() && !is_buffer_full()) {
            push_mutation_fragment(source.pop_mutation_fragment());
        }
        return make_ready_future<bool>(source.is_end_of_stream() && source.is_buffer_empty());
    }
}

template future<bool> mutation_reader::impl::fill_buffer_from<mutation_reader>(mutation_reader&);

void mutation_reader::do_upgrade_schema(const schema_ptr& s) {
    *this = transform(std::move(*this), schema_upgrader_v2(s));
}

void mutation_reader::on_close_error(std::unique_ptr<impl> i, std::exception_ptr ep) noexcept {
    impl* ip = i.get();
    on_internal_error_noexcept(mrlog,
            format("Failed to close {} [{}]: permit {}: {}", typeid(*ip).name(), fmt::ptr(ip), ip->_permit.description(), ep));
}

future<mutation_opt> read_mutation_from_mutation_reader(mutation_reader& r) {
    return r.consume(mutation_rebuilder_v2(r.schema()));
}
