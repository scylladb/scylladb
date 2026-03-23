/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>


class limiting_data_source_impl : public seastar::data_source_impl {
    seastar::data_source _src;
    seastar::noncopyable_function<size_t()> _limit_generator;;
    seastar::temporary_buffer<char> _buf;
    seastar::future<seastar::temporary_buffer<char>> do_get();

public:
    limiting_data_source_impl(seastar::data_source&& src, seastar::noncopyable_function<size_t()>&& limit_generator);

    limiting_data_source_impl(limiting_data_source_impl&&) noexcept = default;
    limiting_data_source_impl& operator=(limiting_data_source_impl&&) noexcept = default;

    seastar::future<seastar::temporary_buffer<char>> get() override;
    seastar::future<seastar::temporary_buffer<char>> skip(uint64_t n) override;
};

/// \brief Creates an data_source from another data_source but returns its data in chunks not bigger than a given limit
///
/// \param src Source data_source from which data will be taken
/// \return resulting data_source that returns data in chunks not bigger than a given limit
seastar::data_source make_limiting_data_source(seastar::data_source&& src,
                                               seastar::noncopyable_function<size_t()>&& limit_generator);
