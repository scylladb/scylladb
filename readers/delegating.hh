/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "readers/flat_mutation_reader.hh"

class flat_mutation_reader;

flat_mutation_reader make_delegating_reader(flat_mutation_reader&);

class delegating_reader : public flat_mutation_reader::impl {
    flat_mutation_reader_opt _underlying_holder;
    flat_mutation_reader* _underlying;
public:
    // when passed a lvalue reference to the reader
    // we don't own it and the caller is responsible
    // for evenetually closing the reader.
    delegating_reader(flat_mutation_reader& r)
        : impl(r.schema(), r.permit())
        , _underlying_holder()
        , _underlying(&r)
    { }
    // when passed a rvalue reference to the reader
    // we assume ownership of it and will close it
    // in close().
    delegating_reader(flat_mutation_reader&& r)
        : impl(r.schema(), r.permit())
        , _underlying_holder(std::move(r))
        , _underlying(&*_underlying_holder)
    { }

    virtual future<> fill_buffer() override;
    virtual future<> fast_forward_to(position_range pr) override;
    virtual future<> next_partition() override;
    virtual future<> fast_forward_to(const dht::partition_range& pr) override;
    virtual future<> close() noexcept override;
};
