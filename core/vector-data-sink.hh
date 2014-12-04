/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef VECTOR_DATA_SINK_HH_
#define VECTOR_DATA_SINK_HH_

#include "core/reactor.hh"

class vector_data_sink final : public data_sink_impl {
public:
    using vector_type = std::vector<net::packet>;
private:
    vector_type& _v;
public:
    vector_data_sink(vector_type& v) : _v(v) {}

    virtual future<> put(net::packet p) override {
        _v.push_back(std::move(p));
        return make_ready_future<>();
    }

    virtual future<> close() override {
        // TODO: close on local side
        return make_ready_future<>();
    }
};

#endif
