/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef VECTOR_DATA_SINK_HH_
#define VECTOR_DATA_SINK_HH_

#include "core/reactor.hh"

class vector_data_sink final : public data_sink_impl {
private:
    using vector_type = std::vector<temporary_buffer<char>>;
    vector_type& _v;
public:
    vector_data_sink(vector_type& v) : _v(v) {}

    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        for (auto&& buf : data) {
            _v.push_back(std::move(buf));
        }
        return make_ready_future<>();
    }

    virtual future<> put(temporary_buffer<char> data) override {
        _v.push_back(std::move(data));
        return make_ready_future<>();
    }
};

#endif
