#ifndef _PACKET_DATA_SOURCE_HH
#define _PACKET_DATA_SOURCE_HH

#include "core/reactor.hh"
#include "net/packet.hh"

namespace net {

class packet_data_source final : public data_source_impl {
    size_t _cur_frag = 0;
    packet _p;
public:
    explicit packet_data_source(net::packet&& p)
        : _p(std::move(p))
    {}

    virtual future<temporary_buffer<char>> get() override {
        if (_cur_frag != _p.nr_frags()) {
            auto& f = _p.fragments()[_cur_frag++];
            return make_ready_future<temporary_buffer<char>>(
                    temporary_buffer<char>(f.base, f.size,
                            make_deleter(deleter(), [p = _p.share()] () mutable {})));
        }
        return make_ready_future<temporary_buffer<char>>(temporary_buffer<char>());
    }
};

static inline
input_stream<char> as_input_stream(packet&& p) {
    return input_stream<char>(data_source(std::make_unique<packet_data_source>(std::move(p))));
}

}

#endif
