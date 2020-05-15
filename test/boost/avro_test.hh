#ifndef AVRO_TEST_HH
#define AVRO_TEST_HH

#include "avro/lang/c++/api/Specific.hh"
#include "avro/lang/c++/api/Encoder.hh"
#include "avro/lang/c++/api/Decoder.hh"

namespace c {
struct cpx {
    double re;
    double im;
};

}
namespace avro {
template<> struct codec_traits<c::cpx> {
    static void encode(Encoder& e, const c::cpx& v) {
        avro::encode(e, v.re);
        avro::encode(e, v.im);
    }
    static void decode(Decoder& d, c::cpx& v) {
        avro::decode(d, v.re);
        avro::decode(d, v.im);
    }
};

}
#endif
