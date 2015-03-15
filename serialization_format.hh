/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

// Abstraction of transport protocol-dependent serialization format
// Protocols v1, v2 used 16 bits for collection sizes, while v3 and
// above use 32 bits.  But letting every bit of the code know what
// transport protocol we're using (and in some cases, we aren't using
// any transport -- it's for internal storage) is bad, so abstract it
// away here.

class serialization_format {
    bool _use_32_bit;
private:
    explicit serialization_format(bool use_32_bit) : _use_32_bit(use_32_bit) {}
public:
    static serialization_format use_16_bit() { return serialization_format(false); }
    static serialization_format use_32_bit() { return serialization_format(true); }
    static serialization_format internal() { return use_32_bit(); }
};



