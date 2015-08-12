/*
* Copyright (C) 2015 Cloudius Systems, Ltd.
*
*/

#pragma once

template<typename T>
static inline T consume_be(temporary_buffer<char>& p) {
    T i = net::ntoh(*unaligned_cast<const T*>(p.get()));
    p.trim_front(sizeof(T));
    return i;
}

namespace data_consumer {
enum class proceed { yes, no };
}
