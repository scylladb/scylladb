/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef IP_CHECKSUM_HH_
#define IP_CHECKSUM_HH_

namespace net {

struct checksummer {
    uint32_t partial = 0;
    bool odd = false;
    void sum(const char* data, size_t len);
    void sum(const packet& p);
    uint16_t get() const;
};

}

#endif /* IP_CHECKSUM_HH_ */
