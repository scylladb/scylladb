/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include <iostream>

#include "keys.hh"

std::ostream& operator<<(std::ostream& out, const partition_key& pk) {
    return out << "pk{" << to_hex(pk) << "}";
}

std::ostream& operator<<(std::ostream& out, const clustering_key& ck) {
    return out << "ck{" << to_hex(ck) << "}";
}

std::ostream& operator<<(std::ostream& out, const clustering_key_prefix& ckp) {
    return out << "ckp{" << to_hex(ckp) << "}";
}
