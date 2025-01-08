/*
 * Copyright (C) 2015 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "encryption.hh"
#include "../../bytes.hh"

namespace encryption {

class symmetric_key;

class system_key {
public:
    virtual ~system_key() {}
    virtual future<shared_ptr<symmetric_key>> get_key() = 0;
    virtual const sstring& name() const = 0;
    virtual bool is_local() const = 0;
    virtual future<> validate() const;

    future<sstring> encrypt(const sstring&);
    future<sstring> decrypt(const sstring&);
    future<bytes> encrypt(const bytes&);
    future<bytes> decrypt(const bytes&);
};

}

