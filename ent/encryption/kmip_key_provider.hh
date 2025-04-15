/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "encryption.hh"
#include "system_key.hh"

namespace encryption {

class kmip_key_provider_factory : public key_provider_factory {
public:
    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;
};

class kmip_host;

class kmip_system_key : public system_key {
    shared_ptr<symmetric_key> _key;
    shared_ptr<kmip_host> _host;
    sstring _name;
public:
    kmip_system_key(encryption_context&, const sstring&);
    ~kmip_system_key();

    static bool is_kmip_path(const sstring&);

    future<shared_ptr<symmetric_key>> get_key() override;
    const sstring& name() const override;
    bool is_local() const override {
        return false;
    }
};

}
