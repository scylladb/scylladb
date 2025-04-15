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

const extern sstring default_key_file_path;

class local_file_provider;

class local_file_provider_factory : public key_provider_factory {
public:
    static shared_ptr<key_provider> find(encryption_context&, const sstring& path);
    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;
};

class local_system_key : public system_key {
    shared_ptr<local_file_provider> _provider;
public:
    local_system_key(encryption_context&, const sstring&);
    ~local_system_key();

    future<shared_ptr<symmetric_key>> get_key() override;
    future<> validate() const override;
    const sstring& name() const override;
    bool is_local() const override {
        return true;
    }
};

}
