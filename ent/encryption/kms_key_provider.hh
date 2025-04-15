/*
 * Copyright (C) 2022 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "encryption.hh"
#include "system_key.hh"

namespace encryption {

class kms_key_provider_factory : public key_provider_factory {
public:
    shared_ptr<key_provider> get_provider(encryption_context&, const options&) override;
};

/**
 * As it stands today, given system_key api (gives keys), and
 * what it is used for (config encryption), we cannot provide
 * a KMS system key. This is because:
 * 
 * a.) KMS does not allow us to store a named object (key) in a secure(ish) way.
 *     We can encrypt/decrypt and create one-off keys for local usage, which are
 *     encoded in their own ID (see kms_host), but having a unique key from
 *     a "path" is not possible. Esp. due to key rotation, encrypted data preamble
 *     etc. We could keep the encrypted key material in a local file, then decrypt
 *     it using a named key on startup, but given b.) it is dubious if this is useful.
 * b.) System keys are only used for config encryption. The authentication config for
 *     AWS/KMS access is typically one of the things that should be encrypted. Thus 
 *     we would create a big chicken and egg problem here.
 */
}
