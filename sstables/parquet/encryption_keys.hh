/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Where a Parquet encryption key comes from.
//
// It comes from Scylla's own encryption-at-rest key providers -- the same ones that back
// `scylla_encryption_options`: local file, replicated, KMIP, AWS KMS, GCP, Azure. That is what
// makes BYOK work, and it is the whole point of this indirection: Parquet Modular Encryption
// supplies the *format*, and ent/encryption supplies the *keys*, and neither has to know much
// about the other.
//
// The interface below is deliberately narrow, and it is an abstract class rather than a direct
// call into `encryption::` for a build reason that is also an architectural one: the
// `scylla_encryption` library already depends on `sstables` (its sstable file-io extension takes
// sstables::sstable), so sstables must not depend back on it. The implementation therefore lives
// on the ent/encryption side (parquet_key_source.cc) and is installed here during startup.
//
// The two key models line up almost exactly, which is why this is thin. ent/encryption's
// key_provider::key() returns a key *and* an opaque id to store next to the data and hand back
// later to retrieve the same key; Parquet's FileCryptoMetaData.key_metadata is documented as
// "whatever the reader needs to find the key". So the id goes in key_metadata, and comes back out
// on read. Some providers (the local-file one, for instance) issue no id at all -- the options
// alone identify the key -- and an empty id means exactly that.

#pragma once

#include "sstables/parquet/format/encryption.hh"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <map>
#include <optional>

class schema;

namespace sstables::parquet {

// ent/encryption's `scylla_encryption_options` vocabulary: key_provider, cipher_algorithm,
// secret_key_strength, and whatever the named provider itself reads (secret_key_file, kmip_host,
// master_key, ...). Carried as an opaque map so a provider option added later needs no change
// here; the set the `parquet` property will *forward* is a closed list, in writer_impl.cc, so a
// typo stays an error rather than becoming an inert setting.
using key_options = std::map<seastar::sstring, seastar::sstring>;

struct resolved_key {
    format::encryption_key key;
    // Opaque provider id, to be stored in FileCryptoMetaData.key_metadata and handed back on
    // read. Empty when the provider issues none, which is not an error: it means the options in
    // the schema are by themselves enough to find the key again.
    seastar::sstring id;
};

class key_source {
public:
    virtual ~key_source() = default;
    // A key to write with, plus the id to record alongside the file.
    virtual seastar::future<resolved_key> key_for_write(const key_options&) = 0;
    // The key a file was written with. `id` is what came out of its key_metadata; empty means
    // the file recorded none, and the provider must derive the key from the options alone.
    virtual seastar::future<format::encryption_key> key_for_read(
            const key_options&, const seastar::sstring& id) = 0;
    // Whether the provider is reachable and the options name something real. Used at DDL time,
    // where the operator is present to be told, rather than at flush time hours later.
    virtual seastar::future<> validate(const key_options&) = 0;
};

// Installed once during startup (from encryption::register_extensions), before anything can open
// or write an sstable, and never replaced. A raw pointer rather than a shared_ptr because it is
// read from every shard and the object outlives every use of it, so there is no refcount to race
// on. Null when the node has no encryption support wired up at all, in which case a table that
// asks for parquet encryption fails loudly rather than writing in the clear.
void set_key_source(key_source*);
key_source* key_source_ptr();

// DDL-time check that a table asking for parquet encryption can actually get a key: the provider
// is reachable, the options name something real, and what comes back is an AES key. Reaching a
// KMIP server or a cloud KMS is I/O, hence the future -- which is why this hangs off the schema
// announcement path rather than off the synchronous property validation.
//
// The alternative to checking here is a table that accepts its DDL and then fails every flush,
// whose first symptom is a compaction error hours later. The DDL is the last moment at which the
// operator is present to be told. It checks the *local* node, which is a real limitation worth
// naming: another node may not reach the same provider, and its writes will fail there.
//
// A no-op for every table that does not ask for encryption, which is nearly all of them, and for
// every non-parquet table.
seastar::future<> validate_encryption(const ::schema&);

// FileCryptoMetaData.key_metadata is opaque to the Parquet spec -- "whatever the reader needs to
// find the key" -- so what goes in it is a deployment choice, not a format one. Two shapes, and the
// default is deliberately the provider-neutral one.
//
//  provider    the key's own identifier, verbatim. This is what Scylla's own encryption at rest
//              deals in: ent/encryption's key_provider::key() returns a key *and* an opaque id to
//              store with the data and hand back to retrieve the same key later, which is exactly
//              the role key_metadata plays. Works with every provider, including BYOK through
//              KMIP/KMS/Azure/GCP, because the provider defines the id and we never interpret it.
//
//  parquet_kms parquet-java's "key tools" key-material JSON. pyarrow's Python API and Spark
//              decrypt *only* through a KMS that requires this shape, so it is the only way those
//              readers can open the file through their high-level API. But it encodes a specific
//              key-management model -- a masterKeyID plus a wrapped DEK -- and a BYOK deployment
//              whose keys live in a KMIP server or a cloud KMS does not necessarily map onto it.
//              Opt-in for that reason: it buys one reader's convenience at the cost of assuming
//              its key management, and that trade is the operator's to make, not ours.
//
// A reader using explicit keys (the C++/Java low-level API) opens either shape. Only the
// KMS-mediated high-level path needs `parquet_kms`.
enum class key_metadata_format { provider, parquet_kms };

std::optional<key_metadata_format> parse_key_metadata_format(std::string_view);
const char* to_string(key_metadata_format);

seastar::sstring make_key_metadata(const seastar::sstring& key_id, key_metadata_format);

// The inverse: pull the key id back out of either shape.
seastar::sstring key_id_from_metadata(const seastar::sstring& key_metadata);

} // namespace sstables::parquet
