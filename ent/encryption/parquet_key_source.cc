/*
 * Copyright (C) 2026-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "parquet_key_source.hh"
#include "encryption.hh"
#include "symmetric_key.hh"
#include "sstables/parquet/encryption_keys.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>

#include <stdexcept>

namespace encryption {

namespace {

// The adapter. Small on purpose: the two key models line up almost one to one, so the only real
// work is turning an opaque `opt_bytes` id into something that fits in a Parquet key_metadata
// string, and back.
class parquet_key_source : public sstables::parquet::key_source {
    seastar::shared_ptr<encryption_context> _ctxt;

    // Throws rather than returning null. A table whose DDL asked for encryption and whose
    // provider resolves to nothing must fail the write, not fall back to plaintext -- the whole
    // reason this feature exists is that nothing downstream would notice.
    shared_ptr<key_provider> provider_for(const options& opts) const {
        if (!_ctxt) {
            throw std::invalid_argument(
                    "parquet encryption: no encryption context is registered on this node");
        }
        auto p = _ctxt->get_provider(opts);
        if (!p) {
            throw std::invalid_argument(
                    "parquet encryption: the key provider options resolve to no provider "
                    "(key_provider: 'none'?)");
        }
        return p;
    }

    // key_metadata is a string; a provider id is opaque bytes (a raw UUID for the replicated
    // provider, a server-side handle for KMIP, a wrapped DEK for the cloud ones). base64 is the
    // encoding ent/encryption already uses for exactly this in its own config and key files, so
    // an operator seeing the id in a `parquet-tools` dump sees the same spelling twice.
    static sstring encode_id(const opt_bytes& id) {
        if (!id || id->empty()) {
            return {};
        }
        return base64_encode(*id);
    }
    static opt_bytes decode_id(const sstring& id) {
        if (id.empty()) {
            return std::nullopt;
        }
        return base64_decode(id);
    }

    static sstables::parquet::format::encryption_key to_parquet_key(const key_ptr& k) {
        const auto& b = k->key();
        sstables::parquet::format::encryption_key out;
        out.bytes.assign(b.begin(), b.end());
        return out;
    }

public:
    explicit parquet_key_source(seastar::shared_ptr<encryption_context> ctxt)
        : _ctxt(std::move(ctxt))
    {}

    void rebind(seastar::shared_ptr<encryption_context> ctxt) {
        _ctxt = std::move(ctxt);
    }

    future<sstables::parquet::resolved_key> key_for_write(
            const sstables::parquet::key_options& kopts) override {
        const options opts(kopts.begin(), kopts.end());
        auto p = provider_for(opts);
        auto [k, id] = co_await p->key(get_key_info(opts));
        co_return sstables::parquet::resolved_key{to_parquet_key(k), encode_id(id)};
    }

    future<sstables::parquet::format::encryption_key> key_for_read(
            const sstables::parquet::key_options& kopts, const sstring& id) override {
        const options opts(kopts.begin(), kopts.end());
        auto p = provider_for(opts);
        // Handing the id back is what makes rotation work: the provider returns *that* key rather
        // than the current one, so a file written before a rotation still opens.
        auto [k, _] = co_await p->key(get_key_info(opts), decode_id(id));
        co_return to_parquet_key(k);
    }

    future<> validate(const sstables::parquet::key_options& kopts) override {
        const options opts(kopts.begin(), kopts.end());
        auto p = provider_for(opts);
        co_await p->validate();
        // Not just reachability: actually ask for a key. A KMIP host that answers but has no key
        // matching the requested algorithm and length is a table that would accept its DDL and
        // then fail every flush, which is the failure this call exists to move forward in time.
        auto [k, id] = co_await p->key(get_key_info(opts));
        (void)id;
        const auto n = k->key().size();
        if (n != 16 && n != 24 && n != 32) {
            throw std::invalid_argument(fmt::format(
                    "parquet encryption: the key provider returned a {}-byte key; AES needs 16, "
                    "24 or 32", n));
        }
    }
};

} // namespace

void register_parquet_key_source(seastar::shared_ptr<encryption_context> ctxt) {
    // One adapter for the life of the process, rebound rather than replaced.
    //
    // The pointer sstables holds is raw and never freed, because it is read from every shard and
    // there must be nothing to race on (see sstables/parquet/encryption_keys.hh). But a *new*
    // adapter per call would accumulate: a test process builds many cql_test_envs and so calls
    // this many times, and each adapter holds a shared_ptr that would keep a whole
    // encryption_context alive -- with its per-shard provider caches and their keys. That is a
    // leak that grows with the number of test environments, not a fixed few bytes, and it showed
    // up as a bad_alloc partway through a batch of them.
    //
    // So: allocate once, and on every later registration just rebind the context, which releases
    // the previous one. Safe because this runs pre-init, before anything can open or write an
    // sstable, on the shard that created the context.
    static parquet_key_source* adapter = nullptr;
    if (!adapter) {
        adapter = new parquet_key_source(std::move(ctxt));
        sstables::parquet::set_key_source(adapter);
    } else {
        adapter->rebind(std::move(ctxt));
    }
}

}
