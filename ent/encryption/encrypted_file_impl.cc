/*
 * Copyright (C) 2018 ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <fcntl.h>
#include <fmt/format.h>
#include <seastar/core/file.hh>
#include <seastar/core/byteorder.hh>

#include "symmetric_key.hh"
#include "encryption.hh"
#include "utils/serialization.hh"
#include "encrypted_file_impl.hh"

namespace encryption {

static inline bool is_aligned(size_t n, size_t a) {
    return (n & (a - 1)) == 0;
}

/**
 * Very simple block-encrypting file wrapper.
 *
 * Uses user provided symmetric key + ESSIV block IV calculation
 * to encrypt data.
 *
 * The essiv block key is created by generating the SHA256 hash
 * of the provided data encryption key bytes, truncated to block_key_len/8
 * and generating an AES/ECB key using this data.
 *
 * The file is divided in N blocks of `block_size` size.
 * Each block is encrypted (unpadded) with the provided key and
 * block mode, using an IV derived by (essiv):
 *
 *  bytes tmp[<key block size, typically 16>] = { 0, ..., uint64_t-little-endian(<block number>) }
 *  iv = block_key->encrypt(tmp);
 *
 * All encryption is done unpadded. To handle file sizes we use
 * a slightly shaky scheme:
 *
 * Since all writes are assumed to be done by us, and must be aligned,
 * we can assume in turn that any resizing should be made by our truncate
 * method. If we attempt to truncate to a size not a multiple of our
 * _key_ block size (typically 16), we add the same size to the actual
 * truncation size.
 * On read we then check the file size. If we're reading from a file
 * with unaliged size, we know there are key-block-size junk at the end.
 * We can align down the last decryption call to match block size, then
 * discard the excessive bytes from the result.
 *
 * If we're in a read/write situation, we need to keep size updated, and
 * we could possibly race with disk op/continuations.
 * But we are really only for ro/wo cases.
 */
class encrypted_file_impl : public seastar::file_impl {
    file _file;
    ::shared_ptr<symmetric_key> _key;
    ::shared_ptr<symmetric_key> _block_key;
    bytes _hash_salt;

    std::optional<uint64_t> _file_length;

    // this is somewhat large, but we assume this is for bulky stuff like sstables/commitlog
    // so large alignment should be preferable to reaclculating block IV too often.
    static constexpr size_t block_size = 4096;
    static constexpr size_t block_key_len = 256;

    class my_file_handle_impl;
    friend class my_file_handle_impl;

    bytes iv_for(uint64_t pos) const;

    using mode = symmetric_key::mode;

    temporary_buffer<uint8_t> transform(uint64_t, const void* buffer, size_t len, mode);
    size_t transform(uint64_t, const void* buffer, size_t len, void*, mode);

    future<> verify_file_length();
    void maybe_set_length(uint64_t);
    void clear_length();

    static ::shared_ptr<symmetric_key> generate_block_key(::shared_ptr<symmetric_key>);

public:
    encrypted_file_impl(file, ::shared_ptr<symmetric_key>);

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override;
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override;
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override;
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override;


    future<> flush() override {
        return _file.flush();
    }
    future<struct stat> stat(void) override;
    future<> truncate(uint64_t length) override;
    future<> discard(uint64_t offset, uint64_t length) override {
        return _file.discard(offset, length);
    }
    future<> allocate(uint64_t position, uint64_t length) override {
        return _file.allocate(position, length);
    }
    future<uint64_t> size(void) override;
    future<> close() override {
        return _file.close();
    }
    std::unique_ptr<file_handle_impl> dup() override;

    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return _file.list_directory(std::move(next));
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override;
};

/**
 * Note: ESSIV block iv generation implementation.
 * See: http://securityevaluators.com/knowledge/papers/fde_whitepaper_draft_20170627.pdf
 *
 * We generate a key based on the sha256 of the data key, then encrypt each block number
 * using this to get per-block IV.
 * The key is AES-256, using ECB (non-iv) encryption
 *
 */
::shared_ptr<symmetric_key> encrypted_file_impl::generate_block_key(::shared_ptr<symmetric_key> key) {
    auto hash = calculate_sha256(key->key());
    hash.resize(block_key_len / 8);
    return ::make_shared<symmetric_key>(key_info{"AES/ECB", block_key_len }, hash);
}

encrypted_file_impl::encrypted_file_impl(file f, ::shared_ptr<symmetric_key> key)
    : _file(std::move(f))
    , _key(std::move(key))
    , _block_key(generate_block_key(_key))
{
    _memory_dma_alignment = std::max<unsigned>(_file.memory_dma_alignment(), block_size);
    _disk_read_dma_alignment = std::max<unsigned>(_file.disk_read_dma_alignment(), block_size);
    _disk_write_dma_alignment = std::max<unsigned>(_file.disk_write_dma_alignment(), block_size);
}

static future<uint64_t> calculate_file_length(const file& f, size_t key_block_size) {
    return f.size().then([key_block_size](uint64_t s) {
        if (!is_aligned(s, key_block_size)) {
            if (s < key_block_size) {
                throw std::domain_error(fmt::format("file size {}, expected 0 or at least {}", s, key_block_size));
            }
            s -= key_block_size;
        }
        return s;
    });
}

future<> encrypted_file_impl::verify_file_length() {
    if (_file_length) {
        return make_ready_future();
    }
    return calculate_file_length(_file, _key->block_size()).then([this](uint64_t s) {
        _file_length = s;
    });
}

void encrypted_file_impl::maybe_set_length(uint64_t s) {
    if (s > _file_length.value_or(0)) {
        _file_length = s;
    }
}

void encrypted_file_impl::clear_length() {
    _file_length = std::nullopt;
}

bytes encrypted_file_impl::iv_for(uint64_t pos) const {
    assert(!(pos & (block_size - 1)));

    // #658. ECB block mode has no IV. Bad for security,
    // but must handle.
    size_t iv_len = _key->iv_len();
    if (iv_len == 0) {
        return bytes{};
    }

    assert(iv_len >= _key->block_size());
    assert(iv_len >= sizeof(uint64_t));

    bytes b(bytes::initialized_later(), std::max(iv_len, _block_key->block_size()));
    std::fill(b.begin(), b.end() - sizeof(uint64_t), 0);

    // write block pos as little endian IV-len integer
    auto block = pos / block_size;
    write_le(reinterpret_cast<char *>(b.end()) - sizeof(uint64_t), block);

    // encrypt the encoded block number to build an IV
    _block_key->encrypt_unpadded(b.data(), b.size(), b.data());

    b.resize(iv_len);

    return b;
}

size_t encrypted_file_impl::transform(uint64_t pos, const void* buffer, size_t len, void* dst, mode m) {
    assert(!(pos & (block_size - 1)));
    assert(_file_length || m == mode::encrypt);

    auto o = reinterpret_cast<char*>(dst);
    auto i = reinterpret_cast<const char*>(buffer);
    auto l = _file_length.value_or(std::numeric_limits<uint64_t>::max());
    auto b = _key->block_size();

    size_t off = 0;

    for (; off < len; off += block_size) {
        auto iv = iv_for(pos + off);
        auto rem = std::min<uint64_t>(block_size, len - off);

        if (rem < block_size || ((pos + off + rem) > l && m == symmetric_key::mode::decrypt)) {
            // truncated block. should be the last one.
            if (m != symmetric_key::mode::decrypt) {
                throw std::invalid_argument("Output data not aligned");
            }
            _key->transform_unpadded(m, i + off, align_down(rem, b), o + off, iv.data());
            // #22236 - ensure we don't wrap numbers here.
            // If reading past actual end of file (_file_length), we can be decoding
            // 1-<key block size> bytes here, that are at the boundary of last
            // (fake) block of the file.
            // Example:
            // File data size: 4095 bytes
            // Physical file size: 4095 + 16 (assume 16 bytes key block)
            // Read 0:4096 -> 4095 bytes
            // If caller now ignores this and just reads 4096 (or more)
            // bytes at next block (4096), we read 15 bytes and decode.
            // But would be past _file_length -> ensure we return zero here.
            return std::max(l, pos) - pos;
        }
        _key->transform_unpadded(m, i + off, block_size, o + off, iv.data());
    }

    return off;
}

temporary_buffer<uint8_t> encrypted_file_impl::transform(uint64_t pos, const void* buffer, size_t len, mode m) {
    assert(!(len & (block_size - 1)));
    auto tmp = temporary_buffer<uint8_t>::aligned(_file.memory_dma_alignment(), len);
    auto s = transform(pos, buffer, len, tmp.get_write(), m);
    tmp.trim(s);
    return tmp;
}

future<size_t> encrypted_file_impl::write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) {
    assert(!(len & (block_size - 1)));
    auto tmp = transform(pos, buffer, len, mode::encrypt);
    assert(tmp.size() == len); // writing
    auto p = tmp.get();
    return _file.dma_write(pos, p, len, intent).then([this, tmp = std::move(tmp), pos](size_t s) {
        maybe_set_length(pos + s);
        return s;
    });
}

future<size_t> encrypted_file_impl::write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) {
    std::vector<temporary_buffer<uint8_t>> tmp;
    tmp.reserve(iov.size());
    size_t n = 0;
    for (auto& i : iov) {
        assert(!(i.iov_len & (block_size - 1)));

        tmp.emplace_back(transform(pos + n, i.iov_base, i.iov_len, mode::encrypt));
        assert(tmp.back().size() == i.iov_len); // writing
        n += i.iov_len;
        i = iovec{ tmp.back().get_write(), tmp.back().size() };
    }
    return _file.dma_write(pos, std::move(iov), intent).then([this, tmp = std::move(tmp), pos](size_t s) {
        maybe_set_length(pos + s);
        return s;
    });
}

future<size_t> encrypted_file_impl::read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) {
    assert(!(len & (block_size - 1)));
    return verify_file_length().then([this, pos, buffer, len, intent] {
        if (pos >= *_file_length) {
            return make_ready_future<size_t>(0);
        }
        return _file.dma_read(pos, buffer, len, intent).then([this, pos, buffer](size_t len) {
            return transform(pos, buffer, len, buffer, mode::decrypt);
        });
    });
}

future<size_t> encrypted_file_impl::read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) {
    return verify_file_length().then([this, pos, iov = std::move(iov), intent]() mutable {
        if (pos >= *_file_length) {
            return make_ready_future<size_t>(0);
        }
        auto f = _file.dma_read(pos, iov, intent);
        return f.then([this, pos, iov = std::move(iov)](size_t len) mutable {
            size_t off = 0;
            for (auto& i : iov) {
                off += transform(pos + off, i.iov_base, i.iov_len, i.iov_base, mode::decrypt);
            }
            return off;
        });
    });
}

future<temporary_buffer<uint8_t>> encrypted_file_impl::dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) {
    return verify_file_length().then([this, offset, range_size, intent]() mutable {
        if (offset >= *_file_length) {
            return make_ready_future<temporary_buffer<uint8_t>>();
        }
        auto front = offset & (block_size - 1);
        offset -= front;
        range_size += front;
        // enterprise #925
        // If caller is clever and asks for the last chunk of file
        // explicitly (as in offset = N, range_size = size() - N),
        // or any other unaligned size, we need to add enough padding
        // to get the actual full block to decode.
        auto block_size = align_up(range_size, _key->block_size());
        return _file.dma_read_bulk<uint8_t>(offset, block_size, intent).then([this, offset, front, range_size](temporary_buffer<uint8_t> result) {
            auto s = transform(offset, result.get(), result.size(), result.get_write(), mode::decrypt);
            // never give back more than asked for.
            result.trim(std::min(s, range_size));
            // #22236 - ensure we don't overtrim if we get a short read.
            result.trim_front(std::min(front, result.size()));
            return result;
        });
    });
}

future<> encrypted_file_impl::truncate(uint64_t length) {
    return size().then([this, length](uint64_t s) {
        if (s >= length) {
            auto kb = _key->block_size();
            auto n = length;
            if (!is_aligned(length, kb)) {
                n += kb;
            }
            return _file.truncate(n).then([this, length] {
                _file_length = length;
            });
        }

        // crap. we need to pad zeros. But zeros here means
        // encrypted zeros. So we must do this surprisingly
        // expensively, by actually writing said zeros block
        // by block. Anyone hoping for sparse files is now
        // severely disappointed!

        auto buf_size = align_up(std::min(length, 32 * block_size), block_size);
        auto aligned_size = align_down(s, block_size);

        temporary_buffer<char> buf(buf_size);
        std::fill(buf.get_write(), buf.get_write() + buf_size, 0);

        struct trunc {
            temporary_buffer<char> buf;
            uint64_t aligned_size;
            uint64_t size;
            uint64_t length;
        };

        return do_with(trunc{std::move(buf), aligned_size, s, length}, [this](trunc & t) {
            return repeat([this, &t] {
                if (t.aligned_size >= t.length) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                auto n = std::min(t.buf.size(), align_up(size_t(t.length - t.aligned_size), block_size));
                if (t.aligned_size < t.size) {
                    return read_dma(t.aligned_size, t.buf.get_write(), n, nullptr).then([&, n](size_t r) mutable {
                        auto rem = size_t(t.size - t.aligned_size);
                        auto ar = align_up(r, block_size);
                        assert(ar <= t.buf.size());
                        if (rem < ar) {
                            std::fill(t.buf.get_write() + rem, t.buf.get_write() + ar, 0);
                        }
                        return write_dma(t.aligned_size, t.buf.get(), ar, nullptr).then([&, n](size_t w) {
                            t.aligned_size += w;
                            // #1869. On btrfs, we get the buffer potentially clobbered up to "n" (max read amount)
                            // even when "r" (actual bytes read) is less.
                            std::fill(t.buf.get_write(), t.buf.get_write() + n, 0);
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    });
                }
                return write_dma(t.aligned_size, t.buf.get(), n, nullptr).then([&](size_t w) {
                    t.aligned_size += w;
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            });
        }).then([this, length] {
            return truncate(length);
        });;
    });
}

future<struct stat> encrypted_file_impl::stat() {
    return _file.stat().then([this](struct stat s) {
        return verify_file_length().then([this, s]() mutable {
            s.st_size = *_file_length;
            return s;
        });
    });
}

future<uint64_t> encrypted_file_impl::size() {
    return  verify_file_length().then([this] {
        return *_file_length;
    });
}


std::unique_ptr<file_handle_impl> encrypted_file_impl::dup() {
    class my_file_handle_impl : public seastar::file_handle_impl {
        seastar::file_handle _handle;
        key_info _info;
        bytes _key;
    public:
        my_file_handle_impl(seastar::file_handle h, const key_info& info, const bytes& key)
            : _handle(std::move(h))
            , _info(info)
            , _key(key)
        {}
        std::unique_ptr<file_handle_impl> clone() const override {
            return std::make_unique<my_file_handle_impl>(_handle, _info, _key);
        }
        seastar::shared_ptr<file_impl> to_file() && override {
            return seastar::make_shared<encrypted_file_impl>(_handle.to_file(), ::make_shared<symmetric_key>(_info, _key));
        }
    };

    return std::make_unique<my_file_handle_impl>(_file.dup(), _key->info(), _key->key());
}

shared_ptr<file_impl> make_encrypted_file(file f, ::shared_ptr<symmetric_key> k) {
    return ::make_shared<encrypted_file_impl>(std::move(f), std::move(k));
}

class indirect_encrypted_file_impl : public file_impl {
    ::shared_ptr<file_impl> _impl;
    file _f;
    size_t _key_block_size;
    get_key_func _get;

    future<> get() {
        if (_impl) {
           return make_ready_future<>();
        }
        return _get().then([this](::shared_ptr<symmetric_key> k) {
            // #978 could be running the getting more than once.
            // Only write _impl once though
            if (!_impl) {
                _impl = make_encrypted_file(_f, std::move(k));
            }
        });
    }
public:
    indirect_encrypted_file_impl(file f, size_t key_block_size, get_key_func get)
        : _f(f), _key_block_size(key_block_size), _get(std::move(get))
    {}

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        return get().then([this, pos, buffer, len, intent]() {
            return _impl->write_dma(pos, buffer, len, intent);
        });
    }
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return get().then([this, pos, iov = std::move(iov), intent]() mutable {
            return _impl->write_dma(pos, std::move(iov), intent);
        });
    }
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        return get().then([this, pos, buffer, len, intent]() {
            return _impl->read_dma(pos, buffer, len, intent);
        });
    }
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return get().then([this, pos, iov = std::move(iov), intent]() mutable {
            return _impl->read_dma(pos, std::move(iov), intent);
        });
    }
    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        return get().then([this, offset, range_size, intent]() {
            return _impl->dma_read_bulk(offset, range_size, intent);
        });
    }
    future<> flush(void) override {
        if (_impl) {
            return _impl->flush();
        }
        return _f.flush();
    }
    future<struct stat> stat(void) override {
        if (_impl) {
            return _impl->stat();
        }
        return _f.stat().then([this](struct stat s) {
           return calculate_file_length(_f, _key_block_size).then([s](uint64_t fs) mutable {
               s.st_size = fs;
               return s;
           });
        });
    }
    future<> truncate(uint64_t length) override {
        if (_impl) {
            return _impl->truncate(length);
        }
        return _f.truncate(length);
    }
    future<> discard(uint64_t offset, uint64_t length) override {
        if (_impl) {
            return _impl->discard(offset, length);
        }
        return _f.discard(offset, length);
    }
    future<> allocate(uint64_t position, uint64_t length) override {
        if (_impl) {
            return _impl->allocate(position, length);
        }
        return _f.allocate(position, length);
    }
    future<uint64_t> size(void) override {
        if (_impl) {
            return _impl->size();
        }
        return calculate_file_length(_f, _key_block_size);
    }
    future<> close() override {
        if (_impl) {
            return _impl->close();
        }
        return _f.close();
    }
    std::unique_ptr<file_handle_impl> dup() override {
        if (_impl) {
            return _impl->dup();
        }
        class my_file_handle_impl : public seastar::file_handle_impl {
            seastar::file_handle _handle;
            size_t _key_block_size;
            get_key_func _get;
        public:
            my_file_handle_impl(seastar::file_handle h, size_t key_block_size, get_key_func get)
                : _handle(std::move(h))
                , _key_block_size(key_block_size)
                , _get(std::move(get))
            {}
            std::unique_ptr<file_handle_impl> clone() const override {
                return std::make_unique<my_file_handle_impl>(_handle, _key_block_size, _get);
            }
            seastar::shared_ptr<file_impl> to_file() && override {
                return make_delayed_encrypted_file(_handle.to_file(), _key_block_size, _get);
            }
        };
        return std::make_unique<my_file_handle_impl>(_f.dup(), _key_block_size, _get);
    }

    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        if (_impl) {
            return _impl->list_directory(std::move(next));
        }
        return _f.list_directory(std::move(next));
    }
};

shared_ptr<seastar::file_impl> make_delayed_encrypted_file(file f, size_t key_block_size, get_key_func get) {
    return ::make_shared<indirect_encrypted_file_impl>(std::move(f), key_block_size, std::move(get));
}


}

