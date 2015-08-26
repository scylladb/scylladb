#pragma once

#include <stdint.h>
#include <memory>
#include "bytes.hh"
#include "utils/allocation_strategy.hh"

struct blob_storage {
    using size_type = uint32_t;
    using char_type = bytes_view::value_type;

    blob_storage** backref;
    size_type size;
    char_type data[];

    blob_storage(blob_storage** backref, size_type size) noexcept
        : backref(backref)
        , size(size)
    {
        *backref = this;
    }

    blob_storage(blob_storage&& o) noexcept
        : backref(o.backref)
        , size(o.size)
    {
        *backref = this;
        memcpy(data, o.data, size);
    }
} __attribute__((packed));

// A managed version of "bytes" (can be used with LSA).
class managed_bytes {
    static constexpr size_t max_inline_size = 15;
    struct small_blob {
        bytes_view::value_type data[max_inline_size];
        int8_t size; // -1 -> use blob_storage
    };
    union {
        blob_storage* ptr;
        small_blob small;
    } _u;
    static_assert(sizeof(small_blob) > sizeof(blob_storage*), "inline size too small");
private:
    bool external() const {
        return _u.small.size < 0;
    }
public:
    using size_type = blob_storage::size_type;
    struct initialized_later {};

    managed_bytes() {
        _u.small.size = 0;
    }

    managed_bytes(const blob_storage::char_type* ptr, size_type size)
        : managed_bytes(bytes_view(ptr, size)) {}

    managed_bytes(const bytes& b) : managed_bytes(static_cast<bytes_view>(b)) {}

    managed_bytes(initialized_later, size_type size) {
        if (size <= max_inline_size) {
            _u.small.size = size;
        } else {
            _u.small.size = -1;
            void* p = current_allocator().alloc(standard_migrator<blob_storage>,
                sizeof(blob_storage) + size, alignof(blob_storage));
            new (p) blob_storage(&_u.ptr, size);
        }
    }

    managed_bytes(bytes_view v) : managed_bytes(initialized_later(), v.size()) {
        memcpy(data(), v.data(), v.size());
    }

    ~managed_bytes() {
        if (external()) {
            current_allocator().destroy(_u.ptr);
        }
    }

    managed_bytes(const managed_bytes& o) : managed_bytes(static_cast<bytes_view>(o)) {}

    managed_bytes(managed_bytes&& o) noexcept
        : _u(o._u)
    {
        if (external()) {
            if (_u.ptr) {
                _u.ptr->backref = &_u.ptr;
            }
        }
        o._u.small.size = 0;
    }

    managed_bytes& operator=(managed_bytes&& o) {
        if (this != &o) {
            this->~managed_bytes();
            new (this) managed_bytes(std::move(o));
        }
        return *this;
    }

    managed_bytes& operator=(const managed_bytes& o) {
        if (this != &o) {
            // FIXME: not exception safe
            this->~managed_bytes();
            new (this) managed_bytes(o);
        }
        return *this;
    }

    bool operator==(const managed_bytes& o) const {
        return static_cast<bytes_view>(*this) == static_cast<bytes_view>(o);
    }

    bool operator!=(const managed_bytes& o) const {
        return !(*this == o);
    }

    operator bytes_view() const {
        return { data(), size() };
    }

    bytes_view::value_type& operator[](size_type index) {
        return data()[index];
    }

    const bytes_view::value_type& operator[](size_type index) const {
        return data()[index];
    }

    size_type size() const {
        if (external()) {
            return _u.ptr->size;
        } else {
            return _u.small.size;
        }
    }

    const blob_storage::char_type* begin() const {
        return data();
    }

    const blob_storage::char_type* end() const {
        return data() + size();
    }

    blob_storage::char_type* begin() {
        return data();
    }

    blob_storage::char_type* end() {
        return data() + size();
    }

    bool empty() const {
        return size() == 0;
    }

    blob_storage::char_type* data() {
        if (external()) {
            return _u.ptr->data;
        } else {
            return _u.small.data;
        }
    }

    const blob_storage::char_type* data() const {
        return const_cast<managed_bytes*>(this)->data();
    }
};

namespace std {

template <>
struct hash<managed_bytes> {
    size_t operator()(managed_bytes v) const {
        return hash<bytes_view>()(v);
    }
};

}
