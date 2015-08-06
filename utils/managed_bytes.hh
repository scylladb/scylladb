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
// FIXME: Add small managed_bytes optimization
class managed_bytes {
    blob_storage* _ptr;
public:
    using size_type = blob_storage::size_type;
    struct initialized_later {};

    managed_bytes() : managed_bytes(bytes_view()) {}

    managed_bytes(const blob_storage::char_type* ptr, size_type size)
        : managed_bytes(bytes_view(ptr, size)) {}

    managed_bytes(const bytes& b) : managed_bytes(static_cast<bytes_view>(b)) {}

    managed_bytes(initialized_later, size_type size) {
        void* p = current_allocator().alloc(standard_migrator<blob_storage>,
            sizeof(blob_storage) + size, alignof(blob_storage));
        new (p) blob_storage(&_ptr, size);
    }

    managed_bytes(bytes_view v) : managed_bytes(initialized_later(), v.size()) {
        memcpy(_ptr->data, v.data(), v.size());
    }

    ~managed_bytes() {
        if (_ptr) {
            current_allocator().destroy(_ptr);
        }
    }

    managed_bytes(const managed_bytes& o) : managed_bytes(static_cast<bytes_view>(o)) {}

    managed_bytes(managed_bytes&& o) noexcept
        : _ptr(o._ptr)
    {
        o._ptr = nullptr;
        if (_ptr) {
            _ptr->backref = &_ptr;
        }
    }

    managed_bytes& operator=(managed_bytes&& o) {
        this->~managed_bytes();
        new (this) managed_bytes(std::move(o));
        return *this;
    }

    managed_bytes& operator=(const managed_bytes& o) {
        this->~managed_bytes();
        new (this) managed_bytes(o);
        return *this;
    }

    bool operator==(const managed_bytes& o) const {
        return static_cast<bytes_view>(*this) == static_cast<bytes_view>(o);
    }

    bool operator!=(const managed_bytes& o) const {
        return !(*this == o);
    }

    operator bytes_view() const {
        return { _ptr->data, _ptr->size };
    }

    bytes_view::value_type& operator[](size_type index) {
        return _ptr->data[index];
    }

    const bytes_view::value_type& operator[](size_type index) const {
        return _ptr->data[index];
    }

    size_type size() const {
        return _ptr->size;
    }

    const blob_storage::char_type* begin() const {
        return _ptr->data;
    }

    const blob_storage::char_type* end() const {
        return _ptr->data + _ptr->size;
    }

    blob_storage::char_type* begin() {
        return _ptr->data;
    }

    blob_storage::char_type* end() {
        return _ptr->data + _ptr->size;
    }

    bool empty() const {
        return size() == 0;
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
