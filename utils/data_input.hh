/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef UTILS_DATA_INPUT_HH_
#define UTILS_DATA_INPUT_HH_

#include "bytes.hh"
#include "net/byteorder.hh"

class data_input {
public:
    data_input(const bytes_view& v)
            : _view(v) {
    }
    data_input(bytes_view&& v)
            : _view(std::move(v)) {
    }
    data_input(const bytes& b)
            : data_input(bytes_view(b)) {
    }
    data_input(const bytes& b, size_t off, size_t n = bytes::npos)
            : data_input(
                    bytes_view(b.c_str() + off, std::min(b.size() - off, n))) {
        if (off > b.size()) {
            throw std::out_of_range("Offset out of range");
        }
    }
    data_input(data_input&&) = default;
    data_input(const data_input&) = default;

    size_t avail() const {
        return _view.size();
    }
    void ensure(size_t s) const {
        if (avail() < s) {
            throw std::out_of_range("Buffer underflow");
        }
    }
    template<typename T> T peek() const;
    template<typename T> T read();
private:
    template<typename T> size_t ssize(const T &) const;
    template<typename T>
    inline std::enable_if_t<std::is_fundamental<T>::value, T> peek_primitive() const {
        ensure(sizeof(T));
        T t;
        std::copy_n(_view.begin(), sizeof(T), reinterpret_cast<char *>(&t));
        return net::ntoh(t);
    }

    bytes_view _view;
};

template<> inline sstring data_input::peek<sstring>() const {
    auto len = peek<uint16_t>();
    ensure(sizeof(uint16_t) + len);
    return sstring(reinterpret_cast<const char*>(_view.data()) + sizeof(uint16_t), len);
}
template<> inline size_t data_input::ssize<sstring>(const sstring & s) const {
    return sizeof(uint16_t) + s.size();
}
template<> inline bytes data_input::peek<bytes>() const {
    auto len = peek<uint32_t>();
    ensure(sizeof(uint32_t) + len);
    return bytes(_view.data() + sizeof(uint32_t), len);
}
template<> inline size_t data_input::ssize<bytes>(const bytes & s) const {
    return sizeof(uint32_t) + s.size();
}
template<> inline size_t data_input::ssize(const bool &) const {
    return sizeof(uint8_t);
}
template<> inline bool data_input::peek<bool>() const {
    return peek<uint8_t>() != 0;
}
template<typename T>
inline T data_input::peek() const {
    return peek_primitive<T>();
}
template<typename T> inline T data_input::read() {
    auto t = peek<T>();
    _view.remove_prefix(ssize(t));
    return std::move(t);
}
template<typename T> inline size_t data_input::ssize(const T &) const {
    return sizeof(T);
}

#endif /* UTILS_DATA_INPUT_HH_ */

