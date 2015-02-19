/*
 * Copyright 2015 Cloudius Systems
 */

#include "log.hh"
#include <vector>
#include <typeinfo>
#include "core/future.hh"
#include "core/future-util.hh"
#include "core/sstring.hh"
#include "core/fstream.hh"
#include "core/shared_ptr.hh"

#include "types.hh"
#include "sstables.hh"

namespace sstables {

thread_local logging::logger sstlog("sstable");

unordered_map<sstable::version_types, sstring> sstable::_version_string = {
    { sstable::version_types::la , "la" }
};

unordered_map<sstable::format_types, sstring> sstable::_format_string = {
    { sstable::format_types::big , "big" }
};

unordered_map<sstable::component_type, sstring> sstable::_component_map = {
    { component_type::Index, "Index.db"},
    { component_type::CompressionInfo, "CompressionInfo.db" },
    { component_type::Data, "Data.db" },
    { component_type::TOC, "TOC.txt" },
    { component_type::Summary, "Summary.db" },
    { component_type::Digest, "Digest.sha1" },
    { component_type::CRC, "CRC.db" },
    { component_type::Filter, "Filter.db" },
    { component_type::Statistics, "Statistics.db" },
};

struct bufsize_mismatch_exception : malformed_sstable_exception {
    bufsize_mismatch_exception(size_t size, size_t expected) :
        malformed_sstable_exception("Buffer to small to hold requested data. Got: " + to_sstring(size) + ". Expected: " + to_sstring(expected))
    {}
};

// This should be used every time we use read_exactly directly.
//
// read_exactly is a lot more convenient of an interface to use, because we'll
// be parsing known quantities.
//
// However, anything other than the size we have asked for, is certainly a bug,
// and we need to do something about it.
static void check_buf_size(temporary_buffer<char>& buf, size_t expected) {
    if (buf.size() < expected) {
        throw bufsize_mismatch_exception(buf.size(), expected);
    }
}

// Base parser, parses an integer type
template <typename T>
typename std::enable_if_t<std::is_integral<T>::value, future<>>
parse(file_input_stream& in, T& i) {
    return in.read_exactly(sizeof(T)).then([&i] (auto buf) {
        check_buf_size(buf, sizeof(T));

        auto *nr = reinterpret_cast<const T *>(buf.get());
        i = net::ntoh(*nr);
        return make_ready_future<>();
    });
}

template <typename T>
typename std::enable_if_t<std::is_enum<T>::value, future<>>
parse(file_input_stream& in, T& i) {
    return parse(in, reinterpret_cast<typename std::underlying_type<T>::type&>(i));
}

future<> parse(file_input_stream& in, bool& i) {
    return parse(in, reinterpret_cast<uint8_t&>(i));
}

template <typename To, typename From>
static inline To convert(From f) {
    static_assert(sizeof(To) == sizeof(From), "Sizes must match");
    union {
        To to;
        From from;
    } conv;

    conv.from = f;
    return conv.to;
}

future<> parse(file_input_stream& in, double& d) {
    return in.read_exactly(sizeof(double)).then([&d] (auto buf) {
        check_buf_size(buf, sizeof(double));

        const unsigned long *nr = reinterpret_cast<const unsigned long *>(buf.get());
        d = convert<double>(net::ntoh(*nr));
        return make_ready_future<>();
    });
}

template <typename T>
future<> parse(file_input_stream& in, T& len, sstring& s) {
    return in.read_exactly(len).then([&s, len] (auto buf) {
        check_buf_size(buf, len);
        s = sstring(buf.get(), len);
    });
}

// All composite parsers must come after this
template<typename First, typename... Rest>
future<> parse(file_input_stream& in, First& first, Rest&&... rest) {
    return parse(in, first).then([&in, &rest...] {
        return parse(in, std::forward<Rest>(rest)...);
    });
}

// For all types that take a size, we provide a template that takes the type
// alone, and another, separate one, that takes a size parameter as well, of
// type Size. This is because although most of the time the size and the data
// are contiguous, it is not always the case. So we want to have the
// flexibility of parsing them separately.
template <typename Size>
future<> parse(file_input_stream& in, disk_string<Size>& s) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &s, len = std::move(len)] {
        return parse(in, *len, s.value);
    });
}

// We cannot simply read the whole array at once, because we don't know its
// full size. We know the number of elements, but if we are talking about
// disk_strings, for instance, we have no idea how much of the stream each
// element will take.
//
// Sometimes we do know the size, like the case of integers. There, all we have
// to do is to convert each member because they are all stored big endian.
// We'll offer a specialization for that case below.
template <typename Size, typename Members>
typename std::enable_if_t<!std::is_integral<Members>::value, future<>>
parse(file_input_stream& in, Size& len, std::vector<Members>& arr) {

    auto count = make_lw_shared<size_t>(0);
    auto eoarr = [count, len] { return *count == len; };

    return do_until(eoarr, [count, &in, &arr] {
        return parse(in, arr[(*count)++]);
    });
}

template <typename Size, typename Members>
typename std::enable_if_t<std::is_integral<Members>::value, future<>>
parse(file_input_stream& in, Size& len, std::vector<Members>& arr) {
    return in.read_exactly(len * sizeof(Members)).then([&arr, len] (auto buf) {
        check_buf_size(buf, len * sizeof(Members));

        auto *nr = reinterpret_cast<const Members *>(buf.get());
        for (size_t i = 0; i < len; ++i) {
            arr[i] = net::ntoh(nr[i]);
        }
        return make_ready_future<>();
    });
}

// We resize the array here, before we pass it to the integer / non-integer
// specializations
template <typename Size, typename Members>
future<> parse(file_input_stream& in, disk_array<Size, Members>& arr) {
    auto len = std::make_unique<Size>();
    auto f = parse(in, *len);
    return f.then([&in, &arr, len = std::move(len)] {
        arr.elements.resize(*len);
        return parse(in, *len, arr.elements);
    });
}

const bool sstable::has_component(component_type f) {
    return _components.count(f);
}

const sstring sstable::filename(component_type f) {

    auto& version = _version_string.at(_version);
    auto& format = _format_string.at(_format);
    auto& component = _component_map.at(f);
    auto epoch =  to_sstring(_epoch);

    return _dir + "/" + version + "-" + epoch + "-" + format + "-" + component;
}
}
