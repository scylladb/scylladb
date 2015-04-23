/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef DB_SERIALIZER_HH_
#define DB_SERIALIZER_HH_

#include "utils/data_input.hh"
#include "utils/data_output.hh"
#include "bytes.hh"
#include "mutation.hh"
#include "database_fwd.hh"

namespace db {
/**
 * Serialization objects for various types and using "internal" format. (Not CQL, origin whatnot).
 * The design rationale is that a "serializer" can be instansiated for an object, and will contain
 * the obj + size, and is useable as a functor.
 *
 * Serialization can also be done "explicitly" through the static method "write"
 * (Not using "serialize", because writing "serializer<apa>::serialize" all the time is tiring and redundant)
 * though care should be takes than data will fit of course.
 */
template<typename T>
class serializer {
public:
    typedef T type;
    typedef data_output output;
    typedef data_input input;
    typedef serializer<T> _MyType;
    typedef database context;

    serializer(const context&, const type&);

    // apply to memory, must be at least size() large.
    const _MyType& operator()(output& out) const {
        write(_ctxt, out, _item);
        return *this;
    }

    static void write(const context&, output&, const T&);
    static void read(const context&, T&, input&);
    static T read(const context&, input&);

    size_t size() const {
        return _size;
    }
private:
    const context& _ctxt;
    const T& _item;
    size_t _size;
};

template<> serializer<utils::UUID>::serializer(const context&, const utils::UUID &);
template<> void serializer<utils::UUID>::write(const context&, output&, const type&);
template<> void serializer<utils::UUID>::read(const context&, utils::UUID&, input&);
template<> utils::UUID serializer<utils::UUID>::read(const context&, input&);

template<> serializer<bytes>::serializer(const context&, const bytes &);
template<> void serializer<bytes>::write(const context&, output&, const type&);
template<> void serializer<bytes>::read(const context&, bytes&, input&);

template<> serializer<bytes_view>::serializer(const context&, const bytes_view&);
template<> void serializer<bytes_view>::write(const context&, output&, const type&);
template<> void serializer<bytes_view>::read(const context&, bytes_view&, input&) = delete;
template<> bytes_view serializer<bytes_view>::read(const context&, input&) = delete;

template<> serializer<sstring>::serializer(const context&, const sstring&);
template<> void serializer<sstring>::write(const context&, output&, const type&);
template<> void serializer<sstring>::read(const context&, sstring&, input&);

template<> serializer<tombstone>::serializer(const context&, const tombstone &);
template<> void serializer<tombstone>::write(const context&, output&, const type&);
template<> void serializer<tombstone>::read(const context&, tombstone&, input&);

template<> serializer<atomic_cell_or_collection>::serializer(const context&, const atomic_cell_or_collection &);
template<> void serializer<atomic_cell_or_collection>::write(const context&, output&, const type&);
template<> void serializer<atomic_cell_or_collection>::read(const context&, atomic_cell_or_collection&, input&);

template<> serializer<row>::serializer(const context&, const row &);
template<> void serializer<row>::write(const context&, output&, const type&);
template<> void serializer<row>::read(const context&, row&, input&);

template<> serializer<mutation_partition>::serializer(const context&, const mutation_partition &);
template<> void serializer<mutation_partition>::write(const context&, output&, const type&);
template<> void serializer<mutation_partition>::read(const context&, mutation_partition&, input&);
template<> mutation_partition serializer<mutation_partition>::read(const context&, input&) = delete;

template<> serializer<mutation>::serializer(const context&, const mutation &);
template<> void serializer<mutation>::write(const context&, output&, const type&);
template<> void serializer<mutation>::read(const context&, mutation&, input&) = delete;
template<> mutation serializer<mutation>::read(const context&, input&);

template<typename T>
T serializer<T>::read(const context& ctxt, input& in) {
    type t;
    read(ctxt, t, in);
    return std::move(t);
}

extern template class serializer<mutation>;
extern template class serializer<mutation_partition>;
extern template class serializer<tombstone>;
extern template class serializer<row>;
extern template class serializer<bytes>;
extern template class serializer<bytes_view>;
extern template class serializer<sstring>;
extern template class serializer<atomic_cell_or_collection>;
extern template class serializer<utils::UUID>;

typedef serializer<mutation> mutation_serializer;
typedef serializer<mutation_partition> mutation_partition_serializer;
typedef serializer<tombstone> tombstone_serializer;
typedef serializer<row> row_serializer;
typedef serializer<bytes> bytes_serializer;
typedef serializer<bytes_view> bytes_view_serializer;
typedef serializer<sstring> sstring_serializer;
typedef serializer<atomic_cell_or_collection> atomic_cell_or_collection_serializer;
typedef serializer<utils::UUID> uuid_serializer;

}

#endif /* DB_SERIALIZER_HH_ */
