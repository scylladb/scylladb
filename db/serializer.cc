/*
 * Copyright 2015 Cloudius Systems
 */

#include "serializer.hh"
#include "types.hh"
#include "util/serialization.hh"

typedef uint32_t count_type; // Me thinks 32-bits are enough for "normal" count purposes.

template<>
db::serializer<utils::UUID>::serializer(const context& ctxt,
        const utils::UUID& uuid)
        : _ctxt(ctxt), _item(uuid), _size(2 * sizeof(uint64_t)) {
}

template<>
void db::serializer<utils::UUID>::write(const context&, output& out,
        const type& t) {
    out.write(t.get_most_significant_bits());
    out.write(t.get_least_significant_bits());
}

template<>
void db::serializer<utils::UUID>::read(const context& ctxt, utils::UUID& uuid,
        input & in) {
    uuid = read(ctxt, in);
}

template<> utils::UUID db::serializer<utils::UUID>::read(const context&,
        input & in) {
    auto msb = in.read<uint64_t>();
    auto lsb = in.read<uint64_t>();
    return utils::UUID(msb, lsb);
}

template<>
db::serializer<bytes>::serializer(const context& ctxt, const bytes& b)
        : _ctxt(ctxt), _item(b), _size(output::serialized_size(b)) {
}

template<>
void db::serializer<bytes>::write(const context&, output& out, const type& t) {
    out.write(t);
}

template<>
void db::serializer<bytes>::read(const context&, bytes& b, input& in) {
    b = in.read<bytes>();
}

template<>
db::serializer<bytes_view>::serializer(const context& ctxt, const bytes_view & v)
        : _ctxt(ctxt), _item(v), _size(output::serialized_size(v)) {
}

template<>
void db::serializer<bytes_view>::write(const context&, output& out, const type& t) {
    out.write(t);
}

template<>
db::serializer<sstring>::serializer(const context& ctxt, const sstring & s)
        : _ctxt(ctxt), _item(s), _size(output::serialized_size(s)) {
}

template<>
void db::serializer<sstring>::write(const context&, output& out, const type& t) {
    out.write(t);
}

template<>
void db::serializer<sstring>::read(const context&, sstring& s, input& in) {
    s = in.read<sstring>();
}

template<>
db::serializer<tombstone>::serializer(const context& ctxt, const tombstone & t)
        : _ctxt(ctxt), _item(t), _size(sizeof(t.timestamp) + sizeof(decltype(t.deletion_time.time_since_epoch().count()))) {
}

template<>
void db::serializer<tombstone>::write(const context&, output& out, const type& t) {
    out.write(t.timestamp);
    out.write(t.deletion_time.time_since_epoch().count());
}

template<>
void db::serializer<tombstone>::read(const context&, tombstone& t, input& in) {
    t.timestamp = in.read<decltype(t.timestamp)>();
    auto deletion_time = in.read<decltype(t.deletion_time.time_since_epoch().count())>();
    t.deletion_time = gc_clock::time_point(gc_clock::duration(deletion_time));
}

template<>
db::serializer<atomic_cell_or_collection>::serializer(const context& ctxt,
        const atomic_cell_or_collection& c)
        : _ctxt(ctxt), _item(c), _size(bytes_serializer(ctxt, c._data).size()) {
}

template<>
void db::serializer<atomic_cell_or_collection>::write(const context& ctxt,
        output& out, const type& t) {
    bytes_serializer::write(ctxt, out, t._data);
}

template<>
void db::serializer<atomic_cell_or_collection>::read(const context& ctxt,
        atomic_cell_or_collection& c, input& in) {
    bytes_serializer::read(ctxt, c._data, in);
}

template<>
db::serializer<row>::serializer(const context& ctxt, const row & r)
        : _ctxt(ctxt), _item(r) {
    size_t s = sizeof(count_type);

    s += r.size() * sizeof(column_id);
    for (auto & e : r) {
        s += atomic_cell_or_collection_serializer(ctxt, e.second).size();
    }
    _size = s;
}

template<>
void db::serializer<row>::write(const context& ctxt, output& out, const type& t) {
    out.write(count_type(t.size()));
    for (auto & e : t) {
        out.write(e.first);
        atomic_cell_or_collection_serializer::write(ctxt, out, e.second);
    }
}

template<>
void db::serializer<row>::read(const context& ctxt, row& r, input& in) {
    auto n = in.read<count_type>();
    r.clear();
    while (n-- > 0) {
        auto id = in.read<column_id>();
        atomic_cell_or_collection c;
        atomic_cell_or_collection_serializer::read(ctxt, c, in);
        r.emplace(id, std::move(c));
    }
}

template<>
db::serializer<mutation_partition>::serializer(const context& ctxt,
        const mutation_partition & p)
        : _ctxt(ctxt), _item(p) {
    size_t s = tombstone_serializer(ctxt, p._tombstone).size();

    s += row_serializer(ctxt, p._static_row).size();

    s += sizeof(count_type); // # rows
    for (auto & dr : p._rows) {
        s += bytes_view_serializer(ctxt, dr.key()).size();
        s += tombstone_serializer(ctxt, dr.row().t).size();
        s += row_serializer(ctxt, dr.row().cells).size();
        s += sizeof(dr.row().created_at);
    }

    s += sizeof(count_type); // # row_tombs
    for (auto & e : p._row_tombstones) {
        s += bytes_view_serializer(ctxt, e.prefix()).size();
        s += tombstone_serializer(ctxt, e.t()).size();
    }
    _size = s;
}

template<>
void db::serializer<mutation_partition>::write(const context& ctxt, output& out,
        const type& t) {
    tombstone_serializer::write(ctxt, out, t._tombstone);
    row_serializer::write(ctxt, out, t._static_row);

    out.write(count_type(t._rows.size()));

    for (auto & dr : t._rows) {
        bytes_view_serializer::write(ctxt, out, dr.key());
        tombstone_serializer::write(ctxt, out, dr.row().t);
        row_serializer::write(ctxt, out, dr.row().cells);
        out.write(dr.row().created_at);
    }

    out.write(count_type(t._row_tombstones.size()));

    for (auto & e : t._row_tombstones) {
        bytes_view_serializer::write(ctxt, out, e.prefix());
        tombstone_serializer::write(ctxt, out, e.t());
    }
}

template<>
void db::serializer<mutation_partition>::read(const context& ctxt,
        mutation_partition& p, input& in) {
    tombstone_serializer::read(ctxt, p._tombstone, in);
    row_serializer::read(ctxt, p._static_row, in);

    auto nr = in.read<count_type>();
    p._rows.clear();
    while (nr-- > 0) {
        auto row = std::make_unique<rows_entry>(
                clustering_key::from_bytes(in.read<bytes>()));
        tombstone_serializer::read(ctxt, row->row().t, in);
        row_serializer::read(ctxt, row->row().cells, in);
        row->row().created_at = in.read<decltype(row->row().created_at)>();
        p._rows.insert(*row);
        row.release();
    }

    auto nt = in.read<count_type>();
    p._row_tombstones.clear();
    while (nt-- > 0) {
        auto te = std::make_unique<row_tombstones_entry>(
                clustering_key_prefix::from_bytes(in.read<bytes>()),
                tombstone_serializer::read(ctxt, in));
        p._row_tombstones.insert(*te);
        te.release();
    }
}

template<>
db::serializer<mutation>::serializer(const context& ctxt, const mutation & m)
        : _ctxt(ctxt), _item(m) {
    size_t s = 0;

    s += bytes_view_serializer(ctxt, m.key).size();
    s += sizeof(bool); // bool

    // schema == null cannot happen (yet). But why not.
    if (_item.schema) {
        s += uuid_serializer(ctxt, _item.schema->id()).size(); // cf UUID
        s += mutation_partition_serializer(ctxt, _item.p).size();
    }
    _size = s;
}

template<>
void db::serializer<mutation>::write(const context& ctxt, output& out,
        const type& t) {
    bytes_view_serializer::write(ctxt, out, t.key);
    out.write(bool(t.schema));

    if (t.schema) {
        uuid_serializer::write(ctxt, out, ctxt.find_uuid(t.schema->ks_name(), t.schema->cf_name()));
        mutation_partition_serializer::write(ctxt, out, t.p);
    }
}

template<>
mutation db::serializer<mutation>::read(const context& ctxt, input& in) {
    auto key = partition_key::from_bytes(in.read<bytes>());
    if (in.read<bool>()) {
        auto sp = ctxt.find_schema(uuid_serializer::read(ctxt, in));
        mutation m(key, sp);
        mutation_partition_serializer::read(ctxt, m.p, in);
        return std::move(m);
    }
    throw std::runtime_error("Should not reach here (yet)");
}

template class db::serializer<mutation> ;
template class db::serializer<mutation_partition> ;
template class db::serializer<tombstone> ;
template class db::serializer<row> ;
template class db::serializer<bytes> ;
template class db::serializer<bytes_view> ;
template class db::serializer<sstring> ;
template class db::serializer<atomic_cell_or_collection> ;
template class db::serializer<utils::UUID> ;

