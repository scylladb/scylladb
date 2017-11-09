/*
 * Copyright (C) 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/util/defer.hh>

#include "partition_version.hh"

static void remove_or_mark_as_unique_owner(partition_version* current)
{
    while (current && !current->is_referenced()) {
        auto next = current->next();
        current_allocator().destroy(current);
        current = next;
    }
    if (current) {
        current->back_reference().mark_as_unique_owner();
    }
}

partition_version::partition_version(partition_version&& pv) noexcept
    : anchorless_list_base_hook(std::move(pv))
    , _backref(pv._backref)
    , _partition(std::move(pv._partition))
{
    if (_backref) {
        _backref->_version = this;
    }
    pv._backref = nullptr;
}

partition_version& partition_version::operator=(partition_version&& pv) noexcept
{
    if (this != &pv) {
        this->~partition_version();
        new (this) partition_version(std::move(pv));
    }
    return *this;
}

partition_version::~partition_version()
{
    if (_backref) {
        _backref->_version = nullptr;
    }
}

size_t partition_version::size_in_allocator(allocation_strategy& allocator) const {
    return allocator.object_memory_size_in_allocator(this) +
           partition().external_memory_usage();
}

namespace {

GCC6_CONCEPT(

// A functor which transforms objects from Domain into objects from CoDomain
template<typename U, typename Domain, typename CoDomain>
concept bool Mapper() {
    return requires(U obj, const Domain& src) {
        { obj(src) } -> const CoDomain&
    };
}

// A functor which merges two objects from Domain into one. The result is stored in the first argument.
template<typename U, typename Domain>
concept bool Reducer() {
    return requires(U obj, Domain& dst, const Domain& src) {
        { obj(dst, src) } -> void;
    };
}

)

// Calculates the value of particular part of mutation_partition represented by
// the version chain starting from v.
// |map| extracts the part from each version.
// |reduce| Combines parts from the two versions.
template <typename Result, typename Map, typename Reduce>
GCC6_CONCEPT(
requires Mapper<Map, mutation_partition, Result>() && Reducer<Reduce, Result>()
)
inline Result squashed(const partition_version_ref& v, Map&& map, Reduce&& reduce) {
    Result r = map(v->partition());
    auto it = v->next();
    while (it) {
        reduce(r, map(it->partition()));
        it = it->next();
    }
    return r;
}

}

row partition_snapshot::static_row() const {
    return ::squashed<row>(version(),
                         [] (const mutation_partition& mp) -> const row& { return mp.static_row(); },
                         [this] (row& a, const row& b) { a.apply(*_schema, column_kind::static_column, b); });
}

tombstone partition_snapshot::partition_tombstone() const {
    return ::squashed<tombstone>(version(),
                               [] (const mutation_partition& mp) { return mp.partition_tombstone(); },
                               [] (tombstone& a, tombstone b) { a.apply(b); });
}

mutation_partition partition_snapshot::squashed() const {
    return ::squashed<mutation_partition>(version(),
                               [] (const mutation_partition& mp) -> const mutation_partition& { return mp; },
                               [this] (mutation_partition& a, const mutation_partition& b) { a.apply(*_schema, b, *_schema); });
}

tombstone partition_entry::partition_tombstone() const {
    return ::squashed<tombstone>(_version,
        [] (const mutation_partition& mp) { return mp.partition_tombstone(); },
        [] (tombstone& a, tombstone b) { a.apply(b); });
}

partition_snapshot::~partition_snapshot() {
    with_allocator(_region.allocator(), [this] {
        if (_version && _version.is_unique_owner()) {
            auto v = &*_version;
            _version = {};
            remove_or_mark_as_unique_owner(v);
        } else if (_entry) {
            _entry->_snapshot = nullptr;
        }
    });
}

void partition_snapshot::merge_partition_versions() {
    if (_version && !_version.is_unique_owner()) {
        auto v = &*_version;
        _version = { };
        auto first_used = v;
        while (first_used->prev() && !first_used->is_referenced()) {
            first_used = first_used->prev();
        }

        auto current = first_used->next();
        while (current && !current->is_referenced()) {
            auto next = current->next();
            try {
                first_used->partition().apply(*_schema, std::move(current->partition()));
                current_allocator().destroy(current);
            } catch (...) {
                // Set _version so that the merge can be retried.
                _version = partition_version_ref(*current);
                throw;
            }
            current = next;
        }
    }
}

unsigned partition_snapshot::version_count()
{
    unsigned count = 0;
    for (auto&& v : versions()) {
        (void)v;
        count++;
    }
    return count;
}

partition_entry::partition_entry(mutation_partition mp)
{
    auto new_version = current_allocator().construct<partition_version>(std::move(mp));
    _version = partition_version_ref(*new_version);
}

partition_entry::~partition_entry() {
    if (!_version) {
        return;
    }
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_version.mark_as_unique_owner();
        _snapshot->_entry = nullptr;
    } else {
        auto v = &*_version;
        _version = { };
        remove_or_mark_as_unique_owner(v);
    }
}

void partition_entry::set_version(partition_version* new_version)
{
    if (_snapshot) {
        _snapshot->_version = std::move(_version);
        _snapshot->_entry = nullptr;
    }

    _snapshot = nullptr;
    _version = partition_version_ref(*new_version);
}

void partition_entry::apply(const schema& s, const mutation_partition& mp, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, mp, mp_schema);
    } else {
        mutation_partition mp1 = mp;
        if (s.version() != mp_schema.version()) {
            mp1.upgrade(mp_schema, s);
        }
        auto new_version = current_allocator().construct<partition_version>(std::move(mp1));
        new_version->insert_before(*_version);

        set_version(new_version);
    }
}

void partition_entry::apply(const schema& s, mutation_partition_view mpv, const schema& mp_schema)
{
    if (!_snapshot) {
        _version->partition().apply(s, mpv, mp_schema);
    } else {
        mutation_partition mp(s.shared_from_this());
        mp.apply(s, mpv, mp_schema);
        auto new_version = current_allocator().construct<partition_version>(std::move(mp));
        new_version->insert_before(*_version);

        set_version(new_version);
    }
}

// Iterates over all rows in mutation represented by partition_entry.
// It abstracts away the fact that rows may be spread across multiple versions.
class partition_entry::rows_iterator final {
    struct version {
        mutation_partition::rows_type::iterator current_row;
        mutation_partition::rows_type* rows;
        bool can_move;
        struct compare {
            const rows_entry::tri_compare& _cmp;
        public:
            explicit compare(const rows_entry::tri_compare& cmp) : _cmp(cmp) { }
            bool operator()(const version& a, const version& b) const {
                return _cmp(*a.current_row, *b.current_row) > 0;
            }
        };
    };
    const schema& _schema;
    rows_entry::tri_compare _rows_cmp;
    rows_entry::compare _rows_less_cmp;
    version::compare _version_cmp;
    std::vector<version> _heap;
    std::vector<version> _current_row;
public:
    rows_iterator(partition_version* version, const schema& schema)
        : _schema(schema)
        , _rows_cmp(schema)
        , _rows_less_cmp(schema)
        , _version_cmp(_rows_cmp)
    {
        bool can_move = true;
        while (version) {
            can_move &= !version->is_referenced();
            auto& rows = version->partition().clustered_rows();
            if (!rows.empty()) {
                _heap.push_back({rows.begin(), &rows, can_move});
            }
            version = version->next();
        }
        boost::range::make_heap(_heap, _version_cmp);
        move_to_next_row();
    }
    bool done() const {
        return _current_row.empty();
    }
    // Return clustering key of the current row in source.
    // Valid only when !is_dummy().
    const clustering_key& key() const {
        return _current_row[0].current_row->key();
    }
    bool is_dummy() const {
        return bool(_current_row[0].current_row->dummy());
    }
    template<typename RowConsumer>
    void consume_row(RowConsumer&& consumer) {
        assert(!_current_row.empty());
        // versions in _current_row are not ordered but it is not a problem
        // due to the fact that all rows are continuous.
        for (version& v : _current_row) {
            if (!v.can_move) {
                consumer(deletable_row(v.current_row->row()));
            } else {
                consumer(std::move(v.current_row->row()));
            }
        }
    }
    void remove_current_row_when_possible() {
        assert(!_current_row.empty());
        auto deleter = current_deleter<rows_entry>();
        for (version& v : _current_row) {
            if (v.can_move) {
                v.rows->erase_and_dispose(v.current_row, deleter);
            }
        }
    }
    void move_to_next_row() {
        _current_row.clear();
        while (!_heap.empty() &&
                (_current_row.empty() || _rows_cmp(*_current_row[0].current_row, *_heap[0].current_row) == 0)) {
            boost::range::pop_heap(_heap, _version_cmp);
            auto& curr = _heap.back();
            _current_row.push_back({curr.current_row, curr.rows, curr.can_move});
            ++curr.current_row;
            if (curr.current_row == curr.rows->end()) {
                _heap.pop_back();
            } else {
                boost::range::push_heap(_heap, _version_cmp);
            }
        }
    }
};

namespace {

// When applying partition_entry to an incomplete partition_entry this class is used to represent
// the target incomplete partition_entry. It encapsulates the logic needed for handling multiple versions.
class apply_incomplete_target final {
    struct version {
        mutation_partition::rows_type::iterator current_row;
        mutation_partition::rows_type* rows;
        size_t version_no;

        struct compare {
            const rows_entry::tri_compare& _cmp;
        public:
            explicit compare(const rows_entry::tri_compare& cmp) : _cmp(cmp) { }
            bool operator()(const version& a, const version& b) const {
                auto res = _cmp(*a.current_row, *b.current_row);
                return res > 0 || (res == 0 && a.version_no > b.version_no);
            }
        };
    };
    const schema& _schema;
    partition_entry& _pe;
    rows_entry::tri_compare _rows_cmp;
    rows_entry::compare _rows_less_cmp;
    version::compare _version_cmp;
    std::vector<version> _heap;
    mutation_partition::rows_type::iterator _next_in_latest_version;
public:
    apply_incomplete_target(partition_entry& pe, const schema& schema)
        : _schema(schema)
        , _pe(pe)
        , _rows_cmp(schema)
        , _rows_less_cmp(schema)
        , _version_cmp(_rows_cmp)
    {
        size_t version_no = 0;
        _next_in_latest_version = pe.version()->partition().clustered_rows().begin();
        for (auto&& v : pe.version()->elements_from_this()) {
            if (!v.partition().clustered_rows().empty()) {
                _heap.push_back({v.partition().clustered_rows().begin(), &v.partition().clustered_rows(), version_no});
            }
            ++version_no;
        }
        boost::range::make_heap(_heap, _version_cmp);
    }
    // Applies the row from source.
    // Must be called for rows with monotonic keys.
    // Weak exception guarantees. The target and source partitions are left
    // in a state such that the two still commute to the same value on retry.
    void apply(partition_entry::rows_iterator& src) {
        auto&& key = src.key();
        while (!_heap.empty() && _rows_less_cmp(*_heap[0].current_row, key)) {
            boost::range::pop_heap(_heap, _version_cmp);
            auto& curr = _heap.back();
            curr.current_row = curr.rows->lower_bound(key, _rows_less_cmp);
            if (curr.version_no == 0) {
                _next_in_latest_version = curr.current_row;
            }
            if (curr.current_row == curr.rows->end()) {
                _heap.pop_back();
            } else {
                boost::range::push_heap(_heap, _version_cmp);
            }
        }

        if (!_heap.empty()) {
            rows_entry& next_row = *_heap[0].current_row;
            if (_rows_cmp(key, next_row) == 0) {
                if (next_row.dummy()) {
                    return;
                }
            } else if (!next_row.continuous()) {
                return;
            }
        }

        mutation_partition::rows_type& rows = _pe.version()->partition().clustered_rows();
        if (_next_in_latest_version != rows.end() && _rows_cmp(key, *_next_in_latest_version) == 0) {
            src.consume_row([&] (deletable_row&& row) {
                _next_in_latest_version->row().apply(_schema, std::move(row));
            });
        } else {
            auto e = current_allocator().construct<rows_entry>(key);
            e->set_continuous(_heap.empty() ? is_continuous::yes : _heap[0].current_row->continuous());
            rows.insert_before(_next_in_latest_version, *e);
            src.consume_row([&] (deletable_row&& row) {
                e->row().apply(_schema, std::move(row));
            });
        }
    }
};

} // namespace

template<typename Func>
void partition_entry::with_detached_versions(Func&& func) {
    partition_version* current = &*_version;
    auto snapshot = _snapshot;
    if (snapshot) {
        snapshot->_version = std::move(_version);
        snapshot->_entry = nullptr;
        _snapshot = nullptr;
    }
    _version = { };

    auto revert = defer([&] {
        if (snapshot) {
            _snapshot = snapshot;
            snapshot->_entry = this;
            _version = std::move(snapshot->_version);
        } else {
            _version = partition_version_ref(*current);
        }
    });

    func(current);
}

void partition_entry::apply_to_incomplete(const schema& s, partition_entry&& pe, const schema& pe_schema)
{
    if (s.version() != pe_schema.version()) {
        partition_entry entry(pe.squashed(pe_schema.shared_from_this(), s.shared_from_this()));
        entry.with_detached_versions([&] (partition_version* v) {
            apply_to_incomplete(s, v);
        });
    } else {
        pe.with_detached_versions([&](partition_version* v) {
            apply_to_incomplete(s, v);
        });
    }
}

void partition_entry::apply_to_incomplete(const schema& s, partition_version* version) {
    partition_version& dst = open_version(s);

    bool can_move = true;
    auto current = version;
    bool static_row_continuous = dst.partition().static_row_continuous();
    while (current) {
        can_move &= !current->is_referenced();
        dst.partition().apply(current->partition().partition_tombstone());
        if (static_row_continuous) {
            row& static_row = dst.partition().static_row();
            if (can_move) {
                static_row.apply(s, column_kind::static_column, std::move(current->partition().static_row()));
            } else {
                static_row.apply(s, column_kind::static_column, current->partition().static_row());
            }
        }
        range_tombstone_list& tombstones = dst.partition().row_tombstones();
        if (can_move) {
            tombstones.apply_monotonically(s, std::move(current->partition().row_tombstones()));
        } else {
            tombstones.apply_monotonically(s, current->partition().row_tombstones());
        }
        current = current->next();
    }

    partition_entry::rows_iterator source(version, s);
    apply_incomplete_target target(*this, s);

    while (!source.done()) {
        if (!source.is_dummy()) {
            target.apply(source);
        }
        source.remove_current_row_when_possible();
        source.move_to_next_row();
    }
}

mutation_partition partition_entry::squashed(schema_ptr from, schema_ptr to)
{
    mutation_partition mp(to);
    mp.set_static_row_continuous(_version->partition().static_row_continuous());
    for (auto&& v : _version->all_elements()) {
        mp.apply(*to, v.partition(), *from);
    }
    return mp;
}

mutation_partition partition_entry::squashed(const schema& s)
{
    return squashed(s.shared_from_this(), s.shared_from_this());
}

void partition_entry::upgrade(schema_ptr from, schema_ptr to)
{
    auto new_version = current_allocator().construct<partition_version>(mutation_partition(to));
    new_version->partition().set_static_row_continuous(_version->partition().static_row_continuous());
    try {
        for (auto&& v : _version->all_elements()) {
            new_version->partition().apply(*to, v.partition(), *from);
        }
    } catch (...) {
        current_allocator().destroy(new_version);
        throw;
    }

    auto old_version = &*_version;
    set_version(new_version);
    remove_or_mark_as_unique_owner(old_version);
}

lw_shared_ptr<partition_snapshot> partition_entry::read(logalloc::region& r,
    schema_ptr entry_schema, partition_snapshot::phase_type phase)
{
    with_allocator(r.allocator(), [&] {
        open_version(*entry_schema, phase);
    });
    if (_snapshot) {
        return _snapshot->shared_from_this();
    } else {
        auto snp = make_lw_shared<partition_snapshot>(entry_schema, r, this, phase);
        _snapshot = snp.get();
        return snp;
    }
}

std::vector<range_tombstone>
partition_snapshot::range_tombstones(const ::schema& s, position_in_partition_view start, position_in_partition_view end)
{
    range_tombstone_list list(s);
    for (auto&& v : versions()) {
        for (auto&& rt : v.partition().row_tombstones().slice(s, start, end)) {
            list.apply(s, rt);
        }
    }
    return boost::copy_range<std::vector<range_tombstone>>(list);
}

std::ostream& operator<<(std::ostream& out, partition_entry& e) {
    out << "{";
    bool first = true;
    if (e._version) {
        for (const partition_version& v : e.versions()) {
            if (!first) {
                out << ", ";
            }
            if (v.is_referenced()) {
                out << "(*) ";
            }
            out << v.partition();
            first = false;
        }
    }
    out << "}";
    return out;
}

void partition_entry::evict() noexcept {
    if (!_version) {
        return;
    }
    for (auto&& v : versions()) {
        v.partition().evict();
    }
    current_allocator().invalidate_references();
}
