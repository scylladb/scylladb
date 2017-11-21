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

#include <stack>
#include <boost/range/algorithm/heap_algorithm.hpp>
#include <seastar/util/defer.hh>

#include "mutation.hh"
#include "streamed_mutation.hh"
#include "utils/move.hh"

std::ostream&
operator<<(std::ostream& os, const clustering_row& row) {
    return os << "{clustering_row: ck " << row._ck << " t " << row._t << " row_marker " << row._marker << " cells " << row._cells << "}";
}

std::ostream&
operator<<(std::ostream& os, const static_row& row) {
    return os << "{static_row: "<< row._cells << "}";
}

std::ostream&
operator<<(std::ostream& os, const partition_start& ph) {
    return os << "{partition_start: pk "<< ph._key << " partition_tombstone " << ph._partition_tombstone << "}";
}

std::ostream&
operator<<(std::ostream& os, const partition_end& eop) {
    return os << "{partition_end}";
}

std::ostream& operator<<(std::ostream& out, partition_region r) {
    switch (r) {
        case partition_region::partition_start: out << "partition_start"; break;
        case partition_region::static_row: out << "static_row"; break;
        case partition_region::clustered: out << "clustered"; break;
        case partition_region::partition_end: out << "partition_end"; break;
    }
    return out;
}

std::ostream& operator<<(std::ostream& out, position_in_partition_view pos) {
    out << "{position: type " << pos._type << ", bound_weight " << pos._bound_weight << ", key ";
    if (pos._ck) {
        out << *pos._ck;
    } else {
        out << "null";
    }
    return out << "}";
}

std::ostream& operator<<(std::ostream& out, const position_in_partition& pos) {
    return out << static_cast<position_in_partition_view>(pos);
}

std::ostream& operator<<(std::ostream& out, const position_range& range) {
    return out << "{" << range.start() << ", " << range.end() << "}";
}

mutation_fragment::mutation_fragment(static_row&& r)
    : _kind(kind::static_row), _data(std::make_unique<data>())
{
    new (&_data->_static_row) static_row(std::move(r));
}

mutation_fragment::mutation_fragment(clustering_row&& r)
    : _kind(kind::clustering_row), _data(std::make_unique<data>())
{
    new (&_data->_clustering_row) clustering_row(std::move(r));
}

mutation_fragment::mutation_fragment(range_tombstone&& r)
    : _kind(kind::range_tombstone), _data(std::make_unique<data>())
{
    new (&_data->_range_tombstone) range_tombstone(std::move(r));
}

mutation_fragment::mutation_fragment(partition_start&& r)
        : _kind(kind::partition_start), _data(std::make_unique<data>())
{
    new (&_data->_partition_start) partition_start(std::move(r));
}

mutation_fragment::mutation_fragment(partition_end&& r)
        : _kind(kind::partition_end), _data(std::make_unique<data>())
{
    new (&_data->_partition_end) partition_end(std::move(r));
}

void mutation_fragment::destroy_data() noexcept
{
    switch (_kind) {
    case kind::static_row:
        _data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.~clustering_row();
        break;
    case kind::range_tombstone:
        _data->_range_tombstone.~range_tombstone();
        break;
    case kind::partition_start:
        _data->_partition_start.~partition_start();
        break;
    case kind::partition_end:
        _data->_partition_end.~partition_end();
        break;
    }
}

namespace {

struct get_key_visitor {
    const clustering_key_prefix& operator()(const clustering_row& cr) { return cr.key(); }
    const clustering_key_prefix& operator()(const range_tombstone& rt) { return rt.start; }
    template <typename T>
    const clustering_key_prefix& operator()(const T&) { abort(); }
};

}

const clustering_key_prefix& mutation_fragment::key() const
{
    assert(has_key());
    return visit(get_key_visitor());
}

void mutation_fragment::apply(const schema& s, mutation_fragment&& mf)
{
    assert(_kind == mf._kind);
    assert(!is_range_tombstone());
    _data->_size_in_bytes = stdx::nullopt;
    switch (_kind) {
    case kind::static_row:
        _data->_static_row.apply(s, std::move(mf._data->_static_row));
        mf._data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.apply(s, std::move(mf._data->_clustering_row));
        mf._data->_clustering_row.~clustering_row();
        break;
    default: abort();
    }
    mf._data.reset();
}

position_in_partition_view mutation_fragment::position() const
{
    return visit([] (auto& mf) -> position_in_partition_view { return mf.position(); });
}

std::ostream& operator<<(std::ostream& os, const streamed_mutation& sm) {
    auto& s = *sm.schema();
    fprint(os, "{%s.%s key %s streamed mutation}", s.ks_name(), s.cf_name(), sm.decorated_key());
    return os;
}

std::ostream& operator<<(std::ostream& os, mutation_fragment::kind k)
{
    switch (k) {
    case mutation_fragment::kind::static_row: return os << "static row";
    case mutation_fragment::kind::clustering_row: return os << "clustering row";
    case mutation_fragment::kind::range_tombstone: return os << "range tombstone";
    case mutation_fragment::kind::partition_start: return os << "partition start";
    case mutation_fragment::kind::partition_end: return os << "partition end";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const mutation_fragment& mf) {
    os << "{mutation_fragment: " << mf._kind << " " << mf.position() << " ";
    mf.visit([&os] (const auto& what) -> void {
       os << what;
    });
    os << "}";
    return os;
}

streamed_mutation make_empty_streamed_mutation(schema_ptr s, dht::decorated_key key, streamed_mutation::forwarding fwd) {
    return streamed_mutation_from_mutation(mutation(std::move(key), std::move(s)), fwd);
}

streamed_mutation streamed_mutation_from_mutation(mutation m, streamed_mutation::forwarding fwd)
{
    class reader final : public streamed_mutation::impl {
        mutation _mutation;
        position_in_partition::less_compare _cmp;
        bool _static_row_done = false;
        mutation_fragment_opt _rt;
        mutation_fragment_opt _cr;
    private:
        void prepare_next_clustering_row() {
            auto& crs = _mutation.partition().clustered_rows();
            while (true) {
                auto re = crs.unlink_leftmost_without_rebalance();
                if (!re) {
                    break;
                }
                auto re_deleter = defer([re] { current_deleter<rows_entry>()(re); });
                if (!re->dummy()) {
                    _cr = mutation_fragment(std::move(*re));
                    break;
                }
            }
        }
        void prepare_next_range_tombstone() {
            auto& rts = _mutation.partition().row_tombstones().tombstones();
            auto rt = rts.unlink_leftmost_without_rebalance();
            if (rt) {
                auto rt_deleter = defer([rt] { current_deleter<range_tombstone>()(rt); });
                _rt = mutation_fragment(std::move(*rt));
            }
        }
        mutation_fragment_opt read_next() {
            if (_cr && (!_rt || _cmp(_cr->position(), _rt->position()))) {
                auto cr = move_and_disengage(_cr);
                prepare_next_clustering_row();
                return cr;
            } else if (_rt) {
                auto rt = move_and_disengage(_rt);
                prepare_next_range_tombstone();
                return rt;
            }
            return { };
        }
    private:
        void do_fill_buffer() {
            if (!_static_row_done) {
                _static_row_done = true;
                if (!_mutation.partition().static_row().empty()) {
                    push_mutation_fragment(static_row(std::move(_mutation.partition().static_row())));
                }
            }
            while (!is_end_of_stream() && !is_buffer_full()) {
                auto mfopt = read_next();
                if (mfopt) {
                    push_mutation_fragment(std::move(*mfopt));
                } else {
                    _end_of_stream = true;
                }
            }
        }
    public:
        explicit reader(mutation m)
            : streamed_mutation::impl(m.schema(), m.decorated_key(), m.partition().partition_tombstone())
            , _mutation(std::move(m))
            , _cmp(*_mutation.schema())
        {
            auto mutation_destroyer = defer([this] { destroy_mutation(); });

            prepare_next_clustering_row();
            prepare_next_range_tombstone();

            do_fill_buffer();

            mutation_destroyer.cancel();
        }

        void destroy_mutation() noexcept {
            // After unlink_leftmost_without_rebalance() was called on a bi::set
            // we need to complete destroying the tree using that function.
            // clear_and_dispose() used by mutation_partition destructor won't
            // work properly.

            auto& crs = _mutation.partition().clustered_rows();
            auto re = crs.unlink_leftmost_without_rebalance();
            while (re) {
                current_deleter<rows_entry>()(re);
                re = crs.unlink_leftmost_without_rebalance();
            }

            auto& rts = _mutation.partition().row_tombstones().tombstones();
            auto rt = rts.unlink_leftmost_without_rebalance();
            while (rt) {
                current_deleter<range_tombstone>()(rt);
                rt = rts.unlink_leftmost_without_rebalance();
            }
        }

        ~reader() {
            destroy_mutation();
        }

        virtual future<> fill_buffer() override {
            do_fill_buffer();
            return make_ready_future<>();
        }
    };

    auto sm = make_streamed_mutation<reader>(std::move(m));
    if (fwd) {
        return make_forwardable(std::move(sm)); // FIXME: optimize
    }
    return std::move(sm);
}

streamed_mutation streamed_mutation_from_forwarding_streamed_mutation(streamed_mutation&& sm)
{
    class reader final : public streamed_mutation::impl {
        streamed_mutation _sm;
        bool _static_row_done = false;
    public:
        explicit reader(streamed_mutation&& sm)
            : streamed_mutation::impl(sm.schema(), sm.decorated_key(), sm.partition_tombstone())
            , _sm(std::move(sm))
        { }

        virtual future<> fill_buffer() override {
            if (!_static_row_done) {
                _static_row_done = true;
                return _sm().then([this] (auto&& mf) {
                    if (mf) {
                        this->push_mutation_fragment(std::move(*mf));
                    }
                    return _sm.fast_forward_to(query::clustering_range{}).then([this] {
                        return this->fill_buffer();
                    });
                });
            }
            return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
                return _sm().then([this] (auto&& mf) {
                    if (mf) {
                        this->push_mutation_fragment(std::move(*mf));
                    } else {
                        _end_of_stream = true;
                    }
                });
            });
        }
    };

    return make_streamed_mutation<reader>(std::move(sm));
}

streamed_mutation make_forwardable(streamed_mutation m) {
    class reader : public streamed_mutation::impl {
        streamed_mutation _sm;
        position_range _current = position_range::for_static_row();
        mutation_fragment_opt _next;
    private:
        // When resolves, _next is engaged or _end_of_stream is set.
        future<> ensure_next() {
            if (_next) {
                return make_ready_future<>();
            }
            return _sm().then([this] (auto&& mfo) {
                _next = std::move(mfo);
                if (!_next) {
                    _end_of_stream = true;
                }
            });
        }
    public:
        explicit reader(streamed_mutation sm)
            : impl(sm.schema(), std::move(sm.decorated_key()), sm.partition_tombstone())
            , _sm(std::move(sm))
        { }

        virtual future<> fill_buffer() override {
            return repeat([this] {
                if (is_buffer_full()) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                return ensure_next().then([this] {
                    if (is_end_of_stream()) {
                        return stop_iteration::yes;
                    }
                    position_in_partition::less_compare cmp(*_sm.schema());
                    if (!cmp(_next->position(), _current.end())) {
                        _end_of_stream = true;
                        // keep _next, it may be relevant for next range
                        return stop_iteration::yes;
                    }
                    if (_next->relevant_for_range(*_schema, _current.start())) {
                        push_mutation_fragment(std::move(*_next));
                    }
                    _next = {};
                    return stop_iteration::no;
                });
            });
        }

        virtual future<> fast_forward_to(position_range pr) override {
            _current = std::move(pr);
            _end_of_stream = false;
            forward_buffer_to(_current.start());
            return make_ready_future<>();
        }
    };

    return make_streamed_mutation<reader>(std::move(m));
}

class mutation_merger final : public streamed_mutation::impl {
    std::vector<streamed_mutation> _original_readers;
    std::vector<streamed_mutation*> _next_readers;
    // FIXME: do not store all in-flight clustering rows in memory
    struct row_and_reader {
        mutation_fragment row;
        streamed_mutation* reader;
    };
    std::vector<row_and_reader> _readers;
    range_tombstone_stream _deferred_tombstones;
private:
    void read_next() {
        if (_readers.empty()) {
            auto rt = _deferred_tombstones.get_next();
            if (rt) {
                push_mutation_fragment(std::move(*rt));
            } else {
                _end_of_stream = true;
            }
            return;
        }

        position_in_partition::less_compare cmp(*_schema);
        auto heap_compare = [&] (auto& a, auto& b) { return cmp(b.row.position(), a.row.position()); };

        auto result = [&] {
            auto rt = _deferred_tombstones.get_next(_readers.front().row);
            if (rt) {
                return std::move(*rt);
            }
            boost::range::pop_heap(_readers, heap_compare);
            auto mf = std::move(_readers.back().row);
            _next_readers.emplace_back(std::move(_readers.back().reader));
            _readers.pop_back();
            return std::move(mf);
        }();

        while (!_readers.empty()) {
            if (cmp(result.position(), _readers.front().row.position())) {
                break;
            }
            boost::range::pop_heap(_readers, heap_compare);
            if (result.is_range_tombstone()) {
                auto remainder = result.as_mutable_range_tombstone().apply(*_schema, std::move(_readers.back().row).as_range_tombstone());
                if (remainder) {
                    _deferred_tombstones.apply(std::move(*remainder));
                }
            } else {
                result.apply(*_schema, std::move(_readers.back().row));
            }
            _next_readers.emplace_back(std::move(_readers.back().reader));
            _readers.pop_back();
        }

        push_mutation_fragment(std::move(result));
    }

    void do_fill_buffer() {
        position_in_partition::less_compare cmp(*_schema);
        auto heap_compare = [&] (auto& a, auto& b) { return cmp(b.row.position(), a.row.position()); };

        for (auto& rd : _next_readers) {
            if (rd->is_buffer_empty()) {
                assert(rd->is_end_of_stream());
                continue;
            }
            _readers.emplace_back(row_and_reader { rd->pop_mutation_fragment(), std::move(rd) });
            boost::range::push_heap(_readers, heap_compare);
        }
        _next_readers.clear();

        read_next();
    }
    void prefill_buffer() {
        while (!is_end_of_stream() && !is_buffer_full()) {
            for (auto& rd : _next_readers) {
                if (rd->is_buffer_empty() && !rd->is_end_of_stream()) {
                    return;
                }
            }
            do_fill_buffer();
        }
    }

    static tombstone merge_partition_tombstones(const std::vector<streamed_mutation>& readers) {
        tombstone t;
        for (auto& r : readers) {
            t.apply(r.partition_tombstone());
        }
        return t;
    }
protected:
    virtual future<> fill_buffer() override  {
        while (!is_end_of_stream() && !is_buffer_full()) {
            std::vector<future<>> more_data;
            for (auto& rd : _next_readers) {
                if (rd->is_buffer_empty() && !rd->is_end_of_stream()) {
                    auto f = rd->fill_buffer();
                    if (!f.available() || f.failed()) {
                        more_data.emplace_back(std::move(f));
                    }
                }
            }
            if (!more_data.empty()) {
                return parallel_for_each(std::move(more_data), [] (auto& f) { return std::move(f); }).then([this] { return fill_buffer(); });
            }
            do_fill_buffer();
        }
        return make_ready_future<>();
    }
    virtual future<> fast_forward_to(position_range pr) override {
        _deferred_tombstones.forward_to(pr.start());
        forward_buffer_to(pr.start());
        _end_of_stream = false;

        _next_readers.clear();
        _readers.clear();
        return parallel_for_each(_original_readers, [this, &pr] (streamed_mutation& rd) {
            _next_readers.emplace_back(&rd);
            return rd.fast_forward_to(pr);
        });
    }
public:
    mutation_merger(schema_ptr s, dht::decorated_key dk, std::vector<streamed_mutation> readers)
        : streamed_mutation::impl(s, std::move(dk), merge_partition_tombstones(readers))
        , _original_readers(std::move(readers)), _deferred_tombstones(*s)
    {
        _next_readers.reserve(_original_readers.size());
        _readers.reserve(_original_readers.size());
        for (auto& rd : _original_readers) {
            _next_readers.emplace_back(&rd);
        }
        prefill_buffer();
    }
};

streamed_mutation merge_mutations(std::vector<streamed_mutation> ms)
{
    assert(!ms.empty());
    if (ms.size() == 1) {
        return std::move(ms[0]);
    }
    return make_streamed_mutation<mutation_merger>(ms.back().schema(), ms.back().decorated_key(), std::move(ms));
}

mutation_fragment_opt range_tombstone_stream::do_get_next()
{
    auto& rt = *_list.tombstones().begin();
    auto mf = mutation_fragment(std::move(rt));
    _list.tombstones().erase(_list.begin());
    current_deleter<range_tombstone>()(&rt);
    return mf;
}

mutation_fragment_opt range_tombstone_stream::get_next(const rows_entry& re)
{
    if (!_list.empty()) {
        return !_cmp(re.position(), _list.begin()->position()) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next(const mutation_fragment& mf)
{
    if (!_list.empty()) {
        return !_cmp(mf.position(), _list.begin()->position()) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next(position_in_partition_view upper_bound)
{
    if (!_list.empty()) {
        return _cmp(_list.begin()->position(), upper_bound) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next()
{
    if (!_list.empty()) {
        return do_get_next();
    }
    return { };
}

void range_tombstone_stream::forward_to(position_in_partition_view pos) {
    _list.erase_where([this, &pos] (const range_tombstone& rt) {
        return !_cmp(pos, rt.end_position());
    });
}

void range_tombstone_stream::apply(const range_tombstone_list& list, const query::clustering_range& range) {
    for (const range_tombstone& rt : list.slice(_schema, range)) {
        _list.apply(_schema, rt);
    }
}

void range_tombstone_stream::reset() {
    _inside_range_tombstone = false;
    _list.clear();
}

streamed_mutation streamed_mutation_returning(schema_ptr s, dht::decorated_key key, std::vector<mutation_fragment> frags, tombstone t) {
    class reader : public streamed_mutation::impl {
    public:
        explicit reader(schema_ptr s, dht::decorated_key key, std::vector<mutation_fragment> frags, tombstone t)
            : streamed_mutation::impl(std::move(s), std::move(key), t)
        {
            for (auto&& f : frags) {
                push_mutation_fragment(std::move(f));
            }
            _end_of_stream = true;
        }

        virtual future<> fill_buffer() override {
            return make_ready_future<>();
        }
    };
    return make_streamed_mutation<reader>(std::move(s), std::move(key), std::move(frags), t);
}

position_range position_range::from_range(const query::clustering_range& range) {
    auto bv_range = bound_view::from_range(range);
    return {
        position_in_partition(position_in_partition::range_tag_t(), bv_range.first),
        position_in_partition(position_in_partition::range_tag_t(), bv_range.second)
    };
}

position_range::position_range(const query::clustering_range& range)
    : position_range(from_range(range))
{ }

position_range::position_range(query::clustering_range&& range)
    : position_range(range) // FIXME: optimize
{ }

void streamed_mutation::impl::forward_buffer_to(const position_in_partition& pos) {
    _buffer.erase(std::remove_if(_buffer.begin(), _buffer.end(), [this, &pos] (mutation_fragment& f) {
        return !f.relevant_for_range_assuming_after(*_schema, pos);
    }), _buffer.end());

    _buffer_size = 0;
    for (auto&& f : _buffer) {
        _buffer_size += f.memory_usage();
    }
}

bool mutation_fragment::relevant_for_range(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare cmp(s);
    if (!cmp(position(), pos)) {
        return true;
    }
    return relevant_for_range_assuming_after(s, pos);
}

bool mutation_fragment::relevant_for_range_assuming_after(const schema& s, position_in_partition_view pos) const {
    position_in_partition::less_compare cmp(s);
    // Range tombstones overlapping with the new range are let in
    return is_range_tombstone() && cmp(pos, as_range_tombstone().end_position());
}

std::ostream& operator<<(std::ostream& out, const range_tombstone_stream& rtl) {
    return out << rtl._list;
}
