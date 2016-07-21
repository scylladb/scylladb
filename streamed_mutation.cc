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

#include "mutation.hh"
#include "streamed_mutation.hh"
#include "utils/move.hh"

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
    }
}

const clustering_key_prefix& mutation_fragment::key() const
{
    assert(has_key());
    struct get_key_visitor {
        const clustering_key_prefix& operator()(const clustering_row& cr) { return cr.key(); }
        const clustering_key_prefix& operator()(const range_tombstone& rt) { return rt.start; }
        const clustering_key_prefix& operator()(...) { abort(); }
    };
    return visit(get_key_visitor());
}

int mutation_fragment::bound_kind_weight() const {
    assert(has_key());
    struct get_bound_kind_weight {
        int operator()(const clustering_row&) { return 0; }
        int operator()(const range_tombstone& rt) { return weight(rt.start_kind); }
        int operator()(...) { abort(); }
    };
    return visit(get_bound_kind_weight());
}

void mutation_fragment::apply(const schema& s, mutation_fragment&& mf)
{
    assert(_kind == mf._kind);
    assert(!is_range_tombstone());
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

position_in_partition mutation_fragment::position() const
{
    struct get_position {
        position_in_partition operator()(const static_row& sr) { return sr.position(); }
        position_in_partition operator()(const clustering_row& cr) { return cr.position(); }
        position_in_partition operator()(const range_tombstone& rt) {
            return position_in_partition(position_in_partition::range_tombstone_tag_t(), rt.start_bound());
        }
    };
    return visit(get_position());
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
    }
    abort();
}

streamed_mutation streamed_mutation_from_mutation(mutation m)
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
            auto re = crs.unlink_leftmost_without_rebalance();
            if (re) {
                _cr = mutation_fragment(std::move(*re));
                current_deleter<rows_entry>()(re);
            }
        }
        void prepare_next_range_tombstone() {
            auto& rts = _mutation.partition().row_tombstones().tombstones();
            auto rt = rts.unlink_leftmost_without_rebalance();
            if (rt) {
                _rt = mutation_fragment(std::move(*rt));
                current_deleter<range_tombstone>()(rt);
            }
        }
        mutation_fragment_opt read_next() {
            if (_cr && (!_rt || _cmp(*_cr, *_rt))) {
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
            prepare_next_clustering_row();
            prepare_next_range_tombstone();

            do_fill_buffer();
        }

        virtual future<> fill_buffer() override {
            do_fill_buffer();
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
            _end_of_stream = true;
            return;
        }

        position_in_partition::less_compare cmp(*_schema);
        auto heap_compare = [&] (auto& a, auto& b) { return cmp(b.row, a.row); };

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
            if (cmp(result, _readers.front().row)) {
                break;
            }
            boost::range::pop_heap(_readers, heap_compare);
            if (result.is_range_tombstone()) {
                auto remainder = result.as_range_tombstone().apply(*_schema, std::move(_readers.back().row.as_range_tombstone()));
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
        auto heap_compare = [&] (auto& a, auto& b) { return cmp(b.row, a.row); };

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
        if (_next_readers.empty()) {
            return make_ready_future<>();
        }
        while (!is_end_of_stream() && !is_buffer_full()) {
            std::vector<future<>> more_data;
            for (auto& rd : _next_readers) {
                if (rd->is_buffer_empty() && !rd->is_end_of_stream()) {
                    more_data.emplace_back(rd->fill_buffer());
                }
            }
            if (!more_data.empty()) {
                return parallel_for_each(std::move(more_data), [] (auto& f) { return std::move(f); }).then([this] { return fill_buffer(); });
            }
            do_fill_buffer();
        }
        return make_ready_future<>();
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
        return !_cmp(re, _list.begin()->start_bound()) ? do_get_next() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next(const mutation_fragment& mf)
{
    if (!_list.empty()) {
        return !_cmp(mf, *_list.begin()) ? do_get_next() : mutation_fragment_opt();
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

streamed_mutation reverse_streamed_mutation(streamed_mutation sm) {
    class reversing_steamed_mutation final : public streamed_mutation::impl {
        streamed_mutation_opt _source;
        mutation_fragment_opt _static_row;
        std::stack<mutation_fragment> _mutation_fragments;
    private:
        future<> consume_source() {
            return repeat([&] {
                return (*_source)().then([&] (mutation_fragment_opt mf) {
                    if (!mf) {
                        return stop_iteration::yes;
                    } else if (mf->is_static_row()) {
                        _static_row = std::move(mf);
                    } else {
                        if (mf->is_range_tombstone()) {
                            mf->as_range_tombstone().flip();
                        }
                        _mutation_fragments.emplace(std::move(*mf));
                    }
                    return stop_iteration::no;
                });
            }).then([&] {
                _source = { };
            });
        }
    public:
        explicit reversing_steamed_mutation(streamed_mutation sm)
            : streamed_mutation::impl(sm.schema(), sm.decorated_key(), sm.partition_tombstone())
            , _source(std::move(sm))
        { }

        virtual future<> fill_buffer() override {
            if (_source) {
                return consume_source().then([this] { return fill_buffer(); });
            }
            if (_static_row) {
                push_mutation_fragment(std::move(*_static_row));
                _static_row = { };
            }
            while (!is_end_of_stream() && !is_buffer_full()) {
                if (_mutation_fragments.empty()) {
                    _end_of_stream = true;
                } else {
                    push_mutation_fragment(std::move(_mutation_fragments.top()));
                    _mutation_fragments.pop();
                }
            }
            return make_ready_future<>();
        }
    };

    return make_streamed_mutation<reversing_steamed_mutation>(std::move(sm));
};