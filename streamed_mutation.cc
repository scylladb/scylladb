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

mutation_fragment::mutation_fragment(range_tombstone_begin&& r)
    : _kind(kind::range_tombstone_begin), _data(std::make_unique<data>())
{
    new (&_data->_range_tombstone_begin) range_tombstone_begin(std::move(r));
}

mutation_fragment::mutation_fragment(range_tombstone_end&& r)
    : _kind(kind::range_tombstone_end), _data(std::make_unique<data>())
{
    new (&_data->_range_tombstone_end) range_tombstone_end(std::move(r));
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
    case kind::range_tombstone_begin:
        _data->_range_tombstone_begin.~range_tombstone_begin();
        break;
    case kind::range_tombstone_end:
        _data->_range_tombstone_end.~range_tombstone_end();
        break;
    }
}

// C++ does not allow local classes that have template member. The rightful
// place of this class is inside mutation_fragment::key().
struct get_mutation_fragment_key_visitor {
    template<typename T>
    const clustering_key_prefix& operator()(const T& mf) { return mf.key(); }
    const clustering_key_prefix& operator()(const static_row& sr) { abort(); }
};

const clustering_key_prefix& mutation_fragment::key() const
{
    assert(has_key());
    return visit(get_mutation_fragment_key_visitor());
}

int mutation_fragment::bound_kind_weight() const {
    assert(has_key());
    struct get_bound_kind_weight {
        int operator()(const clustering_row&) { return 0; }
        int operator()(const range_tombstone_begin& rtb) { return weight(rtb.kind()); }
        int operator()(const range_tombstone_end& rte) { return weight(rte.kind()); }
        int operator()(...) { abort(); }
    };
    return visit(get_bound_kind_weight());
}

void mutation_fragment::apply(const schema& s, mutation_fragment&& mf)
{
    assert(_kind == mf._kind);
    switch (_kind) {
    case kind::static_row:
        _data->_static_row.apply(s, std::move(mf._data->_static_row));
        mf._data->_static_row.~static_row();
        break;
    case kind::clustering_row:
        _data->_clustering_row.apply(s, std::move(mf._data->_clustering_row));
        mf._data->_clustering_row.~clustering_row();
        break;
    case kind::range_tombstone_begin:
        _data->_range_tombstone_begin.apply(std::move(mf._data->_range_tombstone_begin));
        mf._data->_range_tombstone_begin.~range_tombstone_begin();
        break;
    case kind::range_tombstone_end:
        mf._data->_range_tombstone_end.~range_tombstone_end();
        break;
    }
    mf._data.reset();
}

position_in_partition mutation_fragment::position() const
{
    return visit([] (auto& mf) { return mf.position(); });
}

std::ostream& operator<<(std::ostream& os, const streamed_mutation& sm) {
    auto& s = *sm.schema();
    fprint(os, "{%s.%s key %s streamed mutation}", s.ks_name(), s.cf_name(), sm.decorated_key());
    return os;
}

streamed_mutation streamed_mutation_from_mutation(mutation m)
{
    class reader final : public streamed_mutation::impl {
        mutation _mutation;
        bound_view::compare _cmp;
        bool _static_row_done = false;
        range_tombstone::container_type::const_iterator _rt_it;
        range_tombstone::container_type::const_iterator _rt_end;
        mutation_partition::rows_type::const_iterator _cr_it;
        mutation_partition::rows_type::const_iterator _cr_end;
        stdx::optional<range_tombstone_end> _range_tombstone_end;
    private:
        mutation_fragment_opt read_next() {
            if (_cr_it != _cr_end) {
                bool return_ck = true;
                if (_range_tombstone_end) {
                    return_ck = _cmp(_cr_it->key(), _range_tombstone_end->bound());
                } else if (_rt_it != _rt_end) {
                    return_ck = _cmp(_cr_it->key(), _rt_it->start_bound());
                }
                if (return_ck) {
                    return mutation_fragment(std::move(*_cr_it++));
                }
            }
            if (_range_tombstone_end) {
                auto mf = mutation_fragment(std::move(*_range_tombstone_end));
                _range_tombstone_end = { };
                return mf;
            } else if (_rt_it != _rt_end) {
                auto rt = std::move(*_rt_it++);
                _range_tombstone_end = range_tombstone_end(std::move(rt.end), rt.end_kind);
                mutation_fragment mf = range_tombstone_begin(std::move(rt.start), rt.start_kind, rt.tomb);
                return mf;
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
            _rt_it = _mutation.partition().row_tombstones().begin();
            _cr_it = _mutation.partition().clustered_rows().begin();
            _rt_end = _mutation.partition().row_tombstones().end();
            _cr_end = _mutation.partition().clustered_rows().end();

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
    struct streamed_reader {
        tombstone current_tombstone;
        streamed_mutation* reader;
    };
    std::vector<streamed_reader> _next_readers;
    // FIXME: do not store all in-flight clustering rows in memory
    struct row_and_reader {
        mutation_fragment row;
        streamed_reader reader;
    };
    std::vector<row_and_reader> _readers;
    tombstone _current_tombstone;
private:
    static void update_current_tombstone(streamed_reader& sr, mutation_fragment& mf) {
        if (mf.is_range_tombstone_begin()) {
            assert(!sr.current_tombstone);
            sr.current_tombstone = mf.as_range_tombstone_begin().tomb();
        } else if (mf.is_range_tombstone_end()) {
            assert(sr.current_tombstone);
            sr.current_tombstone = { };
        }
    }

    void read_next() {
        if (_readers.empty()) {
            _end_of_stream = true;
            return;
        }

        position_in_partition::less_compare cmp(*_schema);
        auto heap_compare = [&] (auto& a, auto& b) { return cmp(b.row, a.row); };

        boost::range::pop_heap(_readers, heap_compare);
        auto result = std::move(_readers.back().row);
        update_current_tombstone(_readers.back().reader, result);
        _next_readers.emplace_back(std::move(_readers.back().reader));
        _readers.pop_back();

        while (!_readers.empty()) {
            if (cmp(result, _readers.front().row)) {
                break;
            }
            boost::range::pop_heap(_readers, heap_compare);
            update_current_tombstone(_readers.back().reader, _readers.back().row);
            result.apply(*_schema, std::move(_readers.back().row));
            _next_readers.emplace_back(std::move(_readers.back().reader));
            _readers.pop_back();
        }

        bool can_emit_result = true;
        if (result.is_range_tombstone_begin()) {
            auto new_t = result.as_range_tombstone_begin().tomb();
            can_emit_result = _current_tombstone < new_t;
            if (can_emit_result) {
                if (_current_tombstone) {
                    auto& rtb = result.as_range_tombstone_begin();
                    auto rte = range_tombstone_end(rtb.key(), invert_kind(rtb.kind()));
                    push_mutation_fragment(std::move(rte));
                }
                push_mutation_fragment(std::move(result));
                _current_tombstone = new_t;
            }
        } else if (result.is_range_tombstone_end()) {
            tombstone new_t;
            for (auto& r_a_r : _readers) {
                new_t = std::max(new_t, r_a_r.reader.current_tombstone);
            }
            for (auto& r : _next_readers) {
                new_t = std::max(new_t, r.current_tombstone);
            }
            can_emit_result = new_t != _current_tombstone;
            if (can_emit_result) {
                if (new_t) {
                    auto& rte = result.as_range_tombstone_end();
                    auto rtb = range_tombstone_begin(rte.key(), invert_kind(rte.kind()), new_t);
                    push_mutation_fragment(std::move(result));
                    push_mutation_fragment(std::move(rtb));
                } else {
                    push_mutation_fragment(std::move(result));
                }
                _current_tombstone = new_t;
            }
        } else {
            push_mutation_fragment(std::move(result));
        }
    }

    void do_fill_buffer() {
        position_in_partition::less_compare cmp(*_schema);
        auto heap_compare = [&] (auto& a, auto& b) { return cmp(b.row, a.row); };

        for (auto& rd : _next_readers) {
            if (rd.reader->is_buffer_empty()) {
                assert(rd.reader->is_end_of_stream());
                continue;
            }
            _readers.emplace_back(row_and_reader { rd.reader->pop_mutation_fragment(), std::move(rd) });
            boost::range::push_heap(_readers, heap_compare);
        }
        _next_readers.clear();

        read_next();
    }
    void prefill_buffer() {
        while (!is_end_of_stream() && !is_buffer_full()) {
            for (auto& rd : _next_readers) {
                if (rd.reader->is_buffer_empty() && !rd.reader->is_end_of_stream()) {
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
                if (rd.reader->is_buffer_empty() && !rd.reader->is_end_of_stream()) {
                    more_data.emplace_back(rd.reader->fill_buffer());
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
        : streamed_mutation::impl(std::move(s), std::move(dk), merge_partition_tombstones(readers))
        , _original_readers(std::move(readers))
    {
        _next_readers.reserve(_original_readers.size());
        _readers.reserve(_original_readers.size());
        for (auto& rd : _original_readers) {
            _next_readers.emplace_back(streamed_reader { { }, &rd });
        }
        prefill_buffer();
    }
};

streamed_mutation merge_mutations(std::vector<streamed_mutation> ms)
{
    assert(!ms.empty());
    return make_streamed_mutation<mutation_merger>(ms.back().schema(), ms.back().decorated_key(), std::move(ms));
}

mutation_fragment_opt range_tombstone_stream::get_next_start()
{
    auto& rt = *_list.tombstones().begin();
    auto mf = mutation_fragment(range_tombstone_begin(std::move(rt.start), rt.start_kind, rt.tomb));
    rt.start = clustering_key::make_empty();
    rt.start_kind = bound_kind::incl_start;
    _inside_range_tombstone = true;
    return mf;
}

mutation_fragment_opt range_tombstone_stream::get_next_end()
{
    auto& rt = *_list.tombstones().begin();
    auto mf = mutation_fragment(range_tombstone_end(std::move(rt.end), rt.end_kind));
    _list.tombstones().erase(_list.begin());
    current_deleter<range_tombstone>()(&rt);
    _inside_range_tombstone = false;
    return mf;
}

mutation_fragment_opt range_tombstone_stream::get_next(const rows_entry& re)
{
    if (_inside_range_tombstone) {
        return _cmp(_list.begin()->end_bound(), re) ? get_next_end() : mutation_fragment_opt();
    } else if (!_list.empty()) {
        return _cmp(_list.begin()->start_bound(), re) ? get_next_start() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next(const mutation_fragment& mf)
{
    if (_inside_range_tombstone) {
        return _cmp(_list.begin()->end_bound(), mf) ? get_next_end() : mutation_fragment_opt();
    } else if (!_list.empty()) {
        return _cmp(_list.begin()->start_bound(), mf) ? get_next_start() : mutation_fragment_opt();
    }
    return { };
}

mutation_fragment_opt range_tombstone_stream::get_next()
{
    if (_inside_range_tombstone) {
        return get_next_end();
    } else if (!_list.empty()) {
        return get_next_start();
    }
    return { };
}

streamed_mutation reverse_streamed_mutation(streamed_mutation sm) {
    class reversing_steamed_mutation final : public streamed_mutation::impl {
        streamed_mutation_opt _source;
        mutation_fragment_opt _static_row;
        std::stack<mutation_fragment> _mutation_fragments;
        tombstone _current_tombstone;
    private:
        future<> consume_source() {
            return repeat([&] {
                return (*_source)().then([&] (mutation_fragment_opt mf) {
                    if (!mf) {
                        return stop_iteration::yes;
                    } else if (mf->is_static_row()) {
                        _static_row = std::move(mf);
                    } else if (mf->is_range_tombstone_begin()) {
                        auto& rtb = mf->as_range_tombstone_begin();
                        _mutation_fragments.emplace(range_tombstone_end(std::move(rtb.key()), flip_bound_kind(rtb.kind())));
                        _current_tombstone = rtb.tomb();
                    } else if (mf->is_range_tombstone_end()) {
                        auto& rte = mf->as_range_tombstone_end();
                        _mutation_fragments.emplace(range_tombstone_begin(std::move(rte.key()), flip_bound_kind(rte.kind()), _current_tombstone));
                    } else {
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