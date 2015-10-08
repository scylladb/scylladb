/*
 * Copyright 2015 ScyllaDB
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

#include "key_reader.hh"

#include <boost/range/algorithm/heap_algorithm.hpp>
#include "utils/move.hh"

namespace stdx = std::experimental;

namespace {

class combined_reader final : public key_reader::impl {
    std::vector<key_reader> _readers;
    struct key_and_reader {
        dht::decorated_key_opt dk;
        key_reader* kr;

        bool equal(const schema&, const dht::decorated_key_opt&) const;
    };
    std::vector<key_and_reader> _data;
    dht::decorated_key_opt _last;
    bool _inited = false;
    schema_ptr _s;
    static bool heap_compare(const schema& s, const key_and_reader&, const key_and_reader&);
public:
    combined_reader(schema_ptr s, std::vector<key_reader> readers);
    virtual future<dht::decorated_key_opt> operator()() override;
};

combined_reader::combined_reader(schema_ptr s, std::vector<key_reader> readers)
    : _readers(std::move(readers))
    , _s(s)
{
    _data.reserve(_readers.size());

    auto cmp = [this] (auto& x, auto& y) { return heap_compare(*_s, x, y); };
    boost::range::make_heap(_data, cmp);
}

future<dht::decorated_key_opt> combined_reader::operator()()
{
    auto cmp = [this] (auto& x, auto& y) { return heap_compare(*_s, x, y); };
    if (!_inited) {
        return parallel_for_each(_readers, [this] (key_reader& kr) {
            return kr().then([this, &kr] (dht::decorated_key_opt dk) mutable {
                if (dk) {
                    key_and_reader kar;
                    kar.kr = &kr;
                    kar.dk = std::move(dk);
                    _data.emplace_back(std::move(kar));
                }
            });
        }).then([this, cmp] {
            boost::make_heap(_data, cmp);
            boost::pop_heap(_data, cmp);
            _inited = true;
            return operator()();
        });
    }
    if (_data.empty()) {
        return make_ready_future<dht::decorated_key_opt>();
    }
    auto& current = _data.back();
    if (!current.dk || (_last && _last->equal(*_s, *current.dk))) {
        return (*current.kr)().then([this, cmp] (dht::decorated_key_opt dk) mutable {
            if (!dk) {
                _data.pop_back();
            } else {
                _data.back().dk = std::move(dk);
                boost::range::push_heap(_data, cmp);
            }
            boost::range::pop_heap(_data, cmp);
            return operator()();
        });
    }
    _last = move_and_disengage(current.dk);
    return make_ready_future<dht::decorated_key_opt>(_last);
}

bool combined_reader::heap_compare(const schema& s, const key_and_reader& a, const key_and_reader& b)
{
    if (!b.dk) {
        return false;
    }
    if (!a.dk) {
        return true;
    }
    return b.dk->less_compare(s, *a.dk);
}

bool combined_reader::key_and_reader::equal(const schema& s, const dht::decorated_key_opt& other) const
{
    if (!dk && !other) {
        return true;
    } else if (!dk || !other) {
        return false;
    }
    return dk->equal(s, *other);
}

}

key_reader make_combined_reader(schema_ptr s, std::vector<key_reader> readers)
{
    return make_key_reader<combined_reader>(s, std::move(readers));
}