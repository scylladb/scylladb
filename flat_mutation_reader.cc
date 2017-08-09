/*
 * Copyright (C) 2017 ScyllaDB
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

#include "flat_mutation_reader.hh"
#include "mutation_reader.hh"
#include <algorithm>

#include <boost/range/adaptor/transformed.hpp>

void flat_mutation_reader::impl::forward_buffer_to(schema_ptr& schema, const position_in_partition& pos) {
    _buffer.erase(std::remove_if(_buffer.begin(), _buffer.end(), [this, &pos, &schema] (mutation_fragment& f) {
        return !f.relevant_for_range_assuming_after(*schema, pos);
    }), _buffer.end());

    _buffer_size = boost::accumulate(_buffer | boost::adaptors::transformed(std::mem_fn(&mutation_fragment::memory_usage)), size_t(0));
}

void flat_mutation_reader::impl::clear_buffer_to_next_partition() {
    auto next_partition_start = std::find_if(_buffer.begin(), _buffer.end(), [] (const mutation_fragment& mf) {
        return mf.is_partition_start();
    });
    _buffer.erase(_buffer.begin(), next_partition_start);

    _buffer_size = boost::accumulate(_buffer | boost::adaptors::transformed(std::mem_fn(&mutation_fragment::memory_usage)), size_t(0));
}

flat_mutation_reader flat_mutation_reader_from_mutation_reader(schema_ptr s, mutation_reader&& legacy_reader, streamed_mutation::forwarding fwd) {
    class converting_reader final : public flat_mutation_reader::impl {
        schema_ptr _schema;
        mutation_reader _legacy_reader;
        streamed_mutation_opt _sm;
        streamed_mutation::forwarding _fwd;

        future<> get_next_sm() {
            return _legacy_reader().then([this] (auto&& sm) {
                if (bool(sm)) {
                    _sm = std::move(sm);
                    this->push_mutation_fragment(
                            mutation_fragment(partition_start(_sm->decorated_key(), _sm->partition_tombstone())));
                } else {
                    _end_of_stream = true;
                }
            });
        }
        void on_sm_finished() {
            if (_fwd == streamed_mutation::forwarding::yes) {
                _end_of_stream = true;
            } else {
                this->push_mutation_fragment(mutation_fragment(partition_end()));
                _sm = {};
            }
        }
    public:
        converting_reader(schema_ptr s, mutation_reader&& legacy_reader, streamed_mutation::forwarding fwd)
            : _schema(std::move(s)), _legacy_reader(std::move(legacy_reader)), _fwd(fwd)
        { }
        virtual future<> fill_buffer() override {
            return do_until([this] { return is_end_of_stream() || is_buffer_full(); }, [this] {
                if (!_sm) {
                    return get_next_sm();
                } else {
                    if (_sm->is_buffer_empty()) {
                        if (_sm->is_end_of_stream()) {
                            on_sm_finished();
                            return make_ready_future<>();
                        }
                        return _sm->fill_buffer();
                    } else {
                        while (!_sm->is_buffer_empty() && !is_buffer_full()) {
                            this->push_mutation_fragment(_sm->pop_mutation_fragment());
                        }
                        if (_sm->is_end_of_stream() && _sm->is_buffer_empty()) {
                            on_sm_finished();
                        }
                        return make_ready_future<>();
                    }
                }
            });
        }
        virtual void next_partition() override {
            if (_fwd == streamed_mutation::forwarding::yes) {
                clear_buffer();
                _sm = {};
                _end_of_stream = false;
            } else {
                clear_buffer_to_next_partition();
                if (_sm && is_buffer_empty()) {
                    _sm = {};
                }
            }
        }
        virtual future<> fast_forward_to(const dht::partition_range& pr) override {
            clear_buffer();
            _sm = { };
            _end_of_stream = false;
            return _legacy_reader.fast_forward_to(pr);
        };
        virtual future<> fast_forward_to(position_range cr) override {
            forward_buffer_to(_schema, cr.start());
            _end_of_stream = false;
            if (_sm) {
                return _sm->fast_forward_to(std::move(cr));
            } else {
                throw std::runtime_error("fast forward needs _sm to be set");
            }
        };
    };
    return make_flat_mutation_reader<converting_reader>(std::move(s), std::move(legacy_reader), fwd);
}
