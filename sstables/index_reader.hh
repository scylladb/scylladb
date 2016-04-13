/*
 * Copyright 2015 Cloudius Systems
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

#pragma once
#include "sstables.hh"
#include "consumer.hh"

namespace sstables {

class index_consumer {
    uint64_t max_quantity;
public:
    index_list indexes;

    index_consumer(uint64_t q) : max_quantity(q) {
        indexes.reserve(q);
    }

    bool should_continue() {
        return indexes.size() < max_quantity;
    }
    void consume_entry(index_entry&& ie) {
        indexes.push_back(std::move(ie));
    }
};

// IndexConsumer is a concept that implements:
//
// bool should_continue();
// void consume_entry(index_entry&& ie);
template <class IndexConsumer>
class index_consume_entry_context: public data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>> {
    using proceed = data_consumer::proceed;
    using continuous_data_consumer = data_consumer::continuous_data_consumer<index_consume_entry_context<IndexConsumer>>;
private:
    IndexConsumer& _consumer;

    enum class state {
        START,
        KEY_SIZE,
        KEY_BYTES,
        POSITION,
        PROMOTED_SIZE,
        PROMOTED_BYTES,
        CONSUME_ENTRY,
    } _state = state::START;

    temporary_buffer<char> _key;
    temporary_buffer<char> _promoted;

public:
    void verify_end_state() {
    }

    bool non_consuming() const {
        return ((_state == state::CONSUME_ENTRY) || (_state == state::START) ||
                ((_state == state::PROMOTED_BYTES) && (continuous_data_consumer::_prestate == continuous_data_consumer::prestate::NONE)));
    }

    proceed process_state(temporary_buffer<char>& data) {
        switch (_state) {
        // START comes first, to make the handling of the 0-quantity case simpler
        case state::START:
            if (!_consumer.should_continue()) {
                return proceed::no;
            }
            _state = state::KEY_SIZE;
            break;
        case state::KEY_SIZE:
            if (this->read_16(data) != continuous_data_consumer::read_status::ready) {
                _state = state::KEY_BYTES;
                break;
            }
        case state::KEY_BYTES:
            if (this->read_bytes(data, this->_u16, _key) != continuous_data_consumer::read_status::ready) {
                _state = state::POSITION;
                break;
            }
        case state::POSITION:
            if (this->read_64(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PROMOTED_SIZE;
                break;
            }
        case state::PROMOTED_SIZE:
            if (this->read_32(data) != continuous_data_consumer::read_status::ready) {
                _state = state::PROMOTED_BYTES;
                break;
            }
        case state::PROMOTED_BYTES:
            if (this->read_bytes(data, this->_u32, _promoted) != continuous_data_consumer::read_status::ready) {
                _state = state::CONSUME_ENTRY;
                break;
            }
        case state::CONSUME_ENTRY:
            _consumer.consume_entry(index_entry(std::move(_key), this->_u64, std::move(_promoted)));
            _state = state::START;
            break;
        default:
            throw malformed_sstable_exception("unknown state");
        }
        return proceed::yes;
    }

    index_consume_entry_context(IndexConsumer& consumer,
            input_stream<char>&& input, uint64_t maxlen)
        : continuous_data_consumer(std::move(input), maxlen)
        , _consumer(consumer)
    {}

};
}
