/*
 * Copyright (C) 2018 ScyllaDB
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

#include "atomic_cell.hh"
#include "atomic_cell_or_collection.hh"
#include "types.hh"

/// LSA mirator for cells with irrelevant type
///
///
const data::type_imr_descriptor& no_type_imr_descriptor() {
    static thread_local data::type_imr_descriptor state(data::type_info::make_variable_size());
    return state;
}

atomic_cell atomic_cell::make_dead(api::timestamp_type timestamp, gc_clock::time_point deletion_time) {
    auto& imr_data = no_type_imr_descriptor();
    return atomic_cell(
            imr_data.type_info(),
            imr_object_type::make(data::cell::make_dead(timestamp, deletion_time), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value, atomic_cell::collection_member cm) {
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value, bool(cm)), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, ser::buffer_view<bytes_ostream::fragment_iterator> value, atomic_cell::collection_member cm) {
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value, bool(cm)), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, const fragmented_temporary_buffer::view& value, collection_member cm)
{
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value, bool(cm)), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, bytes_view value,
                             gc_clock::time_point expiry, gc_clock::duration ttl, atomic_cell::collection_member cm) {
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value, expiry, ttl, bool(cm)), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, ser::buffer_view<bytes_ostream::fragment_iterator> value,
                             gc_clock::time_point expiry, gc_clock::duration ttl, atomic_cell::collection_member cm) {
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value, expiry, ttl, bool(cm)), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live(const abstract_type& type, api::timestamp_type timestamp, const fragmented_temporary_buffer::view& value,
                                   gc_clock::time_point expiry, gc_clock::duration ttl, collection_member cm)
{
    auto& imr_data = type.imr_state();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live(imr_data.type_info(), timestamp, value, expiry, ttl, bool(cm)), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live_counter_update(api::timestamp_type timestamp, int64_t value) {
    auto& imr_data = no_type_imr_descriptor();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live_counter_update(timestamp, value), &imr_data.lsa_migrator())
    );
}

atomic_cell atomic_cell::make_live_uninitialized(const abstract_type& type, api::timestamp_type timestamp, size_t size) {
    auto& imr_data = no_type_imr_descriptor();
    return atomic_cell(
        imr_data.type_info(),
        imr_object_type::make(data::cell::make_live_uninitialized(imr_data.type_info(), timestamp, size), &imr_data.lsa_migrator())
    );
}

static imr::utils::object<data::cell::structure> copy_cell(const data::type_imr_descriptor& imr_data, const uint8_t* ptr)
{
    using imr_object_type = imr::utils::object<data::cell::structure>;

    // If the cell doesn't own any memory it is trivial and can be copied with
    // memcpy.
    auto f = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
    if (!f.template get<data::cell::tags::external_data>()) {
        data::cell::context ctx(f, imr_data.type_info());
        // XXX: We may be better off storing the total cell size in memory. Measure!
        auto size = data::cell::structure::serialized_object_size(ptr, ctx);
        return imr_object_type::make_raw(size, [&] (uint8_t* dst) noexcept {
            std::copy_n(ptr, size, dst);
        }, &imr_data.lsa_migrator());
    }

    return imr_object_type::make(data::cell::copy_fn(imr_data.type_info(), ptr), &imr_data.lsa_migrator());
}

atomic_cell::atomic_cell(const abstract_type& type, atomic_cell_view other)
    : atomic_cell(type.imr_state().type_info(),
                  copy_cell(type.imr_state(), other._view.raw_pointer()))
{ }

atomic_cell_or_collection atomic_cell_or_collection::copy(const abstract_type& type) const {
    if (!_data.get()) {
        return atomic_cell_or_collection();
    }
    auto& imr_data = type.imr_state();
    return atomic_cell_or_collection(
        copy_cell(imr_data, _data.get())
    );
}

atomic_cell_or_collection::atomic_cell_or_collection(const abstract_type& type, atomic_cell_view acv)
    : _data(copy_cell(type.imr_state(), acv._view.raw_pointer()))
{
}

static collection_mutation_view get_collection_mutation_view(const uint8_t* ptr)
{
    auto f = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
    auto ti = data::type_info::make_collection();
    data::cell::context ctx(f, ti);
    auto view = data::cell::structure::get_member<data::cell::tags::cell>(ptr).as<data::cell::tags::collection>(ctx);
    auto dv = data::cell::variable_value::make_view(view, f.get<data::cell::tags::external_data>());
    return collection_mutation_view { dv };
}

collection_mutation_view atomic_cell_or_collection::as_collection_mutation() const {
    return get_collection_mutation_view(_data.get());
}

collection_mutation::collection_mutation(const collection_type_impl& type, collection_mutation_view v)
    : _data(imr_object_type::make(data::cell::make_collection(v.data), &type.imr_state().lsa_migrator()))
{
}

collection_mutation::collection_mutation(const collection_type_impl& type, bytes_view v)
    : _data(imr_object_type::make(data::cell::make_collection(v), &type.imr_state().lsa_migrator()))
{
}

collection_mutation::operator collection_mutation_view() const
{
    return get_collection_mutation_view(_data.get());
}

bool atomic_cell_or_collection::equals(const abstract_type& type, const atomic_cell_or_collection& other) const
{
    auto ptr_a = _data.get();
    auto ptr_b = other._data.get();

    if (!ptr_a || !ptr_b) {
        return !ptr_a && !ptr_b;
    }

    if (type.is_atomic()) {
        auto a = atomic_cell_view::from_bytes(type.imr_state().type_info(), _data);
        auto b = atomic_cell_view::from_bytes(type.imr_state().type_info(), other._data);
        if (a.timestamp() != b.timestamp()) {
            return false;
        }
        if (a.is_live()) {
            if (!b.is_live()) {
                return false;
            }
            if (a.is_counter_update()) {
                if (!b.is_counter_update()) {
                    return false;
                }
                return a.counter_update_value() == b.counter_update_value();
            }
            if (a.is_live_and_has_ttl()) {
                if (!b.is_live_and_has_ttl()) {
                    return false;
                }
                if (a.ttl() != b.ttl() || a.expiry() != b.expiry()) {
                    return false;
                }
            }
            return a.value() == b.value();
        }
        return a.deletion_time() == b.deletion_time();
    } else {
        return as_collection_mutation().data == other.as_collection_mutation().data;
    }
}

size_t atomic_cell_or_collection::external_memory_usage(const abstract_type& t) const
{
    if (!_data.get()) {
        return 0;
    }
    auto ctx = data::cell::context(_data.get(), t.imr_state().type_info());

    auto view = data::cell::structure::make_view(_data.get(), ctx);
    auto flags = view.get<data::cell::tags::flags>();

    size_t external_value_size = 0;
    if (flags.get<data::cell::tags::external_data>()) {
        if (flags.get<data::cell::tags::collection>()) {
            external_value_size = get_collection_mutation_view(_data.get()).data.size_bytes();
        } else {
            auto cell_view = data::cell::atomic_cell_view(t.imr_state().type_info(), view);
            external_value_size = cell_view.value_size();
        }
        // Add overhead of chunk headers. The last one is a special case.
        external_value_size += (external_value_size - 1) / data::cell::maximum_external_chunk_length * data::cell::external_chunk_overhead;
        external_value_size += data::cell::external_last_chunk_overhead;
    }
    return data::cell::structure::serialized_object_size(_data.get(), ctx)
        + imr_object_type::size_overhead + external_value_size;
}

std::ostream& operator<<(std::ostream& os, const atomic_cell_or_collection& c) {
    if (!c._data.get()) {
        return os << "{ null atomic_cell_or_collection }";
    }
    using dc = data::cell;
    os << "{ ";
    if (dc::structure::get_member<dc::tags::flags>(c._data.get()).get<dc::tags::collection>()) {
        os << "collection";
    } else {
        os << "atomic cell";
    }
    return os << " @" << static_cast<const void*>(c._data.get()) << " }";
}
