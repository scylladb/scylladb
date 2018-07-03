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

#pragma once

#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/for_each.hpp>

#include <seastar/util/variant_utils.hh>

#include "imr/compound.hh"
#include "imr/fundamental.hh"
#include "imr/alloc.hh"
#include "imr/utils.hh"
#include "imr/concepts.hh"

#include "data/schema_info.hh"
#include "data/value_view.hh"

#include "gc_clock.hh"
#include "timestamp.hh"

namespace data {

template<typename T>
class value_writer;

struct cell {
    static constexpr size_t maximum_internal_storage_length = value_view::maximum_internal_storage_length;
    static constexpr size_t maximum_external_chunk_length = value_view::maximum_external_chunk_length;

    struct tags {
        class cell;
        class atomic_cell;
        class collection;

        class flags;
        class live;
        class expiring;
        class counter_update;
        class external_data;

        class ttl;
        class expiry;
        class empty;
        class timestamp;
        class value;
        class dead;
        class counter_update;
        class fixed_value;
        class variable_value;
        class value_size;
        class value_data;
        class pointer;
        class data;
        class external_data;

        class chunk_back_pointer;
        class chunk_next;
        class chunk_data;
        class last_chunk_size;
    };

    using flags = imr::flags<
        tags::collection,
        tags::live,
        tags::expiring,
        tags::counter_update,
        tags::empty,
        tags::external_data
    >;

    /// Variable-length cell value
    ///
    /// This is a definition of the IMR structure of a variable-length value.
    /// It is used both by collections, counters and regular cells which type
    /// is variable-sized. The data can be stored internally, if its size is
    /// smaller or equal maximum_internal_storage_length or externally if it
    /// larger.
    struct variable_value {
        using data_variant = imr::variant<tags::value_data,
            imr::member<tags::pointer, imr::tagged_type<tags::pointer, imr::pod<uint8_t*>>>,
            imr::member<tags::data, imr::buffer<tags::data>>
        >;

        using structure = imr::structure<
            imr::member<tags::value_size, imr::pod<uint32_t>>,
            imr::member<tags::value_data, data_variant>
        >;

        /// Create writer of a variable-size value
        ///
        /// Returns a function object that can be used as a writer of a variable
        /// value. The first argument is expected to be either IMR sizer or
        /// serializer and the second is an appropriate IMR allocator helper
        /// object.
        /// \arg force_internal if set to true stores the value internally
        /// regardless of its size (used by collection members).
        template<typename FragmentRange>
        static value_writer<std::decay_t<FragmentRange>> write(FragmentRange&& value, bool force_internal = false) noexcept;
        static auto write(bytes_view value, bool force_internal = false) noexcept;

        /// Create writer of an uninitialised variable-size value
        static value_writer<empty_fragment_range> write(size_t size, bool force_internal = false) noexcept;

        class context {
            bool _external_storage;
            uint32_t _value_size;
        public:
            explicit context(bool external_storage, uint32_t value_size) noexcept
                : _external_storage(external_storage), _value_size(value_size) { }
            template<typename Tag>
            auto active_alternative_of() const noexcept {
                if (_external_storage) {
                    return data_variant::index_for<tags::pointer>();
                } else {
                    return data_variant::index_for<tags::data>();
                }
            }
            template<typename Tag>
            size_t size_of() const noexcept {
                return _value_size;
            }
            template<typename Tag, typename... Args>
            auto context_for(Args&&...) const noexcept {
                return *this;
            }
        };

        template<mutable_view is_mutable>
        static basic_value_view<is_mutable> do_make_view(structure::basic_view<is_mutable> view, bool external_storage);

        static data::value_view make_view(structure::view view, bool external_storage) {
            return do_make_view(view, external_storage);
        }
        static data::value_mutable_view make_view(structure::mutable_view view, bool external_storage) {
            return do_make_view(view, external_storage);
        }
    };

    using fixed_value = imr::buffer<tags::fixed_value>;
    /// Cell value
    ///
    /// The cell value can be either a deletion time (if the cell is dead),
    /// a delta (counter update cell), fixed-size value or variable-sized value.
    using value_variant = imr::variant<tags::value,
        imr::member<tags::dead, imr::pod<int32_t>>,
        imr::member<tags::counter_update, imr::pod<int64_t>>,
        imr::member<tags::fixed_value, fixed_value>,
        imr::member<tags::variable_value, variable_value::structure>
    >;
    /// Atomic cell
    ///
    /// Atomic cells can be either regular cells or counters. Moreover, the
    /// cell may be live or dead and the regular cells may have expiration time.
    /// Counter cells may be either sets of shards or a delta. The former is not
    /// fully converted to the IMR yet and still use a custom serilalisation
    /// format. The IMR treats such cells the same way it handles regular blobs.
    using atomic_cell = imr::structure<
        imr::member<tags::timestamp, imr::pod<api::timestamp_type>>,
        imr::optional_member<tags::expiring, imr::structure<
            imr::member<tags::ttl, imr::pod<int32_t>>,
            imr::member<tags::expiry, imr::pod<int32_t>>
        >>,
        imr::member<tags::value, value_variant>
    >;
    using atomic_cell_or_collection = imr::variant<tags::cell,
        imr::member<tags::atomic_cell, atomic_cell>,
        imr::member<tags::collection, variable_value::structure>
    >;

    /// Top IMR definition of a cell
    ///
    /// A cell in Scylla's data model can be either atomic (a regular cell,
    /// a counter or a frozen collection) or an unfrozen collection. As for now
    /// only regular cells are fully utilising the IMR. Collections are still
    /// using custom serialisation format and from the IMR point of view are
    /// just opaque values.
    using structure = imr::structure<
        imr::member<tags::flags, flags>,
        imr::member<tags::cell, atomic_cell_or_collection>
    >;

    /// An fragment of externally stored value
    ///
    /// If a cell value size is above maximum_internal_storage_length it is
    /// stored externally. Moreover, in order to avoid stressing the memory
    /// allocators with large allocations values are fragmented in chunks
    /// no larger than maximum_external_chunk_length. The size of all chunks,
    /// but the last one is always maximum_external_chunk_length.
    using external_chunk = imr::structure<
        imr::member<tags::chunk_back_pointer, imr::tagged_type<tags::chunk_back_pointer, imr::pod<uint8_t*>>>,
        imr::member<tags::chunk_next, imr::pod<uint8_t*>>,
        imr::member<tags::chunk_data, imr::buffer<tags::chunk_data>>
    >;
    static constexpr size_t external_chunk_overhead = sizeof(uint8_t*) * 2;

    using external_last_chunk_size = imr::pod<uint16_t>;
    /// The last fragment of an externally stored value
    ///
    /// The size of the last fragment of a value stored externally may vary.
    /// Due to the requirements the LSA imposes on migrators we need to store
    /// the size inside it so that it can be retrieved when the LSA migrates
    /// object.
    using external_last_chunk = imr::structure<
        imr::member<tags::chunk_back_pointer, imr::tagged_type<tags::chunk_back_pointer, imr::pod<uint8_t*>>>,
        imr::member<tags::last_chunk_size, external_last_chunk_size>,
        imr::member<tags::chunk_data, imr::buffer<tags::chunk_data>>
    >;
    static constexpr size_t external_last_chunk_overhead = sizeof(uint8_t*) + sizeof(uint16_t);

    class context;
    class minimal_context;

    /// Value fragment deserialisation context
    ///
    /// This is a deserialization context for all, but last, value fragments.
    /// Their size is fixed.
    struct chunk_context {
        explicit constexpr chunk_context(const uint8_t*) noexcept { }

        template<typename Tag>
        static constexpr size_t size_of() noexcept {
            return cell::maximum_external_chunk_length;
        }
        template<typename Tag, typename... Args>
        auto context_for(Args&&...) const noexcept {
            return *this;
        }
    };

    /// Last value fragment deserialisation context
    class last_chunk_context {
        uint16_t _size;
    public:
        explicit last_chunk_context(const uint8_t* ptr) noexcept
                : _size(external_last_chunk::get_member<tags::last_chunk_size>(ptr).load())
        { }

        template<typename Tag>
        size_t size_of() const noexcept {
            return _size;
        }

        template<typename Tag, typename... Args>
        auto context_for(Args&&...) const noexcept {
            return *this;
        }
    };

    template<mutable_view is_mutable>
    class basic_atomic_cell_view;

    using atomic_cell_view = basic_atomic_cell_view<mutable_view::no>;
    using mutable_atomic_cell_view = basic_atomic_cell_view<mutable_view::yes>;
private:
    static thread_local imr::alloc::lsa_migrate_fn<external_last_chunk,
        imr::alloc::context_factory<last_chunk_context>> lsa_last_chunk_migrate_fn;
    static thread_local imr::alloc::lsa_migrate_fn<external_chunk,
        imr::alloc::context_factory<chunk_context>> lsa_chunk_migrate_fn;
public:
    /// Make a writer that copies a cell
    ///
    /// This function creates a writer that copies a cell. It can be either
    /// atomic or a collection.
    ///
    /// \arg ptr needs to remain valid as long as the writer is in use.
    /// \returns imr::WriterAllocator for cell::structure.
    static auto copy_fn(const type_info& ti, const uint8_t* ptr);

    /// Make a writer for a collection
    ///
    /// \arg data needs to remain valid as long as the writer is in use.
    /// \returns imr::WriterAllocator for cell::structure.
    template<typename FragmentRange, typename = std::enable_if_t<is_fragment_range_v<std::decay_t<FragmentRange>>>>
    static auto make_collection(FragmentRange&& data) noexcept {
        return [data] (auto&& serializer, auto&& allocations) noexcept {
            return serializer
                .serialize(imr::set_flag<tags::collection>(),
                           imr::set_flag<tags::external_data>(data.size_bytes() > maximum_internal_storage_length))
                .template serialize_as<tags::collection>(variable_value::write(data), allocations)
                .done();
        };
    }

    static auto make_collection(bytes_view data) noexcept {
        return make_collection(single_fragment_range(data));
    }

    /// Make a writer for a dead cell
    ///
    /// This function returns a generic lambda that is a writer for a dead
    /// cell with the specified timestamp and deletion time.
    ///
    /// \returns imr::WriterAllocator for cell::structure.
    static auto make_dead(api::timestamp_type ts, gc_clock::time_point deletion_time) noexcept {
        return [ts, deletion_time] (auto&& serializer, auto&&...) noexcept {
            return serializer
                .serialize()
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip()
                    .template serialize_as<tags::dead>(deletion_time.time_since_epoch().count())
                    .done()
                .done();
        };
    }
    static auto make_live_counter_update(api::timestamp_type ts, int64_t delta) noexcept {
        return [ts, delta] (auto&& serializer, auto&&...) noexcept {
            return serializer
                .serialize(imr::set_flag<tags::live>(),
                           imr::set_flag<tags::counter_update>())
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip()
                    .template serialize_as<tags::counter_update>(delta)
                    .done()
                .done();
        };
    }

    /// Make a writer for a live non-expiring cell
    ///
    /// \arg value needs to remain valid as long as the writer is in use.
    /// \arg force_internal always store the value internally regardless of its
    /// size. This is a temporary (hopefully, sorry if you are reading this in
    /// 2020) hack to make integration with collections easier.
    ///
    /// \returns imr::WriterAllocator for cell::structure.
    template<typename FragmentRange, typename = std::enable_if_t<is_fragment_range_v<std::decay_t<FragmentRange>>>>
    static auto make_live(const type_info& ti, api::timestamp_type ts, FragmentRange&& value, bool force_internal = false) noexcept {
        return [&ti, ts, value, force_internal] (auto&& serializer, auto&& allocations) noexcept {
            auto after_expiring = serializer
                .serialize(imr::set_flag<tags::live>(),
                           imr::set_flag<tags::empty>(value.empty()),
                           imr::set_flag<tags::external_data>(!force_internal && !ti.is_fixed_size() && value.size_bytes() > maximum_internal_storage_length))
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip();
            return [&] {
                if (ti.is_fixed_size()) {
                    return after_expiring.template serialize_as<tags::fixed_value>(value);
                } else {
                    return after_expiring
                        .template serialize_as<tags::variable_value>(variable_value::write(value, force_internal), allocations);
                }
            }().done().done();
        };
    }

    static auto make_live(const type_info& ti, api::timestamp_type ts, bytes_view value, bool force_internal = false) noexcept {
        return make_live(ti, ts, single_fragment_range(value), force_internal);
    }

    template<typename FragmentRange, typename = std::enable_if_t<is_fragment_range_v<std::decay_t<FragmentRange>>>>
    static auto make_live(const type_info& ti, api::timestamp_type ts, FragmentRange&& value, gc_clock::time_point expiry, gc_clock::duration ttl, bool force_internal = false) noexcept
    {
        return [&ti, ts, value, expiry, ttl, force_internal] (auto&& serializer, auto&& allocations) noexcept {
            auto after_expiring = serializer
                .serialize(imr::set_flag<tags::live>(),
                           imr::set_flag<tags::expiring>(),
                           imr::set_flag<tags::empty>(value.empty()),
                           imr::set_flag<tags::external_data>(!force_internal && !ti.is_fixed_size() && value.size_bytes() > maximum_internal_storage_length))
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .serialize_nested()
                        .serialize(ttl.count())
                        .serialize(expiry.time_since_epoch().count())
                        .done();
            return [&] {
                if (ti.is_fixed_size()) {
                    return after_expiring.template serialize_as<tags::fixed_value>(value);
                } else {
                    return after_expiring
                        .template serialize_as<tags::variable_value>(variable_value::write(value, force_internal), allocations);
                }
            }().done().done();
        };
    }

    static auto make_live(const type_info& ti, api::timestamp_type ts, bytes_view value, gc_clock::time_point expiry, gc_clock::duration ttl, bool force_internal = false) noexcept {
        return make_live(ti, ts, single_fragment_range(value), expiry, ttl, force_internal);
    }

    /// Make a writer of a live cell with uninitialised value
    ///
    /// This function returns a function object which is a writer of a live
    /// cell. The space for value is allocated but not initialised. This can be
    /// used if the value is a result of some IMR-independent serialisation
    /// (e.g. counters).
    ///
    /// \returns imr::WriterAllocator for cell::structure.
    static auto make_live_uninitialized(const type_info& ti, api::timestamp_type ts, size_t size) noexcept {
        return [&ti, ts, size] (auto&& serializer, auto&& allocations) noexcept {
            auto after_expiring = serializer
                .serialize(imr::set_flag<tags::live>(),
                           imr::set_flag<tags::empty>(!size),
                           imr::set_flag<tags::external_data>(!ti.is_fixed_size() && size > maximum_internal_storage_length))
                .template serialize_as_nested<tags::atomic_cell>()
                    .serialize(ts)
                    .skip();
            return [&] {
                if (ti.is_fixed_size()) {
                    return after_expiring.template serialize_as<tags::fixed_value>(size, [] (uint8_t*) noexcept { });
                } else {
                    return after_expiring
                        .template serialize_as<tags::variable_value>(variable_value::write(size, false), allocations);
                }
            }().done().done();
        };
    }

    template<typename Builder>
    static size_t size_of(Builder&& builder, imr::alloc::object_allocator& allocator) noexcept {
        return structure::size_when_serialized(std::forward<Builder>(builder), allocator.get_sizer());
    }

    template<typename Builder>
    static size_t serialize(uint8_t* ptr, Builder&& builder, imr::alloc::object_allocator& allocator) noexcept {
        return structure::serialize(ptr, std::forward<Builder>(builder), allocator.get_serializer());
    }

    static atomic_cell_view make_atomic_cell_view(const type_info& ti, const uint8_t* ptr) noexcept;
    static mutable_atomic_cell_view make_atomic_cell_view(const type_info& ti, uint8_t* ptr) noexcept;

    static void destroy(uint8_t* ptr) noexcept;
};

/// Minimal cell deserialisation context
///
/// This is a minimal deserialisation context that doesn't require the cell
/// type to be known, but allows only some operations to be performed. In
/// particular it is able to provide sufficient information to destroy a cell.
class cell::minimal_context {
protected:
    cell::flags::view _flags;
public:
    explicit minimal_context(cell::flags::view flags) noexcept
        : _flags(flags) { }

    template<typename Tag>
    bool is_present() const noexcept;

    template<typename Tag>
    auto active_alternative_of() const noexcept;

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto context_for(const uint8_t*) const noexcept {
        return *this;
    }
};

template<>
inline bool cell::minimal_context::is_present<cell::tags::expiring>() const noexcept {
    return _flags.get<tags::expiring>();
}

template<>
inline auto cell::minimal_context::active_alternative_of<cell::tags::cell>() const noexcept {
    if (_flags.get<tags::collection>()) {
        return cell::atomic_cell_or_collection::index_for<tags::collection>();
    } else {
        return cell::atomic_cell_or_collection::index_for<tags::atomic_cell>();
    }
}

/// Cell deserialisation context
///
/// This class combines schema-dependnent and instance-specific information
/// and provides an appropriate interface for the IMR deserialisation routines
/// to read a cell.
class cell::context : public cell::minimal_context {
    type_info _type;
public:
    explicit context(const uint8_t* ptr, const type_info& tinfo) noexcept
        : context(structure::get_member<tags::flags>(ptr), tinfo) { }

    explicit context(cell::flags::view flags, const type_info& tinfo) noexcept
        : minimal_context(flags), _type(tinfo) { }

    template<typename Tag>
    bool is_present() const noexcept {
        return minimal_context::is_present<Tag>();
    }

    template<typename Tag>
    auto active_alternative_of() const noexcept {
        return minimal_context::active_alternative_of<Tag>();
    }

    template<typename Tag>
    size_t size_of() const noexcept;

    template<typename Tag>
    auto context_for(const uint8_t*) const noexcept {
        return *this;
    }
};

template<>
inline auto cell::context::context_for<cell::tags::variable_value>(const uint8_t* ptr) const noexcept {
    auto length = variable_value::structure::get_member<tags::value_size>(ptr);
    return variable_value::context(_flags.get<tags::external_data>(), length.load());
}

template<>
inline auto cell::context::context_for<cell::tags::collection>(const uint8_t* ptr) const noexcept {
    auto length = variable_value::structure::get_member<tags::value_size>(ptr);
    return variable_value::context(_flags.get<tags::external_data>(), length.load());
}


template<>
inline auto cell::context::active_alternative_of<cell::tags::value>() const noexcept {
    if (_flags.get<tags::live>()) {
        if (__builtin_expect(_flags.get<tags::counter_update>(), false)) {
            return cell::value_variant::index_for<tags::counter_update>();
        }
        if (_type.is_fixed_size()) {
            return cell::value_variant::index_for<tags::fixed_value>();
        } else {
            return cell::value_variant::index_for<tags::variable_value>();
        }
    } else {
        return cell::value_variant::index_for<tags::dead>();
    }
}

template<>
inline size_t cell::context::size_of<cell::tags::fixed_value>() const noexcept {
    return _flags.get<tags::empty>() ? 0 : _type.value_size();
}

/// Atomic cell view
///
/// This is a, possibly mutable, view of an atomic cell. It is a wrapper on top
/// of IMR-generated view that provides more convenient interface which doesn't
/// depend on the actual cell structure.
///
/// \note Instances of this class are being copied and passed by value a lot.
/// It is desireable that it remains small and trivial, so that the compiler
/// can try to keep it in registers at all times. We also should not worry too
/// much about computing the same thing more than once (unless the profiler
/// tells otherwise, of course). Most of the IMR code and its direct users rely
/// heavily on inlining which would allow the compiler remove duplicated
/// computations.
template<mutable_view is_mutable>
class cell::basic_atomic_cell_view {
public:
    using view_type = structure::basic_view<is_mutable>;
private:
    type_info _type;
    view_type _view;
private:
    flags::view flags_view() const noexcept {
        return _view.template get<tags::flags>();
    }
    atomic_cell::basic_view<is_mutable> cell_view() const noexcept {
        return _view.template get<tags::cell>().template as<tags::atomic_cell>();
    }
    context make_context() const noexcept {
        return context(flags_view(), _type);
    }
public:
    basic_atomic_cell_view(const type_info& ti, view_type v) noexcept
        : _type(ti), _view(std::move(v)) { }

    operator basic_atomic_cell_view<mutable_view::no>() const noexcept {
        return basic_atomic_cell_view<mutable_view::no>(_type, _view);
    }

    const uint8_t* raw_pointer() const { return _view.raw_pointer(); }

    bytes_view serialize() const noexcept {
        assert(!flags_view().template get<tags::external_data>());
        auto ptr = raw_pointer();
        auto len = structure::serialized_object_size(ptr, make_context());
        return bytes_view(reinterpret_cast<const int8_t*>(ptr), len);
    }

    bool is_live() const noexcept {
        return flags_view().template get<tags::live>();
    }
    bool is_expiring() const noexcept {
        return flags_view().template get<tags::expiring>();
    }
    bool is_counter_update() const noexcept {
        return flags_view().template get<tags::counter_update>();
    }

    api::timestamp_type timestamp() const noexcept {
        return cell_view().template get<tags::timestamp>().load();
    }
    void set_timestamp(api::timestamp_type ts) noexcept {
        cell_view().template get<tags::timestamp>().store(ts);
    }

    gc_clock::time_point expiry() const noexcept {
        auto v = cell_view().template get<tags::expiring>().get().template get<tags::expiry>().load();
        return gc_clock::time_point(gc_clock::duration(v));
    }
    gc_clock::duration ttl() const noexcept {
        auto v = cell_view().template get<tags::expiring>().get().template get<tags::ttl>().load();
        return gc_clock::duration(v);
    }

    gc_clock::time_point deletion_time() const noexcept {
        auto v = cell_view().template get<tags::value>(make_context()).template as<tags::dead>().load();
        return gc_clock::time_point(gc_clock::duration(v));
    }

    int64_t counter_update_value() const noexcept {
        return cell_view().template get<tags::value>(make_context()).template as<tags::counter_update>().load();
    }

    basic_value_view<is_mutable> value() const noexcept {
        auto ctx = make_context();
        return cell_view().template get<tags::value>(ctx).visit(make_visitor(
                [] (fixed_value::basic_view<is_mutable> view) { return basic_value_view<is_mutable>(view, 0, nullptr); },
                [&] (variable_value::structure::basic_view<is_mutable> view) {
                    return variable_value::make_view(view, flags_view().template get<tags::external_data>());
                },
                [] (...) -> basic_value_view<is_mutable> { abort(); }
        ), ctx);
    }

    size_t value_size() const noexcept {
        auto ctx = make_context();
        return cell_view().template get<tags::value>(ctx).visit(make_visitor(
                [] (fixed_value::view view) -> size_t { return view.size(); },
                [] (variable_value::structure::view view) -> size_t {
                    return view.template get<tags::value_size>().load();
                },
                [] (...) -> size_t { abort(); }
        ), ctx);
    }

    bool is_value_fragmented() const noexcept {
        return flags_view().template get<tags::external_data>() && value_size() > maximum_external_chunk_length;
    }
};

inline auto cell::copy_fn(const type_info& ti, const uint8_t* ptr)
{
    // Slow path
    return [&ti, ptr] (auto&& serializer, auto&& allocations) noexcept {
        auto f = structure::get_member<tags::flags>(ptr);
        context ctx(ptr, ti);
        if (f.get<tags::collection>()) {
            auto view = structure::get_member<tags::cell>(ptr).as<tags::collection>(ctx);
            auto dv = variable_value::make_view(view, f.get<tags::external_data>());
            return make_collection(dv)(serializer, allocations);
        } else {
            auto acv = atomic_cell_view(ti, structure::make_view(ptr, ti));
            if (acv.is_live()) {
                if (acv.is_counter_update()) {
                    return make_live_counter_update(acv.timestamp(), acv.counter_update_value())(serializer, allocations);
                } else if (acv.is_expiring()) {
                    return make_live(ti, acv.timestamp(), acv.value(), acv.expiry(), acv.ttl())(serializer, allocations);
                }
                return make_live(ti, acv.timestamp(), acv.value())(serializer, allocations);
            } else {
                return make_dead(acv.timestamp(), acv.deletion_time())(serializer, allocations);
            }
        }
    };
}

inline cell::atomic_cell_view cell::make_atomic_cell_view(const type_info& ti, const uint8_t* ptr) noexcept {
    return atomic_cell_view(ti, structure::make_view(ptr));
}

inline cell::mutable_atomic_cell_view cell::make_atomic_cell_view(const type_info& ti, uint8_t* ptr) noexcept {
    return mutable_atomic_cell_view(ti, structure::make_view(ptr));
}

/// Context for external value destruction
///
/// When a cell value is stored externally as a list of fragments we need to
/// know when we reach the last fragment. The way to do that is to read the
/// total value size from the parent cell object and use the fact that the size
/// of all fragments except the last one is cell::maximum_external_chunk_length.
class fragment_chain_destructor_context : public imr::no_context_t {
    size_t _total_length;
public:
    explicit fragment_chain_destructor_context(size_t total_length) noexcept
            : _total_length(total_length) { }

    void next_chunk() noexcept { _total_length -= data::cell::maximum_external_chunk_length; }
    bool is_last_chunk() const noexcept { return _total_length <= data::cell::maximum_external_chunk_length; }
};

}

namespace imr {
namespace methods {

/// Cell destructor
///
/// If the cell value exceeds certain thresholds its value is stored externally
/// (possibly fragmented). This requires a destructor so that the owned memory
/// can be freed when the cell is destroyed.
/// Note that we don't need to know the actual type of the cell to destroy it,
/// since all the necessary information is stored in each instance. This means
/// that IMR cells can be owned by C++ objects without the problem of passing
/// arguments to C++ destructors.
template<>
struct destructor<data::cell::structure> {
    static void run(uint8_t* ptr, ...) {
        auto flags = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
        if (flags.get<data::cell::tags::external_data>()) {
            auto cell_offset = data::cell::structure::offset_of<data::cell::tags::cell>(ptr);
            auto variable_value_ptr = [&] {
                if (flags.get<data::cell::tags::collection>()) {
                    return ptr + cell_offset;
                } else {
                    auto ctx = data::cell::minimal_context(flags);
                    auto offset = data::cell::atomic_cell::offset_of<data::cell::tags::value>(ptr + cell_offset, ctx);
                    return ptr + cell_offset + offset;
                }
            }();
            imr::methods::destroy<data::cell::variable_value>(variable_value_ptr);
        }
    }
};

/// Cell mover
template<>
struct mover<data::cell::structure> {
    static void run(uint8_t* ptr, ...) {
        auto flags = data::cell::structure::get_member<data::cell::tags::flags>(ptr);
        if (flags.get<data::cell::tags::external_data>()) {
            auto cell_offset = data::cell::structure::offset_of<data::cell::tags::cell>(ptr);
            auto variable_value_ptr = [&] {
                if (flags.get<data::cell::tags::collection>()) {
                    return ptr + cell_offset;
                } else {
                    auto ctx = data::cell::minimal_context(flags);
                    auto offset = data::cell::atomic_cell::offset_of<data::cell::tags::value>(ptr + cell_offset, ctx);
                    return ptr + cell_offset + offset;
                }
            }();
            variable_value_ptr += data::cell::variable_value::structure::offset_of<data::cell::tags::value_data>(variable_value_ptr);
            imr::methods::move<imr::tagged_type<data::cell::tags::pointer, imr::pod<uint8_t*>>>(variable_value_ptr);
        }
    }
};

template<>
struct destructor<data::cell::variable_value> {
    static void run(uint8_t* ptr, ...) {
        auto varval = data::cell::variable_value::structure::make_view(ptr);
        auto total_length = varval.template get<data::cell::tags::value_size>().load();
        if (total_length <= data::cell::maximum_internal_storage_length) {
            return;
        }
        auto ctx = data::fragment_chain_destructor_context(total_length);
        auto ptr_view = varval.get<data::cell::tags::value_data>().as<data::cell::tags::pointer>();
        if (ctx.is_last_chunk()) {
            imr::methods::destroy<data::cell::external_last_chunk>(ptr_view.load());
        } else {
            imr::methods::destroy<data::cell::external_chunk>(ptr_view.load(), ctx);
        }
        current_allocator().free(ptr_view.load());
    }
};

template<>
struct mover<imr::tagged_type<data::cell::tags::pointer, imr::pod<uint8_t*>>> {
    static void run(uint8_t* ptr, ...) {
        auto ptr_view = imr::pod<uint8_t*>::make_view(ptr);
        auto chk_ptr = ptr_view.load();
        auto chk = data::cell::external_last_chunk::make_view(chk_ptr, data::cell::last_chunk_context(chk_ptr));
        chk.get<data::cell::tags::chunk_back_pointer>().store(ptr);
    }
};

template<>
struct mover<imr::tagged_type<data::cell::tags::chunk_back_pointer, imr::pod<uint8_t*>>> {
    static void run(uint8_t* bptr, ...) {
        auto bptr_view = imr::pod<uint8_t*>::make_view(bptr);
        auto ptr_ptr = bptr_view.load();
        auto ptr = imr::pod<uint8_t*>::make_view(ptr_ptr);
        ptr.store(bptr);

    }
};

/// External chunk destructor
template<>
struct destructor<data::cell::external_chunk> {
    static void run(uint8_t* ptr, data::fragment_chain_destructor_context ctx) {
        bool first = true;
        while (true) {
            ctx.next_chunk();

            auto echk_view = data::cell::external_chunk::make_view(ptr);
            auto ptr_view = echk_view.get<data::cell::tags::chunk_next>();
            if (ctx.is_last_chunk()) {
                imr::methods::destroy<data::cell::external_last_chunk>(ptr_view.load());
                current_allocator().free(ptr_view.load());
                if (!first) {
                    current_allocator().free(ptr);
                }
                break;
            } else {
                auto last = ptr;
                ptr = ptr_view.load();
                if (!first) {
                    current_allocator().free(last);
                } else {
                    first = false;
                }
            }

        }
    }
};

template<>
struct mover<data::cell::external_chunk> {
    static void run(uint8_t* ptr, ...) {
        auto echk_view = data::cell::external_chunk::make_view(ptr, data::cell::chunk_context(ptr));
        auto next_ptr = echk_view.get<data::cell::tags::chunk_next>().load();
        auto bptr = imr::pod<uint8_t*>::make_view(next_ptr);
        bptr.store(ptr + echk_view.offset_of<data::cell::tags::chunk_next>());

        auto back_ptr = echk_view.get<data::cell::tags::chunk_back_pointer>().load();
        auto nptr = imr::pod<uint8_t*>::make_view(back_ptr);
        nptr.store(ptr);
    }
};

}
}

template<>
struct appending_hash<data::value_view> {
    template<typename Hasher>
    void operator()(Hasher& h, data::value_view v) const {
        feed_hash(h, v.size_bytes());
        using boost::range::for_each;
        for_each(v, [&h] (auto&& chk) {
            h.update(reinterpret_cast<const char*>(chk.data()), chk.size());
        });
    }
};

int compare_unsigned(data::value_view lhs, data::value_view rhs) noexcept;

namespace data {

struct type_imr_descriptor {
    using context_factory = imr::alloc::context_factory<imr::utils::object_context<cell::context, data::type_info>, data::type_info>;
    using lsa_migrate_fn = imr::alloc::lsa_migrate_fn<imr::utils::object<cell::structure>::structure, context_factory>;
private:
    data::type_info _type_info;
    lsa_migrate_fn _lsa_migrator;
public:
    explicit type_imr_descriptor(data::type_info ti)
        : _type_info(ti)
        , _lsa_migrator(context_factory(ti))
    { }

    const data::type_info& type_info() const { return _type_info; }
    const lsa_migrate_fn& lsa_migrator() const { return _lsa_migrator; }
};

}

#include "value_view_impl.hh"
#include "cell_impl.hh"
