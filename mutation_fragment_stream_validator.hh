/*
 * Copyright (C) 2021 ScyllaDB
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

#include "mutation_fragment.hh"

enum class mutation_fragment_stream_validation_level {
    partition_region, // fragment kind
    partition_key,
    clustering_key,
};

/// Low level fragment stream validator.
///
/// Tracks and validates the monotonicity of the passed in fragment kinds,
/// position in partition and partition keys. Any subset of these can be
/// used, but what is used have to be consistent across the entire stream.
class mutation_fragment_stream_validator {
    const schema& _schema;
    mutation_fragment::kind _prev_kind;
    position_in_partition _prev_pos;
    dht::decorated_key _prev_partition_key;
public:
    explicit mutation_fragment_stream_validator(const schema& s);

    /// Validate the monotonicity of the fragment kind.
    ///
    /// Should be used when the full, more heavy-weight position-in-partition
    /// monotonicity validation provided by
    /// `operator()(const mutation_fragment&)` is not desired.
    /// Using both overloads for the same stream is not supported.
    /// Advances the previous fragment kind, but only if the validation passes.
    ///
    /// \returns true if the fragment kind is valid.
    bool operator()(mutation_fragment::kind kind);

    /// Validates the monotonicity of the mutation fragment kind and position.
    ///
    /// Validates the mutation fragment kind monotonicity and
    /// position-in-partition.
    /// A more complete version of `operator()(mutation_fragment::kind)`.
    /// Using both overloads for the same stream is not supported.
    /// Advances the previous fragment kind and position-in-partition, but only
    /// if the validation passes.
    ///
    /// \returns true if the mutation fragment kind is valid.
    bool operator()(mutation_fragment::kind kind, position_in_partition_view pos);

    /// Validates the monotonicity of the mutation fragment.
    ///
    /// Equivalent to calling `operator()(mf.kind(), mf.position())`.
    /// See said overload for more details.
    ///
    /// \returns true if the mutation fragment kind is valid.
    bool operator()(const mutation_fragment& mf);

    /// Validates the monotonicity of the partition.
    ///
    /// Does not check fragment level monotonicity.
    /// Advances the previous partition-key, but only if the validation passes.
    //
    /// \returns true if the partition key is valid.
    bool operator()(const dht::decorated_key& dk);

    /// Validate that the stream was properly closed.
    ///
    /// \returns false if the last partition wasn't closed, i.e. the last
    /// fragment wasn't a `partition_end` fragment.
    bool on_end_of_stream();

    /// The previous valid fragment kind.
    mutation_fragment::kind previous_mutation_fragment_kind() const {
        return _prev_kind;
    }
    /// The previous valid position.
    ///
    /// Not meaningful, when operator()(position_in_partition_view) is not used.
    const position_in_partition& previous_position() const {
        return _prev_pos;
    }
    /// The previous valid partition key.
    const dht::decorated_key& previous_partition_key() const {
        return _prev_partition_key;
    }
};

struct invalid_mutation_fragment_stream : public std::runtime_error {
    explicit invalid_mutation_fragment_stream(std::runtime_error e);
};

/// Track position_in_partition transitions and validate monotonicity.
///
/// Will throw `invalid_mutation_fragment_stream` if any violation is found.
/// If the `abort_on_internal_error` configuration option is set, it will
/// abort instead.
/// Implements the FlattenedConsumerFilter concept.
class mutation_fragment_stream_validating_filter {
    mutation_fragment_stream_validator _validator;
    sstring _name;
    mutation_fragment_stream_validation_level _validation_level;

public:
    /// Constructor.
    ///
    /// \arg name is used in log messages to identify the validator, the
    ///     schema identity is added automatically
    /// \arg compare_keys enable validating clustering key monotonicity
    mutation_fragment_stream_validating_filter(sstring_view name, const schema& s, mutation_fragment_stream_validation_level level);

    bool operator()(const dht::decorated_key& dk);
    bool operator()(mutation_fragment::kind kind, position_in_partition_view pos);
    /// Equivalent to `operator()(mf.kind(), mf.position())`
    bool operator()(const mutation_fragment& mv);
    /// Equivalent to `operator()(partition_end{})`
    bool on_end_of_partition();
    void on_end_of_stream();
};
