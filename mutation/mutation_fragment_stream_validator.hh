/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "mutation_fragment_v2.hh"

enum class mutation_fragment_stream_validation_level {
    none, // disable validation altogether
    partition_region, // fragment kind
    token,
    partition_key,
    clustering_key,
};

/// Low level fragment stream validator.
///
/// Tracks and validates the monotonicity of the passed in fragment kinds,
/// position in partition, token or partition keys.
class mutation_fragment_stream_validator {
public:
    class validation_result {
        sstring _what;
    private:
        explicit validation_result() = default;
        explicit validation_result(sstring what) : _what(std::move(what)) { }
    public:
        static validation_result invalid(sstring what) { return validation_result(what); }
        static validation_result valid() { return validation_result(); }
        bool is_valid() const { return _what.empty(); }
        const sstring& what() const { return _what; }
        explicit operator bool() const { return is_valid(); }
        bool operator!() const { return !is_valid(); }
    };

private:
    const ::schema& _schema;
    mutation_fragment_v2::kind _prev_kind;
    position_in_partition _prev_pos;
    dht::decorated_key _prev_partition_key;
    tombstone _current_tombstone;

private:
    validation_result validate(dht::token t, const partition_key* pkey);
    validation_result validate(mutation_fragment_v2::kind kind, std::optional<position_in_partition_view> pos,
        std::optional<tombstone> new_current_tombstone);
public:
    explicit mutation_fragment_stream_validator(const schema& s);

    const ::schema& schema() const { return _schema; }

    /// Validate the monotonicity of the fragment kind.
    ///
    /// Should be used when the full, more heavy-weight position-in-partition
    /// monotonicity validation provided by
    /// `operator()(const mutation_fragment&)` is not desired.
    /// Using both overloads for the same stream is not supported.
    /// Advances the previous fragment kind, but only if the validation passes.
    /// `new_current_tombstone` should be engaged only when the fragment changes
    /// the current tombstone (range tombstone change fragments).
    ///
    /// \returns true if the fragment kind is valid.
    validation_result operator()(mutation_fragment_v2::kind kind, std::optional<tombstone> new_current_tombstone);
    validation_result operator()(mutation_fragment::kind kind);

    /// Validates the monotonicity of the mutation fragment kind and position.
    ///
    /// Validates the mutation fragment kind monotonicity and
    /// position-in-partition.
    /// A more complete version of `operator()(mutation_fragment::kind)`.
    /// Using both overloads for the same stream is not supported.
    /// Advances the previous fragment kind and position-in-partition, but only
    /// if the validation passes.
    /// `new_current_tombstone` should be engaged only when the fragment changes
    /// the current tombstone (range tombstone change fragments).
    ///
    /// \returns true if the mutation fragment kind is valid.
    validation_result operator()(mutation_fragment_v2::kind kind, position_in_partition_view pos, std::optional<tombstone> new_current_tombstone);
    validation_result operator()(mutation_fragment::kind kind, position_in_partition_view pos);

    /// Validates the monotonicity of the mutation fragment.
    ///
    /// Equivalent to calling `operator()(mf.kind(), mf.position())`.
    /// See said overload for more details.
    ///
    /// \returns true if the mutation fragment kind is valid.
    validation_result operator()(const mutation_fragment_v2& mf);
    validation_result operator()(const mutation_fragment& mf);

    /// Validates the monotonicity of the token.
    ///
    /// Does not check fragment level monotonicity.
    /// Advances the previous token, but only if the validation passes.
    /// Cannot be used in parallel with the `dht::decorated_key`
    /// overload.
    ///
    /// \returns true if the token is valid.
    validation_result operator()(dht::token t);

    /// Validates the monotonicity of the partition.
    ///
    /// Does not check fragment level monotonicity.
    /// Advances the previous partition-key, but only if the validation passes.
    /// Cannot be used in parallel with the `dht::token`
    /// overload.
    ///
    /// \returns true if the partition key is valid.
    validation_result operator()(const dht::decorated_key& dk);

    /// Reset the state of the validator to the given partition
    ///
    /// Reset the state of the validator as if it has just validated a valid
    /// partition start with the provided key. This can be used t force a reset
    /// to a given partition that is normally invalid and hence wouldn't advance
    /// the internal state. This can be used by users that can correct such
    /// invalid streams and wish to continue validating it.
    void reset(dht::decorated_key dk);

    /// Reset the state of the validator to the given fragment
    ///
    /// Reset the state of the validator as if it has just validated a valid
    /// fragment. This can be used t force a reset to a given fragment that is
    /// normally invalid and hence wouldn't advance the internal state. This
    /// can be used by users that can correct such invalid streams and wish to
    /// continue validating it.
    void reset(mutation_fragment_v2::kind kind, position_in_partition_view pos, std::optional<tombstone> new_current_tombstone);
    void reset(const mutation_fragment&);
    void reset(const mutation_fragment_v2&);

    /// Validate that the stream was properly closed.
    ///
    /// \returns false if the last partition wasn't closed, i.e. the last
    /// fragment wasn't a `partition_end` fragment.
    validation_result on_end_of_stream();

    /// The previous valid fragment kind.
    mutation_fragment_v2::kind previous_mutation_fragment_kind() const {
        return _prev_kind;
    }
    /// The previous valid position.
    ///
    /// Call only if operator()(position_in_partition_view) was used.
    const position_in_partition& previous_position() const {
        return _prev_pos;
    }
    /// Get the current effective tombstone
    ///
    /// Call only if operator()(mutation_fragment_v2) or
    /// operator()(mutation_fragment_v2::kind, position_in_partition_view, std::optional<tombstone>)
    /// was not used.
    tombstone current_tombstone() const {
        return _current_tombstone;
    }
    /// The previous valid partition key.
    ///
    /// Call only if operator()(dht::token) or operator()(const dht::decorated_key&) was used.
    dht::token previous_token() const {
        return _prev_partition_key.token();
    }
    /// The previous valid partition key.
    ///
    /// Call only if operator()(const dht::decorated_key&) was used.
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
    sstring _name_storage;
    std::string_view _name_view; // always valid
    mutation_fragment_stream_validation_level _validation_level;
    bool _raise_errors;

private:
    mutation_fragment_stream_validating_filter(const char* name_literal, sstring name_value, const schema& s,
            mutation_fragment_stream_validation_level level, bool raise_errors);

public:
    /// Constructor.
    ///
    /// \arg name is used in log messages to identify the validator, the
    ///     schema identity is added automatically
    /// \arg compare_keys enable validating clustering key monotonicity
    mutation_fragment_stream_validating_filter(sstring name, const schema& s, mutation_fragment_stream_validation_level level, bool raise_errors = true);
    mutation_fragment_stream_validating_filter(const char* name, const schema& s, mutation_fragment_stream_validation_level level, bool raise_errors = true);

    mutation_fragment_stream_validating_filter(mutation_fragment_stream_validating_filter&&) = delete;
    mutation_fragment_stream_validating_filter(const mutation_fragment_stream_validating_filter&) = delete;

    sstring full_name() const;

    bool raise_errors() const { return _raise_errors; }

    const mutation_fragment_stream_validator& validator() const { return  _validator; }

    bool operator()(const dht::decorated_key& dk);
    bool operator()(mutation_fragment_v2::kind kind, position_in_partition_view pos, std::optional<tombstone> new_current_tombstone);
    bool operator()(mutation_fragment::kind kind, position_in_partition_view pos);
    /// Equivalent to `operator()(mf.kind(), mf.position())`
    bool operator()(const mutation_fragment_v2& mv);
    bool operator()(const mutation_fragment& mv);
    void reset(mutation_fragment_v2::kind kind, position_in_partition_view pos, std::optional<tombstone> new_current_tombstone);
    void reset(const mutation_fragment_v2& mf);
    /// Equivalent to `operator()(partition_end{})`
    bool on_end_of_partition();
    bool on_end_of_stream();
    mutation_fragment_stream_validator& validator() { return _validator; }
};
