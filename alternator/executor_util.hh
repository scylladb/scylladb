/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This header file, and the implementation file executor_util.cc, contain
// various utility functions that are reused in many different operations
// (API requests) across Alternator's code - in files such as executor.cc,
// executor_read.cc, streams.cc, ttl.cc, and more. These utility functions
// include things like extracting and validating pieces from a JSON request,
// checking permissions, constructing auxiliary table names, and more.

#pragma once

#include <map>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <seastar/core/future.hh>
#include <seastar/util/noncopyable_function.hh>

#include "utils/rjson.hh"
#include "schema/schema_fwd.hh"
#include "types/types.hh"
#include "auth/permission.hh"
#include "alternator/stats.hh"
#include "alternator/attribute_path.hh"
#include "audit/audit.hh"
#include "utils/managed_bytes.hh"

namespace query { class partition_slice; class result; }
namespace cql3::selection { class selection; }
namespace data_dictionary { class database; }
namespace service { class storage_proxy; class client_state; }

namespace alternator {

/// The body_writer is used for streaming responses - where the response body
/// is written in chunks to the output_stream. This allows for efficient
/// handling of large responses without needing to allocate a large buffer in
/// memory. It is one of the variants of executor::request_return_type.
using body_writer = noncopyable_function<future<>(output_stream<char>&&)>;

/// Get the value of an integer attribute, or an empty optional if it is
/// missing. If the attribute exists, but is not an integer, a descriptive
/// api_error is thrown.
std::optional<int> get_int_attribute(const rjson::value& value, std::string_view attribute_name);

/// Get the value of a string attribute, or a default value if it is missing.
/// If the attribute exists, but is not a string, a descriptive api_error is
/// thrown.
std::string get_string_attribute(const rjson::value& value, std::string_view attribute_name, const char* default_return);

/// Get the value of a boolean attribute, or a default value if it is missing.
/// If the attribute exists, but is not a bool, a descriptive api_error is
/// thrown.
bool get_bool_attribute(const rjson::value& value, std::string_view attribute_name, bool default_return);

/// Extract table name from a request.
/// Most requests expect the table's name to be listed in a "TableName" field.
/// get_table_name() returns the name or api_error in case the table name is
/// missing or not a string.
std::string get_table_name(const rjson::value& request);

/// find_table_name() is like get_table_name() except that it returns an
/// optional table name - it returns an empty optional when the TableName
/// is missing from the request, instead of throwing as get_table_name()
/// does. However, find_table_name() still throws if a TableName exists but
/// is not a string.
std::optional<std::string> find_table_name(const rjson::value& request);

/// Extract table schema from a request.
/// Many requests expect the table's name to be listed in a "TableName" field
/// and need to look it up as an existing table. The get_table() function
/// does this, with the appropriate validation and api_error in case the table
/// name is missing, invalid or the table doesn't exist. If everything is
/// successful, it returns the table's schema.
schema_ptr get_table(service::storage_proxy& proxy, const rjson::value& request);

/// This find_table() variant is like get_table() excepts that it returns a
/// nullptr instead of throwing if the request does not mention a TableName.
/// In other cases of errors (i.e., a table is mentioned but doesn't exist)
/// this function throws too.
schema_ptr find_table(service::storage_proxy& proxy, const rjson::value& request);

/// This find_table() variant is like the previous one except that it takes
/// the table name directly instead of a request object. It is used in cases
/// where we already have the table name extracted from the request.
schema_ptr find_table(service::storage_proxy& proxy, std::string_view table_name);

// We would have liked to support table names up to 255 bytes, like DynamoDB.
// But Scylla creates a directory whose name is the table's name plus 33
// bytes (dash and UUID), and since directory names are limited to 255 bytes,
// we need to limit table names to 222 bytes, instead of 255. See issue #4480.
// We actually have two limits here,
// * max_table_name_length is the limit that Alternator will impose on names
//   of new Alternator tables.
// * max_auxiliary_table_name_length is the potentially higher absolute limit
//   that Scylla imposes on the names of auxiliary tables that Alternator
//   wants to create internally - i.e. materialized views or CDC log tables.
// The second limit might mean that it is not possible to add a GSI to an
// existing table, because the name of the new auxiliary table may go over
// the limit. The second limit is also one of the reasons why the first limit
// is set lower than 222 - to have room to enable streams which add the extra
// suffix "_scylla_cdc_log" to the table name.
inline constexpr int max_table_name_length = 192;
inline constexpr int max_auxiliary_table_name_length = 222;

/// validate_table_name() validates the TableName parameter in a request - it
/// should be called in CreateTable, and in other requests only when noticing
/// that the named table doesn't exist.
/// The DynamoDB developer guide, https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
/// specifies that table "names must be between 3 and 255 characters long and
/// can contain only the following characters: a-z, A-Z, 0-9, _ (underscore),
/// - (dash), . (dot)". However, Alternator only allows max_table_name_length
/// characters (see above) - not 255.
/// validate_table_name() throws the appropriate api_error if this validation
/// fails.
void validate_table_name(std::string_view name, const char* source = "TableName");

/// Validate that a CDC log table could be created for the base table with a
/// given table_name, and if not, throw a user-visible api_error::validation.
/// It is not possible to create a CDC log table if the table name is so long
/// that adding the 15-character suffix "_scylla_cdc_log" (cdc_log_suffix)
/// makes it go over max_auxiliary_table_name_length.
/// Note that if max_table_name_length is set to less than 207 (which is
/// max_auxiliary_table_name_length-15), then this function will never
/// fail. However, it's still important to call it in UpdateTable, in case
/// we have pre-existing tables with names longer than this to avoid #24598.
void validate_cdc_log_name_length(std::string_view table_name);

/// Checks if a keyspace, given by its name, is an Alternator keyspace.
/// This just checks if the name begins in executor::KEYSPACE_NAME_PREFIX,
/// a prefix that all keyspaces created by Alternator's CreateTable use.
bool is_alternator_keyspace(std::string_view ks_name);

/// Wraps db::get_tags_of_table() and throws api_error::validation if the
/// table is missing the tags extension.
const std::map<sstring, sstring>& get_tags_of_table_or_throw(schema_ptr schema);

/// Returns a type object representing the type of the ":attrs" column used
/// by Alternator to store all non-key attribute. This type is a map from
/// string (attribute name) to bytes (serialized attribute value).
map_type attrs_type();

// In DynamoDB index names are local to a table, while in Scylla, materialized
// view names are global (in a keyspace). So we need to compose a unique name
// for the view taking into account both the table's name and the index name.
// We concatenate the table and index name separated by a delim character
// (a character not allowed by DynamoDB in ordinary table names, default: ":").
// The downside of this approach is that it limits the sum of the lengths,
// instead of each component individually as DynamoDB does.
// The view_name() function assumes the table_name has already been validated
// but validates the legality of index_name and the combination of both.
std::string view_name(std::string_view table_name, std::string_view index_name,
        const std::string& delim = ":", bool validate_len = true);
std::string gsi_name(std::string_view table_name, std::string_view index_name,
        bool validate_len = true);
std::string lsi_name(std::string_view table_name, std::string_view index_name,
        bool validate_len = true);

/// After calling pk_from_json() and ck_from_json() to extract the pk and ck
/// components of a key, and if that succeeded, call check_key() to further
/// check that the key doesn't have any spurious components.
void check_key(const rjson::value& key, const schema_ptr& schema);

/// Fail with api_error::validation if the expression if has unused attribute
/// names or values. This is how DynamoDB behaves, so we do too.
void verify_all_are_used(const rjson::value* field,
        const std::unordered_set<std::string>& used,
        const char* field_name,
        const char* operation);

/// Check CQL's Role-Based Access Control (RBAC) permission (MODIFY,
/// SELECT, DROP, etc.) on the given table. When permission is denied an
/// appropriate user-readable api_error::access_denied is thrown.
future<> verify_permission(bool enforce_authorization, bool warn_authorization, const service::client_state&, const schema_ptr&, auth::permission, stats& stats);

/// Similar to verify_permission() above, but just for CREATE operations.
/// Those do not operate on any specific table, so require permissions on
/// ALL KEYSPACES instead of any specific table.
future<> verify_create_permission(bool enforce_authorization, bool warn_authorization, const service::client_state&, stats& stats);

// Sets a KeySchema JSON array inside the given parent object describing the
// key attributes of the given schema as HASH or RANGE keys. Additionally,
// adds mappings from key attribute names to their DynamoDB type string into
// attribute_types.
void describe_key_schema(rjson::value& parent, const schema&, std::unordered_map<std::string, std::string>* attribute_types = nullptr, const std::map<sstring, sstring>* tags = nullptr);

// Returns how many of the *leading* clustering-key columns of the given
// schema are genuine, user-specified sort-key (RANGE key) attributes, as
// opposed to spurious base-table key columns Alternator appends to a
// GSI view for materialized-view correctness. For base tables (which
// never have spurious clustering columns) and for schemas without the
// NUMBER_OF_USER_SPECIFIED_RANGE_KEYS_TAG_KEY tag (e.g., LSIs,
// or GSIs predating composite keys), this falls back to translation of the
// SPURIOUS_RANGE_KEY_ADDED_TO_GSI_AND_USER_DIDNT_SPECIFY_RANGE_KEY_TAG_KEY
// tag to either "0" or "1" genuine user-specified range keys.
uint8_t genuine_range_key_count(const schema&, const std::map<sstring, sstring>* tags);

/// The genuine user-specified partition key and sort key sizes
/// of a schema.
struct key_sizes {
    uint8_t pk_size;
    uint8_t sk_size;
    uint8_t total_size;
};

// Returns the number of partition key columns and genuine sort key
// columns (see genuine_range_key_count() above) of the given schema,
// GSIs may have a composite (multi-attribute) partition key of up to 4
// HASH columns and a composite sort key of up to 4 RANGE columns.
key_sizes get_key_sizes(const schema_ptr schema);

/// is_big() checks approximately if the given JSON value is "bigger" than
/// the given big_size number of bytes. The goal is to *quickly* detect
/// oversized JSON that, for example, is too large to be serialized to a
/// contiguous string - we don't need an accurate size for that. Moreover,
/// as soon as we detect that the JSON is indeed "big", we can return true
/// and don't need to continue calculating its exact size.
bool is_big(const rjson::value& val, int big_size = 100'000);

/// try_get_internal_table() handles the special case that the given table_name
/// begins with INTERNAL_TABLE_PREFIX (".scylla.alternator."). In that case,
/// this function assumes that the rest of the name refers to an internal
/// Scylla table (e.g., system table) and returns the schema of that table -
/// or an exception if it doesn't exist. Otherwise, if table_name does not
/// start with INTERNAL_TABLE_PREFIX, this function returns an empty schema_ptr
/// and the caller should look for a normal Alternator table with that name.
schema_ptr try_get_internal_table(const data_dictionary::database& db, std::string_view table_name);

/// get_table_from_batch_request() is used by batch write/read operations to
/// look up the schema for a table named in a batch request, by the JSON member
/// name (which is the table name in a BatchWriteItem or BatchGetItem request).
schema_ptr get_table_from_batch_request(const service::storage_proxy& proxy, const rjson::value::ConstMemberIterator& batch_request);

/// Returns (or lazily creates) the per-table stats object for the given schema.
/// If the table has been deleted, returns a temporary stats object.
lw_shared_ptr<stats> get_stats_from_schema(service::storage_proxy& sp, const schema& schema);

/// Filter tables from a batch request's "RequestItems" JSON
/// object. Removes members whose key is not in `tbl_name_filter`.
void filter_batch_request_items_by_tbl_name(rjson::value& request, const audit::audit_table_set& tbl_name_filter);

/// Writes one item's attributes into `item` from the given selection result
/// row. If include_all_embedded_attributes is true, all attributes from the
/// ATTRS_COLUMN map column are included regardless of attrs_to_get.
void describe_single_item(const cql3::selection::selection&,
    const std::vector<managed_bytes_opt>&,
    const std::optional<attrs_to_get>&,
    rjson::value&,
    uint64_t* item_length_in_bytes = nullptr,
    bool include_all_embedded_attributes = false);

/// Converts a single result row to a JSON item, or returns an empty optional
/// if the result is empty.
std::optional<rjson::value> describe_single_item(schema_ptr,
    const query::partition_slice&,
    const cql3::selection::selection&,
    const query::result&,
    const std::optional<attrs_to_get>&,
    uint64_t* item_length_in_bytes = nullptr);

/// Make a body_writer (function that can write output incrementally to the
/// HTTP stream) from the given JSON object.
/// Note: only useful for (very) large objects as there are overhead issues
/// with this as well, but for massive lists of return objects this can
/// help avoid large allocations/many re-allocs.
body_writer make_streamed(rjson::value&&);

} // namespace alternator
