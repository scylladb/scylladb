#!/usr/local/bin/thrift --java --php --py
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Copyright (C) 2014 ScyllaDB
#

#
# This file has been modified from the Apache distribution
# by ScyllaDB
#


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# *** PLEASE REMEMBER TO EDIT THE VERSION CONSTANT WHEN MAKING CHANGES ***
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#
# Interface definition for Cassandra Service
#

namespace java org.apache.cassandra.thrift
namespace cpp cassandra
namespace csharp Apache.Cassandra
namespace py cassandra
namespace php cassandra
namespace perl Cassandra

# Thrift.rb has a bug where top-level modules that include modules 
# with the same name are not properly referenced, so we can't do
# Cassandra::Cassandra::Client.
namespace rb CassandraThrift

# The API version (NOT the product version), composed as a dot delimited
# string with major, minor, and patch level components.
#
#  - Major: Incremented for backward incompatible changes. An example would
#           be changes to the number or disposition of method arguments.
#  - Minor: Incremented for backward compatible changes. An example would
#           be the addition of a new (optional) method.
#  - Patch: Incremented for bug fixes. The patch level should be increased
#           for every edit that doesn't result in a change to major/minor.
#
# See the Semantic Versioning Specification (SemVer) http://semver.org.
#
# Note that this backwards compatibility is from the perspective of the server,
# not the client. Cassandra should always be able to talk to older client
# software, but client software may not be able to talk to older Cassandra
# instances.
#
# An effort should be made not to break forward-client-compatibility either
# (e.g. one should avoid removing obsolete fields from the IDL), but no
# guarantees in this respect are made by the Cassandra project.
const string VERSION_ = "20.1.0"


#
# data structures
#

/** Basic unit of data within a ColumnFamily.
 * @param name, the name by which this column is set and retrieved.  Maximum 64KB long.
 * @param value. The data associated with the name.  Maximum 2GB long, but in practice you should limit it to small numbers of MB (since Thrift must read the full value into memory to operate on it).
 * @param timestamp. The timestamp is used for conflict detection/resolution when two columns with same name need to be compared.
 * @param ttl. An optional, positive delay (in seconds) after which the column will be automatically deleted. 
 */
struct Column {
   1: required binary name,
   2: optional binary value,
   3: optional i64 timestamp,
   4: optional i32 ttl,
}

/** A named list of columns.
 * @param name. see Column.name.
 * @param columns. A collection of standard Columns.  The columns within a super column are defined in an adhoc manner.
 *                 Columns within a super column do not have to have matching structures (similarly named child columns).
 */
struct SuperColumn {
   1: required binary name,
   2: required list<Column> columns,
}

struct CounterColumn {
    1: required binary name,
    2: required i64 value
}

struct CounterSuperColumn {
    1: required binary name,
    2: required list<CounterColumn> columns
}

/**
    Methods for fetching rows/records from Cassandra will return either a single instance of ColumnOrSuperColumn or a list
    of ColumnOrSuperColumns (get_slice()). If you're looking up a SuperColumn (or list of SuperColumns) then the resulting
    instances of ColumnOrSuperColumn will have the requested SuperColumn in the attribute super_column. For queries resulting
    in Columns, those values will be in the attribute column. This change was made between 0.3 and 0.4 to standardize on
    single query methods that may return either a SuperColumn or Column.

    If the query was on a counter column family, you will either get a counter_column (instead of a column) or a 
    counter_super_column (instead of a super_column)

    @param column. The Column returned by get() or get_slice().
    @param super_column. The SuperColumn returned by get() or get_slice().
    @param counter_column. The Counterolumn returned by get() or get_slice().
    @param counter_super_column. The CounterSuperColumn returned by get() or get_slice().
 */
struct ColumnOrSuperColumn {
    1: optional Column column,
    2: optional SuperColumn super_column,
    3: optional CounterColumn counter_column,
    4: optional CounterSuperColumn counter_super_column
}


#
# Exceptions
# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)
#

/** A specific column was requested that does not exist. */
exception NotFoundException {
}

/** Invalid request could mean keyspace or column family does not exist, required parameters are missing, or a parameter is malformed. 
    why contains an associated error message.
*/
exception InvalidRequestException {
    1: required string why
}

/** Not all the replicas required could be created and/or read. */
exception UnavailableException {
}

/** RPC timeout was exceeded.  either a node failed mid-operation, or load was too high, or the requested op was too large. */
exception TimedOutException {
    /**
     * if a write operation was acknowledged by some replicas but not by enough to
     * satisfy the required ConsistencyLevel, the number of successful
     * replies will be given here. In case of atomic_batch_mutate method this field
     * will be set to -1 if the batch was written to the batchlog and to 0 if it wasn't.
     */
    1: optional i32 acknowledged_by

    /** 
     * in case of atomic_batch_mutate method this field tells if the batch 
     * was written to the batchlog.  
     */
    2: optional bool acknowledged_by_batchlog

    /** 
     * for the CAS method, this field tells if we timed out during the paxos
     * protocol, as opposed to during the commit of our update
     */
    3: optional bool paxos_in_progress
}

/** invalid authentication request (invalid keyspace, user does not exist, or credentials invalid) */
exception AuthenticationException {
    1: required string why
}

/** invalid authorization request (user does not have access to keyspace) */
exception AuthorizationException {
    1: required string why
}

/**
 * NOTE: This up outdated exception left for backward compatibility reasons,
 * no actual schema agreement validation is done starting from Cassandra 1.2
 *
 * schemas are not in agreement across all nodes
 */
exception SchemaDisagreementException {
}


#
# service api
#
/** 
 * The ConsistencyLevel is an enum that controls both read and write
 * behavior based on the ReplicationFactor of the keyspace.  The
 * different consistency levels have different meanings, depending on
 * if you're doing a write or read operation. 
 *
 * If W + R > ReplicationFactor, where W is the number of nodes to
 * block for on write, and R the number to block for on reads, you
 * will have strongly consistent behavior; that is, readers will
 * always see the most recent write. Of these, the most interesting is
 * to do QUORUM reads and writes, which gives you consistency while
 * still allowing availability in the face of node failures up to half
 * of <ReplicationFactor>. Of course if latency is more important than
 * consistency then you can use lower values for either or both.
 * 
 * Some ConsistencyLevels (ONE, TWO, THREE) refer to a specific number
 * of replicas rather than a logical concept that adjusts
 * automatically with the replication factor.  Of these, only ONE is
 * commonly used; TWO and (even more rarely) THREE are only useful
 * when you care more about guaranteeing a certain level of
 * durability, than consistency.
 * 
 * Write consistency levels make the following guarantees before reporting success to the client:
 *   ANY          Ensure that the write has been written once somewhere, including possibly being hinted in a non-target node.
 *   ONE          Ensure that the write has been written to at least 1 node's commit log and memory table
 *   TWO          Ensure that the write has been written to at least 2 node's commit log and memory table
 *   THREE        Ensure that the write has been written to at least 3 node's commit log and memory table
 *   QUORUM       Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes
 *   LOCAL_ONE    Ensure that the write has been written to 1 node within the local datacenter (requires NetworkTopologyStrategy)
 *   LOCAL_QUORUM Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes, within the local datacenter (requires NetworkTopologyStrategy)
 *   EACH_QUORUM  Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes in each datacenter (requires NetworkTopologyStrategy)
 *   ALL          Ensure that the write is written to <code>&lt;ReplicationFactor&gt;</code> nodes before responding to the client.
 * 
 * Read consistency levels make the following guarantees before returning successful results to the client:
 *   ANY          Not supported. You probably want ONE instead.
 *   ONE          Returns the record obtained from a single replica.
 *   TWO          Returns the record with the most recent timestamp once two replicas have replied.
 *   THREE        Returns the record with the most recent timestamp once three replicas have replied.
 *   QUORUM       Returns the record with the most recent timestamp once a majority of replicas have replied.
 *   LOCAL_ONE    Returns the record with the most recent timestamp once a single replica within the local datacenter have replied.
 *   LOCAL_QUORUM Returns the record with the most recent timestamp once a majority of replicas within the local datacenter have replied.
 *   EACH_QUORUM  Returns the record with the most recent timestamp once a majority of replicas within each datacenter have replied.
 *   ALL          Returns the record with the most recent timestamp once all replicas have replied (implies no replica may be down)..
*/
enum ConsistencyLevel {
    ONE = 1,
    QUORUM = 2,
    LOCAL_QUORUM = 3,
    EACH_QUORUM = 4,
    ALL = 5,
    ANY = 6,
    TWO = 7,
    THREE = 8,
    SERIAL = 9,
    LOCAL_SERIAL = 10,
    LOCAL_ONE = 11,
}

/**
    ColumnParent is used when selecting groups of columns from the same ColumnFamily. In directory structure terms, imagine
    ColumnParent as ColumnPath + '/../'.

    See also <a href="cassandra.html#Struct_ColumnPath">ColumnPath</a>
 */
struct ColumnParent {
    3: required string column_family,
    4: optional binary super_column,
}

/** The ColumnPath is the path to a single column in Cassandra. It might make sense to think of ColumnPath and
 * ColumnParent in terms of a directory structure.
 *
 * ColumnPath is used to looking up a single column.
 *
 * @param column_family. The name of the CF of the column being looked up.
 * @param super_column. The super column name.
 * @param column. The column name.
 */
struct ColumnPath {
    3: required string column_family,
    4: optional binary super_column,
    5: optional binary column,
}

/**
    A slice range is a structure that stores basic range, ordering and limit information for a query that will return
    multiple columns. It could be thought of as Cassandra's version of LIMIT and ORDER BY

    @param start. The column name to start the slice with. This attribute is not required, though there is no default value,
                  and can be safely set to '', i.e., an empty byte array, to start with the first column name. Otherwise, it
                  must a valid value under the rules of the Comparator defined for the given ColumnFamily.
    @param finish. The column name to stop the slice at. This attribute is not required, though there is no default value,
                   and can be safely set to an empty byte array to not stop until 'count' results are seen. Otherwise, it
                   must also be a valid value to the ColumnFamily Comparator.
    @param reversed. Whether the results should be ordered in reversed order. Similar to ORDER BY blah DESC in SQL.
    @param count. How many columns to return. Similar to LIMIT in SQL. May be arbitrarily large, but Thrift will
                  materialize the whole result into memory before returning it to the client, so be aware that you may
                  be better served by iterating through slices by passing the last value of one call in as the 'start'
                  of the next instead of increasing 'count' arbitrarily large.
 */
struct SliceRange {
    1: required binary start,
    2: required binary finish,
    3: required bool reversed=0,
    4: required i32 count=100,
}

/**
    A SlicePredicate is similar to a mathematic predicate (see http://en.wikipedia.org/wiki/Predicate_(mathematical_logic)),
    which is described as "a property that the elements of a set have in common."

    SlicePredicate's in Cassandra are described with either a list of column_names or a SliceRange.  If column_names is
    specified, slice_range is ignored.

    @param column_name. A list of column names to retrieve. This can be used similar to Memcached's "multi-get" feature
                        to fetch N known column names. For instance, if you know you wish to fetch columns 'Joe', 'Jack',
                        and 'Jim' you can pass those column names as a list to fetch all three at once.
    @param slice_range. A SliceRange describing how to range, order, and/or limit the slice.
 */
struct SlicePredicate {
    1: optional list<binary> column_names,
    2: optional SliceRange   slice_range,
}

enum IndexOperator {
    EQ,
    GTE,
    GT,
    LTE,
    LT
}

struct IndexExpression {
    1: required binary column_name,
    2: required IndexOperator op,
    3: required binary value,
}

/**
 * @deprecated use a KeyRange with row_filter in get_range_slices instead
 */
struct IndexClause {
    1: required list<IndexExpression> expressions,
    2: required binary start_key,
    3: required i32 count=100,
}


/**
The semantics of start keys and tokens are slightly different.
Keys are start-inclusive; tokens are start-exclusive.  Token
ranges may also wrap -- that is, the end token may be less
than the start one.  Thus, a range from keyX to keyX is a
one-element range, but a range from tokenY to tokenY is the
full ring.
*/
struct KeyRange {
    1: optional binary start_key,
    2: optional binary end_key,
    3: optional string start_token,
    4: optional string end_token,
    6: optional list<IndexExpression> row_filter,
    5: required i32 count=100
}

/**
    A KeySlice is key followed by the data it maps to. A collection of KeySlice is returned by the get_range_slice operation.

    @param key. a row key
    @param columns. List of data represented by the key. Typically, the list is pared down to only the columns specified by
                    a SlicePredicate.
 */
struct KeySlice {
    1: required binary key,
    2: required list<ColumnOrSuperColumn> columns,
}

struct KeyCount {
    1: required binary key,
    2: required i32 count
}

/**
 * Note that the timestamp is only optional in case of counter deletion.
 */
struct Deletion {
    1: optional i64 timestamp,
    2: optional binary super_column,
    3: optional SlicePredicate predicate,
}

/**
    A Mutation is either an insert (represented by filling column_or_supercolumn) or a deletion (represented by filling the deletion attribute).
    @param column_or_supercolumn. An insert to a column or supercolumn (possibly counter column or supercolumn)
    @param deletion. A deletion of a column or supercolumn
*/
struct Mutation {
    1: optional ColumnOrSuperColumn column_or_supercolumn,
    2: optional Deletion deletion,
}

struct EndpointDetails {
    1: string host,
    2: string datacenter,
    3: optional string rack
}

struct CASResult {
    1: required bool success,
    2: optional list<Column> current_values,
}

/**
    A TokenRange describes part of the Cassandra ring, it is a mapping from a range to
    endpoints responsible for that range.
    @param start_token The first token in the range
    @param end_token The last token in the range
    @param endpoints The endpoints responsible for the range (listed by their configured listen_address)
    @param rpc_endpoints The endpoints responsible for the range (listed by their configured rpc_address)
*/
struct TokenRange {
    1: required string start_token,
    2: required string end_token,
    3: required list<string> endpoints,
    4: optional list<string> rpc_endpoints
    5: optional list<EndpointDetails> endpoint_details,
}

/**
    Authentication requests can contain any data, dependent on the IAuthenticator used
*/
struct AuthenticationRequest {
    1: required map<string, string> credentials
}

enum IndexType {
    KEYS,
    CUSTOM,
    COMPOSITES
}

/* describes a column in a column family. */
struct ColumnDef {
    1: required binary name,
    2: required string validation_class,
    3: optional IndexType index_type,
    4: optional string index_name,
    5: optional map<string,string> index_options
}

/**
    Describes a trigger.
    `options` should include at least 'class' param.
    Other options are not supported yet.
*/
struct TriggerDef {
    1: required string name,
    2: required map<string,string> options
}

/* describes a column family. */
struct CfDef {
    1: required string keyspace,
    2: required string name,
    3: optional string column_type="Standard",
    5: optional string comparator_type="BytesType",
    6: optional string subcomparator_type,
    8: optional string comment,
    12: optional double read_repair_chance,
    13: optional list<ColumnDef> column_metadata,
    14: optional i32 gc_grace_seconds,
    15: optional string default_validation_class,
    16: optional i32 id,
    17: optional i32 min_compaction_threshold,
    18: optional i32 max_compaction_threshold,
    26: optional string key_validation_class,
    28: optional binary key_alias,
    29: optional string compaction_strategy,
    30: optional map<string,string> compaction_strategy_options,
    32: optional map<string,string> compression_options,
    33: optional double bloom_filter_fp_chance,
    34: optional string caching="keys_only",
    37: optional double dclocal_read_repair_chance = 0.0,
    39: optional i32 memtable_flush_period_in_ms,
    40: optional i32 default_time_to_live,
    42: optional string speculative_retry="NONE",
    43: optional list<TriggerDef> triggers,
    44: optional string cells_per_row_to_cache = "100",
    45: optional i32 min_index_interval,
    46: optional i32 max_index_interval,

    /* All of the following are now ignored and unsupplied. */

    /** @deprecated */
    9: optional double row_cache_size,
    /** @deprecated */
    11: optional double key_cache_size,
    /** @deprecated */
    19: optional i32 row_cache_save_period_in_seconds,
    /** @deprecated */
    20: optional i32 key_cache_save_period_in_seconds,
    /** @deprecated */
    21: optional i32 memtable_flush_after_mins,
    /** @deprecated */
    22: optional i32 memtable_throughput_in_mb,
    /** @deprecated */
    23: optional double memtable_operations_in_millions,
    /** @deprecated */
    24: optional bool replicate_on_write,
    /** @deprecated */
    25: optional double merge_shards_chance,
    /** @deprecated */
    27: optional string row_cache_provider,
    /** @deprecated */
    31: optional i32 row_cache_keys_to_save,
    /** @deprecated */
    38: optional bool populate_io_cache_on_flush,
    /** @deprecated */
    41: optional i32 index_interval,
}

/* describes a keyspace. */
struct KsDef {
    1: required string name,
    2: required string strategy_class,
    3: optional map<string,string> strategy_options,

    /** @deprecated ignored */
    4: optional i32 replication_factor,

    5: required list<CfDef> cf_defs,
    6: optional bool durable_writes=1,
}

/** CQL query compression */
enum Compression {
    GZIP = 1,
    NONE = 2
}

enum CqlResultType {
    ROWS = 1,
    VOID = 2,
    INT = 3
}

/** 
  Row returned from a CQL query.

  This struct is used for both CQL2 and CQL3 queries.  For CQL2, the partition key
  is special-cased and is always returned.  For CQL3, it is not special cased;
  it will be included in the columns list if it was included in the SELECT and
  the key field is always null.
*/
struct CqlRow {
    1: required binary key,
    2: required list<Column> columns
}

struct CqlMetadata {
    1: required map<binary,string> name_types,
    2: required map<binary,string> value_types,
    3: required string default_name_type,
    4: required string default_value_type
}

struct CqlResult {
    1: required CqlResultType type,
    2: optional list<CqlRow> rows,
    3: optional i32 num,
    4: optional CqlMetadata schema
}

struct CqlPreparedResult {
    1: required i32 itemId,
    2: required i32 count,
    3: optional list<string> variable_types,
    4: optional list<string> variable_names
}

/** Represents input splits used by hadoop ColumnFamilyRecordReaders */
struct CfSplit {
    1: required string start_token,
    2: required string end_token,
    3: required i64 row_count
}

/** The ColumnSlice is used to select a set of columns from inside a row. 
 * If start or finish are unspecified they will default to the start-of
 * end-of value.
 * @param start. The start of the ColumnSlice inclusive
 * @param finish. The end of the ColumnSlice inclusive
 */
struct ColumnSlice {
    1: optional binary start,
    2: optional binary finish
}

/**
 * Used to perform multiple slices on a single row key in one rpc operation
 * @param key. The row key to be multi sliced
 * @param column_parent. The column family (super columns are unsupported)
 * @param column_slices. 0 to many ColumnSlice objects each will be used to select columns
 * @param reversed. Direction of slice
 * @param count. Maximum number of columns
 * @param consistency_level. Level to perform the operation at
 */
struct MultiSliceRequest {
    1: optional binary key,
    2: optional ColumnParent column_parent,
    3: optional list<ColumnSlice> column_slices,
    4: optional bool reversed=false,
    5: optional i32 count=1000,
    6: optional ConsistencyLevel consistency_level=ConsistencyLevel.ONE
}

service Cassandra {
  # auth methods
  void login(1: required AuthenticationRequest auth_request) throws (1:AuthenticationException authnx, 2:AuthorizationException authzx),
 
  # set keyspace
  void set_keyspace(1: required string keyspace) throws (1:InvalidRequestException ire),
  
  # retrieval methods

  /**
    Get the Column or SuperColumn at the given column_path. If no value is present, NotFoundException is thrown. (This is
    the only method that can throw an exception under non-failure conditions.)
   */
  ColumnOrSuperColumn get(1:required binary key,
                          2:required ColumnPath column_path,
                          3:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                      throws (1:InvalidRequestException ire, 2:NotFoundException nfe, 3:UnavailableException ue, 4:TimedOutException te),

  /**
    Get the group of columns contained by column_parent (either a ColumnFamily name or a ColumnFamily/SuperColumn name
    pair) specified by the given SlicePredicate. If no matching values are found, an empty list is returned.
   */
  list<ColumnOrSuperColumn> get_slice(1:required binary key, 
                                      2:required ColumnParent column_parent, 
                                      3:required SlicePredicate predicate, 
                                      4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                            throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    returns the number of columns matching <code>predicate</code> for a particular <code>key</code>, 
    <code>ColumnFamily</code> and optionally <code>SuperColumn</code>.
  */
  i32 get_count(1:required binary key, 
                2:required ColumnParent column_parent, 
                3:required SlicePredicate predicate,
                4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Performs a get_slice for column_parent and predicate for the given keys in parallel.
  */
  map<binary,list<ColumnOrSuperColumn>> multiget_slice(1:required list<binary> keys, 
                                                       2:required ColumnParent column_parent, 
                                                       3:required SlicePredicate predicate, 
                                                       4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                                        throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Perform a get_count in parallel on the given list<binary> keys. The return value maps keys to the count found.
  */
  map<binary, i32> multiget_count(1:required list<binary> keys,
                2:required ColumnParent column_parent,
                3:required SlicePredicate predicate,
                4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   returns a subset of columns for a contiguous range of keys.
  */
  list<KeySlice> get_range_slices(1:required ColumnParent column_parent, 
                                  2:required SlicePredicate predicate,
                                  3:required KeyRange range,
                                  4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   returns a range of columns, wrapping to the next rows if necessary to collect max_results.
  */
  list<KeySlice> get_paged_slice(1:required string column_family,
                                 2:required KeyRange range,
                                 3:required binary start_column,
                                 4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Returns the subset of columns specified in SlicePredicate for the rows matching the IndexClause
    @deprecated use get_range_slices instead with range.row_filter specified
    */
  list<KeySlice> get_indexed_slices(1:required ColumnParent column_parent,
                                    2:required IndexClause index_clause,
                                    3:required SlicePredicate column_predicate,
                                    4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  # modification methods

  /**
   * Insert a Column at the given column_parent.column_family and optional column_parent.super_column.
   */
  void insert(1:required binary key, 
              2:required ColumnParent column_parent,
              3:required Column column,
              4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   * Increment or decrement a counter.
   */
  void add(1:required binary key,
           2:required ColumnParent column_parent,
           3:required CounterColumn column,
           4:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   * Atomic compare and set.
   *
   * If the cas is successfull, the success boolean in CASResult will be true and there will be no current_values.
   * Otherwise, success will be false and current_values will contain the current values for the columns in
   * expected (that, by definition of compare-and-set, will differ from the values in expected).
   *
   * A cas operation takes 2 consistency level. The first one, serial_consistency_level, simply indicates the
   * level of serialization required. This can be either ConsistencyLevel.SERIAL or ConsistencyLevel.LOCAL_SERIAL.
   * The second one, commit_consistency_level, defines the consistency level for the commit phase of the cas. This
   * is a more traditional consistency level (the same CL than for traditional writes are accepted) that impact
   * the visibility for reads of the operation. For instance, if commit_consistency_level is QUORUM, then it is
   * guaranteed that a followup QUORUM read will see the cas write (if that one was successful obviously). If
   * commit_consistency_level is ANY, you will need to use a SERIAL/LOCAL_SERIAL read to be guaranteed to see
   * the write.
   */
  CASResult cas(1:required binary key,
                2:required string column_family,
                3:list<Column> expected,
                4:list<Column> updates,
                5:required ConsistencyLevel serial_consistency_level=ConsistencyLevel.SERIAL,
                6:required ConsistencyLevel commit_consistency_level=ConsistencyLevel.QUORUM)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Remove data from the row specified by key at the granularity specified by column_path, and the given timestamp. Note
    that all the values in column_path besides column_path.column_family are truly optional: you can remove the entire
    row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column by specifying those levels too.
   */
  void remove(1:required binary key,
              2:required ColumnPath column_path,
              3:required i64 timestamp,
              4:ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   * Remove a counter at the specified location.
   * Note that counters have limited support for deletes: if you remove a counter, you must wait to issue any following update
   * until the delete has reached all the nodes and all of them have been fully compacted.
   */
  void remove_counter(1:required binary key,
                      2:required ColumnPath path,
                      3:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Mutate many columns or super columns for many row keys. See also: Mutation.

    mutation_map maps key to column family to a list of Mutation objects to take place at that scope.
  **/
  void batch_mutate(1:required map<binary, map<string, list<Mutation>>> mutation_map,
                    2:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Atomically mutate many columns or super columns for many row keys. See also: Mutation.

    mutation_map maps key to column family to a list of Mutation objects to take place at that scope.
  **/
  void atomic_batch_mutate(1:required map<binary, map<string, list<Mutation>>> mutation_map,
                           2:required ConsistencyLevel consistency_level=ConsistencyLevel.ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   Truncate will mark and entire column family as deleted.
   From the user's perspective a successful call to truncate will result complete data deletion from cfname.
   Internally, however, disk space will not be immediatily released, as with all deletes in cassandra, this one
   only marks the data as deleted.
   The operation succeeds only if all hosts in the cluster at available and will throw an UnavailableException if 
   some hosts are down.
  */
  void truncate(1:required string cfname)
       throws (1: InvalidRequestException ire, 2: UnavailableException ue, 3: TimedOutException te),

  /**
  * Select multiple slices of a key in a single RPC operation
  */
  list<ColumnOrSuperColumn> get_multi_slice(1:required MultiSliceRequest request)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),
    
  // Meta-APIs -- APIs to get information about the node or cluster,
  // rather than user data.  The nodeprobe program provides usage examples.
  
  /** 
   * for each schema version present in the cluster, returns a list of nodes at that version.
   * hosts that do not respond will be under the key DatabaseDescriptor.INITIAL_VERSION. 
   * the cluster is all on the same version if the size of the map is 1. 
   */
  map<string, list<string>> describe_schema_versions()
       throws (1: InvalidRequestException ire),

  /** list the defined keyspaces in this cluster */
  list<KsDef> describe_keyspaces()
    throws (1:InvalidRequestException ire),

  /** get the cluster name */
  string describe_cluster_name(),

  /** get the thrift api version */
  string describe_version(),

  /** get the token ring: a map of ranges to host addresses,
      represented as a set of TokenRange instead of a map from range
      to list of endpoints, because you can't use Thrift structs as
      map keys:
      https://issues.apache.org/jira/browse/THRIFT-162 

      for the same reason, we can't return a set here, even though
      order is neither important nor predictable. */
  list<TokenRange> describe_ring(1:required string keyspace)
                   throws (1:InvalidRequestException ire),


  /** same as describe_ring, but considers only nodes in the local DC */
  list<TokenRange> describe_local_ring(1:required string keyspace)
                   throws (1:InvalidRequestException ire),

  /** get the mapping between token->node ip
      without taking replication into consideration
      https://issues.apache.org/jira/browse/CASSANDRA-4092 */
  map<string, string> describe_token_map()
                    throws (1:InvalidRequestException ire),
  
  /** returns the partitioner used by this cluster */
  string describe_partitioner(),

  /** returns the snitch used by this cluster */
  string describe_snitch(),

  /** describe specified keyspace */
  KsDef describe_keyspace(1:required string keyspace)
    throws (1:NotFoundException nfe, 2:InvalidRequestException ire),

  /** experimental API for hadoop/parallel query support.  
      may change violently and without warning. 

      returns list of token strings such that first subrange is (list[0], list[1]],
      next is (list[1], list[2]], etc. */
  list<string> describe_splits(1:required string cfName,
                               2:required string start_token, 
                               3:required string end_token,
                               4:required i32 keys_per_split)
    throws (1:InvalidRequestException ire),

  /** Enables tracing for the next query in this connection and returns the UUID for that trace session
      The next query will be traced idependently of trace probability and the returned UUID can be used to query the trace keyspace */
  binary trace_next_query(),

  list<CfSplit> describe_splits_ex(1:required string cfName,
                                   2:required string start_token,
                                   3:required string end_token,
                                   4:required i32 keys_per_split)
    throws (1:InvalidRequestException ire), 

  /** adds a column family. returns the new schema id. */
  string system_add_column_family(1:required CfDef cf_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
    
  /** drops a column family. returns the new schema id. */
  string system_drop_column_family(1:required string column_family)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde), 
  
  /** adds a keyspace and any column families that are part of it. returns the new schema id. */
  string system_add_keyspace(1:required KsDef ks_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
  
  /** drops a keyspace and any column families that are part of it. returns the new schema id. */
  string system_drop_keyspace(1:required string keyspace)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
  
  /** updates properties of a keyspace. returns the new schema id. */
  string system_update_keyspace(1:required KsDef ks_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),
        
  /** updates properties of a column family. returns the new schema id. */
  string system_update_column_family(1:required CfDef cf_def)
    throws (1:InvalidRequestException ire, 2:SchemaDisagreementException sde),


  /**
   * @deprecated Throws InvalidRequestException since 3.0. Please use the CQL3 version instead.
   */
  CqlResult execute_cql_query(1:required binary query, 2:required Compression compression)
    throws (1:InvalidRequestException ire,
            2:UnavailableException ue,
            3:TimedOutException te,
            4:SchemaDisagreementException sde)

  /**
   * Executes a CQL3 (Cassandra Query Language) statement and returns a
   * CqlResult containing the results.
   */
  CqlResult execute_cql3_query(1:required binary query, 2:required Compression compression, 3:required ConsistencyLevel consistency)
    throws (1:InvalidRequestException ire,
            2:UnavailableException ue,
            3:TimedOutException te,
            4:SchemaDisagreementException sde)


  /**
   * @deprecated Throws InvalidRequestException since 3.0. Please use the CQL3 version instead.
   */
  CqlPreparedResult prepare_cql_query(1:required binary query, 2:required Compression compression)
    throws (1:InvalidRequestException ire)

  /**
   * Prepare a CQL3 (Cassandra Query Language) statement by compiling and returning
   * - the type of CQL statement
   * - an id token of the compiled CQL stored on the server side.
   * - a count of the discovered bound markers in the statement
   */
  CqlPreparedResult prepare_cql3_query(1:required binary query, 2:required Compression compression)
    throws (1:InvalidRequestException ire)


  /**
   * @deprecated Throws InvalidRequestException since 3.0. Please use the CQL3 version instead.
   */
  CqlResult execute_prepared_cql_query(1:required i32 itemId, 2:required list<binary> values)
    throws (1:InvalidRequestException ire,
            2:UnavailableException ue,
            3:TimedOutException te,
            4:SchemaDisagreementException sde)

  /**
   * Executes a prepared CQL3 (Cassandra Query Language) statement by passing an id token, a list of variables
   * to bind, and the consistency level, and returns a CqlResult containing the results.
   */
  CqlResult execute_prepared_cql3_query(1:required i32 itemId, 2:required list<binary> values, 3:required ConsistencyLevel consistency)
    throws (1:InvalidRequestException ire,
            2:UnavailableException ue,
            3:TimedOutException te,
            4:SchemaDisagreementException sde)

  /**
   * @deprecated This is now a no-op. Please use the CQL3 specific methods instead.
   */
  void set_cql_version(1: required string version) throws (1:InvalidRequestException ire)
}
