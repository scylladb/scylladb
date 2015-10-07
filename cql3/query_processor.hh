/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2015 Cloudius Systems
 *
 * Modified by Cloudius Systems
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

#include <experimental/string_view>
#include <unordered_map>

#include "core/shared_ptr.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/cf_statement.hh"
#include "service/migration_manager.hh"
#include "service/query_state.hh"
#include "log.hh"
#include "core/distributed.hh"
#include "transport/messages/result_message.hh"
#include "untyped_result_set.hh"

namespace cql3 {

namespace statements {
class batch_statement;
}

class query_processor {
private:
    distributed<service::storage_proxy>& _proxy;
    distributed<database>& _db;

    class internal_state;
    std::unique_ptr<internal_state> _internal_state;

public:
    query_processor(distributed<service::storage_proxy>& proxy, distributed<database>& db);
    ~query_processor();

    static const sstring CQL_VERSION;

    distributed<database>& db() {
        return _db;
    }
    distributed<service::storage_proxy>& proxy() {
        return _proxy;
    }
#if 0
    public static final QueryProcessor instance = new QueryProcessor();
#endif
private:
#if 0
    private static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
    private static final MemoryMeter meter = new MemoryMeter().withGuessing(MemoryMeter.Guess.FALLBACK_BEST).ignoreKnownSingletons();
    private static final long MAX_CACHE_PREPARED_MEMORY = Runtime.getRuntime().maxMemory() / 256;

    private static EntryWeigher<MD5Digest, ParsedStatement.Prepared> cqlMemoryUsageWeigher = new EntryWeigher<MD5Digest, ParsedStatement.Prepared>()
    {
        @Override
        public int weightOf(MD5Digest key, ParsedStatement.Prepared value)
        {
            return Ints.checkedCast(measure(key) + measure(value.statement) + measure(value.boundNames));
        }
    };

    private static EntryWeigher<Integer, ParsedStatement.Prepared> thriftMemoryUsageWeigher = new EntryWeigher<Integer, ParsedStatement.Prepared>()
    {
        @Override
        public int weightOf(Integer key, ParsedStatement.Prepared value)
        {
            return Ints.checkedCast(measure(key) + measure(value.statement) + measure(value.boundNames));
        }
    };
#endif

    std::unordered_map<bytes, ::shared_ptr<statements::parsed_statement::prepared>> _prepared_statements;
    std::unordered_map<sstring, ::shared_ptr<statements::parsed_statement::prepared>> _internal_statements;
#if 0
    private static final ConcurrentLinkedHashMap<Integer, ParsedStatement.Prepared> thriftPreparedStatements;

    // A map for prepared statements used internally (which we don't want to mix with user statement, in particular we don't
    // bother with expiration on those.
    private static final ConcurrentMap<String, ParsedStatement.Prepared> internalStatements = new ConcurrentHashMap<>();

    // Direct calls to processStatement do not increment the preparedStatementsExecuted/regularStatementsExecuted
    // counters. Callers of processStatement are responsible for correctly notifying metrics
    public static final CQLMetrics metrics = new CQLMetrics();

    private static final AtomicInteger lastMinuteEvictionsCount = new AtomicInteger(0);

    static
    {
        preparedStatements = new ConcurrentLinkedHashMap.Builder<MD5Digest, ParsedStatement.Prepared>()
                             .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                             .weigher(cqlMemoryUsageWeigher)
                             .listener(new EvictionListener<MD5Digest, ParsedStatement.Prepared>()
                             {
                                 public void onEviction(MD5Digest md5Digest, ParsedStatement.Prepared prepared)
                                 {
                                     metrics.preparedStatementsEvicted.inc();
                                     lastMinuteEvictionsCount.incrementAndGet();
                                 }
                             }).build();

        thriftPreparedStatements = new ConcurrentLinkedHashMap.Builder<Integer, ParsedStatement.Prepared>()
                                   .maximumWeightedCapacity(MAX_CACHE_PREPARED_MEMORY)
                                   .weigher(thriftMemoryUsageWeigher)
                                   .listener(new EvictionListener<Integer, ParsedStatement.Prepared>()
                                   {
                                       public void onEviction(Integer integer, ParsedStatement.Prepared prepared)
                                       {
                                           metrics.preparedStatementsEvicted.inc();
                                           lastMinuteEvictionsCount.incrementAndGet();
                                       }
                                   })
                                   .build();

        ScheduledExecutors.scheduledTasks.scheduleAtFixedRate(new Runnable()
        {
            public void run()
            {
                long count = lastMinuteEvictionsCount.getAndSet(0);
                if (count > 0)
                    logger.info("{} prepared statements discarded in the last minute because cache limit reached ({} bytes)",
                                count,
                                MAX_CACHE_PREPARED_MEMORY);
            }
        }, 1, 1, TimeUnit.MINUTES);
    }

    public static int preparedStatementsCount()
    {
        return preparedStatements.size() + thriftPreparedStatements.size();
    }

    // Work around initialization dependency
    private static enum InternalStateInstance
    {
        INSTANCE;

        private final QueryState queryState;

        InternalStateInstance()
        {
            ClientState state = ClientState.forInternalCalls();
            try
            {
                state.setKeyspace(SystemKeyspace.NAME);
            }
            catch (InvalidRequestException e)
            {
                throw new RuntimeException();
            }
            this.queryState = new QueryState(state);
        }
    }

    private static QueryState internalQueryState()
    {
        return InternalStateInstance.INSTANCE.queryState;
    }

    private QueryProcessor()
    {
        MigrationManager.instance.register(new MigrationSubscriber());
    }
#endif
public:
    ::shared_ptr<statements::parsed_statement::prepared> get_prepared(const bytes& id) {
        auto it = _prepared_statements.find(id);
        if (it == _prepared_statements.end()) {
            return ::shared_ptr<statements::parsed_statement::prepared>{};
        }
        return it->second;
    }

#if 0
    public ParsedStatement.Prepared getPreparedForThrift(Integer id)
    {
        return thriftPreparedStatements.get(id);
    }

    public static void validateKey(ByteBuffer key) throws InvalidRequestException
    {
        if (key == null || key.remaining() == 0)
        {
            throw new InvalidRequestException("Key may not be empty");
        }

        // check that key can be handled by FBUtilities.writeShortByteArray
        if (key.remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            throw new InvalidRequestException("Key length of " + key.remaining() +
                                              " is longer than maximum of " + FBUtilities.MAX_UNSIGNED_SHORT);
        }
    }

    public static void validateCellNames(Iterable<CellName> cellNames, CellNameType type) throws InvalidRequestException
    {
        for (CellName name : cellNames)
            validateCellName(name, type);
    }

    public static void validateCellName(CellName name, CellNameType type) throws InvalidRequestException
    {
        validateComposite(name, type);
        if (name.isEmpty())
            throw new InvalidRequestException("Invalid empty value for clustering column of COMPACT TABLE");
    }

    public static void validateComposite(Composite name, CType type) throws InvalidRequestException
    {
        long serializedSize = type.serializer().serializedSize(name, TypeSizes.NATIVE);
        if (serializedSize > Cell.MAX_NAME_LENGTH)
            throw new InvalidRequestException(String.format("The sum of all clustering columns is too long (%s > %s)",
                                                            serializedSize,
                                                            Cell.MAX_NAME_LENGTH));
    }
#endif
public:
    future<::shared_ptr<transport::messages::result_message>> process_statement(::shared_ptr<cql_statement> statement,
            service::query_state& query_state, const query_options& options);

#if 0
    public static ResultMessage process(String queryString, ConsistencyLevel cl, QueryState queryState)
    throws RequestExecutionException, RequestValidationException
    {
        return instance.process(queryString, queryState, QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList()));
    }
#endif

    future<::shared_ptr<transport::messages::result_message>> process(const std::experimental::string_view& query_string,
            service::query_state& query_state, query_options& options);

#if 0
    public static ParsedStatement.Prepared parseStatement(String queryStr, QueryState queryState) throws RequestValidationException
    {
        return getStatement(queryStr, queryState.getClientState());
    }

    public static UntypedResultSet process(String query, ConsistencyLevel cl) throws RequestExecutionException
    {
        try
        {
            ResultMessage result = instance.process(query, QueryState.forInternalCalls(), QueryOptions.forInternalCalls(cl, Collections.<ByteBuffer>emptyList()));
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static QueryOptions makeInternalOptions(ParsedStatement.Prepared prepared, Object[] values)
    {
        if (prepared.boundNames.size() != values.length)
            throw new IllegalArgumentException(String.format("Invalid number of values. Expecting %d but got %d", prepared.boundNames.size(), values.length));

        List<ByteBuffer> boundValues = new ArrayList<ByteBuffer>(values.length);
        for (int i = 0; i < values.length; i++)
        {
            Object value = values[i];
            AbstractType type = prepared.boundNames.get(i).type;
            boundValues.add(value instanceof ByteBuffer || value == null ? (ByteBuffer)value : type.decompose(value));
        }
        return QueryOptions.forInternalCalls(boundValues);
    }

    private static ParsedStatement.Prepared prepareInternal(String query) throws RequestValidationException
    {
        ParsedStatement.Prepared prepared = internalStatements.get(query);
        if (prepared != null)
            return prepared;

        // Note: if 2 threads prepare the same query, we'll live so don't bother synchronizing
        prepared = parseStatement(query, internalQueryState());
        prepared.statement.validate(internalQueryState().getClientState());
        internalStatements.putIfAbsent(query, prepared);
        return prepared;
    }
#endif
private:
    ::shared_ptr<statements::parsed_statement::prepared> prepare_internal(const std::experimental::string_view& query);
    query_options make_internal_options(::shared_ptr<statements::parsed_statement::prepared>, const std::initializer_list<boost::any>&);

public:
    future<::shared_ptr<untyped_result_set>> execute_internal(
            const std::experimental::string_view& query_string,
            const std::initializer_list<boost::any>& = { });

    /*
     * This function provides a timestamp that is guaranteed to be higher than any timestamp
     * previously used in internal queries.
     *
     * This is useful because the client_state have a built-in mechanism to guarantee monotonicity.
     * Bypassing that mechanism by the use of some other clock may yield times in the past, even if the operation
     * was done in the future.
     */
    api::timestamp_type next_timestamp();

#if 0
    public static UntypedResultSet executeInternalWithPaging(String query, int pageSize, Object... values)
    {
        try
        {
            ParsedStatement.Prepared prepared = prepareInternal(query);
            if (!(prepared.statement instanceof SelectStatement))
                throw new IllegalArgumentException("Only SELECTs can be paged");

            SelectStatement select = (SelectStatement)prepared.statement;
            QueryPager pager = QueryPagers.localPager(select.getPageableCommand(makeInternalOptions(prepared, values)));
            return UntypedResultSet.create(select, pager, pageSize);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating query" + e);
        }
    }

    /**
     * Same than executeInternal, but to use for queries we know are only executed once so that the
     * created statement object is not cached.
     */
    public static UntypedResultSet executeOnceInternal(String query, Object... values)
    {
        try
        {
            ParsedStatement.Prepared prepared = parseStatement(query, internalQueryState());
            prepared.statement.validate(internalQueryState().getClientState());
            ResultMessage result = prepared.statement.executeInternal(internalQueryState(), makeInternalOptions(prepared, values));
            if (result instanceof ResultMessage.Rows)
                return UntypedResultSet.create(((ResultMessage.Rows)result).result);
            else
                return null;
        }
        catch (RequestExecutionException e)
        {
            throw new RuntimeException(e);
        }
        catch (RequestValidationException e)
        {
            throw new RuntimeException("Error validating query " + query, e);
        }
    }

    public static UntypedResultSet resultify(String query, Row row)
    {
        return resultify(query, Collections.singletonList(row));
    }

    public static UntypedResultSet resultify(String query, List<Row> rows)
    {
        try
        {
            SelectStatement ss = (SelectStatement) getStatement(query, null).statement;
            ResultSet cqlRows = ss.process(rows);
            return UntypedResultSet.create(cqlRows);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e);
        }
    }
#endif

    future<::shared_ptr<transport::messages::result_message::prepared>>
    prepare(const std::experimental::string_view& query_string, service::query_state& query_state);

    future<::shared_ptr<transport::messages::result_message::prepared>>
    prepare(const std::experimental::string_view& query_string, const service::client_state& client_state, bool for_thrift);

    static bytes compute_id(const std::experimental::string_view& query_string, const sstring& keyspace);

#if 0
    private static Integer computeThriftId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return toHash.hashCode();
    }
#endif
private:
    ::shared_ptr<transport::messages::result_message::prepared>
    get_stored_prepared_statement(const std::experimental::string_view& query_string, const sstring& keyspace, bool for_thrift);

    future<::shared_ptr<transport::messages::result_message::prepared>>
    store_prepared_statement(const std::experimental::string_view& query_string, const sstring& keyspace, ::shared_ptr<statements::parsed_statement::prepared> prepared, bool for_thrift);

    void invalidate_prepared_statement(bytes statement_id);

#if 0
    public ResultMessage processPrepared(CQLStatement statement, QueryState queryState, QueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        List<ByteBuffer> variables = options.getValues();
        // Check to see if there are any bound variables to verify
        if (!(variables.isEmpty() && (statement.getBoundTerms() == 0)))
        {
            if (variables.size() != statement.getBoundTerms())
                throw new InvalidRequestException(String.format("there were %d markers(?) in CQL but %d bound variables",
                                                                statement.getBoundTerms(),
                                                                variables.size()));

            // at this point there is a match in count between markers and variables that is non-zero

            if (logger.isTraceEnabled())
                for (int i = 0; i < variables.size(); i++)
                    logger.trace("[{}] '{}'", i+1, variables.get(i));
        }

        metrics.preparedStatementsExecuted.inc();
        return processStatement(statement, queryState, options);
    }
#endif

public:
    future<::shared_ptr<transport::messages::result_message>> process_batch(::shared_ptr<statements::batch_statement>,
            service::query_state& query_state, query_options& options);

    ::shared_ptr<statements::parsed_statement::prepared> get_statement(const std::experimental::string_view& query,
            const service::client_state& client_state);
    static ::shared_ptr<statements::parsed_statement> parse_statement(const std::experimental::string_view& query);

#if 0
    private static long measure(Object key)
    {
        return meter.measureDeep(key);
    }
#endif
public:
    future<> stop() {
        return make_ready_future<>();
    }
    class migration_subscriber;

    friend class migration_subscriber;
};

class query_processor::migration_subscriber : public service::migration_listener {
    query_processor* _qp;
public:
    migration_subscriber(query_processor* qp);

    virtual void on_create_keyspace(const sstring& ks_name) override;
    virtual void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_create_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_create_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_update_keyspace(const sstring& ks_name) override;
    virtual void on_update_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_update_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_update_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;

    virtual void on_drop_keyspace(const sstring& ks_name) override;
    virtual void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override;
    virtual void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override;
    virtual void on_drop_function(const sstring& ks_name, const sstring& function_name) override;
    virtual void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override;
private:
    void remove_invalid_prepared_statements(sstring ks_name, std::experimental::optional<sstring> cf_name);
    bool should_invalidate(sstring ks_name, std::experimental::optional<sstring> cf_name, ::shared_ptr<cql_statement> statement);
};

extern distributed<query_processor> _the_query_processor;

inline distributed<query_processor>& get_query_processor() {
    return _the_query_processor;
}

inline query_processor& get_local_query_processor() {
    return _the_query_processor.local();
}

}
