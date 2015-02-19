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

#pragma once

#include <experimental/string_view>

#include "core/shared_ptr.hh"
#include "exceptions/exceptions.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/cf_statement.hh"
#include "service/query_state.hh"
#include "log.hh"
#include "core/distributed.hh"

namespace cql3 {

class query_processor {
private:
    service::storage_proxy& _proxy;
    distributed<database>& _db;
public:
    query_processor(service::storage_proxy& proxy, distributed<database>& db) : _proxy(proxy), _db(db) {}

#if 0
    public static final SemanticVersion CQL_VERSION = new SemanticVersion("3.2.0");

    public static final QueryProcessor instance = new QueryProcessor();

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

    private static final ConcurrentLinkedHashMap<MD5Digest, ParsedStatement.Prepared> preparedStatements;
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

    public ParsedStatement.Prepared getPrepared(MD5Digest id)
    {
        return preparedStatements.get(id);
    }

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

    public static UntypedResultSet executeInternal(String query, Object... values)
    {
        try
        {
            ParsedStatement.Prepared prepared = prepareInternal(query);
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
            throw new RuntimeException("Error validating " + query, e);
        }
    }

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

    public ResultMessage.Prepared prepare(String queryString, QueryState queryState)
    throws RequestValidationException
    {
        ClientState cState = queryState.getClientState();
        return prepare(queryString, cState, cState instanceof ThriftClientState);
    }

    public static ResultMessage.Prepared prepare(String queryString, ClientState clientState, boolean forThrift)
    throws RequestValidationException
    {
        ResultMessage.Prepared existing = getStoredPreparedStatement(queryString, clientState.getRawKeyspace(), forThrift);
        if (existing != null)
            return existing;

        ParsedStatement.Prepared prepared = getStatement(queryString, clientState);
        int boundTerms = prepared.statement.getBoundTerms();
        if (boundTerms > FBUtilities.MAX_UNSIGNED_SHORT)
            throw new InvalidRequestException(String.format("Too many markers(?). %d markers exceed the allowed maximum of %d", boundTerms, FBUtilities.MAX_UNSIGNED_SHORT));
        assert boundTerms == prepared.boundNames.size();

        return storePreparedStatement(queryString, clientState.getRawKeyspace(), prepared, forThrift);
    }

    private static MD5Digest computeId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return MD5Digest.compute(toHash);
    }

    private static Integer computeThriftId(String queryString, String keyspace)
    {
        String toHash = keyspace == null ? queryString : keyspace + queryString;
        return toHash.hashCode();
    }

    private static ResultMessage.Prepared getStoredPreparedStatement(String queryString, String keyspace, boolean forThrift)
    throws InvalidRequestException
    {
        if (forThrift)
        {
            Integer thriftStatementId = computeThriftId(queryString, keyspace);
            ParsedStatement.Prepared existing = thriftPreparedStatements.get(thriftStatementId);
            return existing == null ? null : ResultMessage.Prepared.forThrift(thriftStatementId, existing.boundNames);
        }
        else
        {
            MD5Digest statementId = computeId(queryString, keyspace);
            ParsedStatement.Prepared existing = preparedStatements.get(statementId);
            return existing == null ? null : new ResultMessage.Prepared(statementId, existing);
        }
    }

    private static ResultMessage.Prepared storePreparedStatement(String queryString, String keyspace, ParsedStatement.Prepared prepared, boolean forThrift)
    throws InvalidRequestException
    {
        // Concatenate the current keyspace so we don't mix prepared statements between keyspace (#5352).
        // (if the keyspace is null, queryString has to have a fully-qualified keyspace so it's fine.
        long statementSize = measure(prepared.statement);
        // don't execute the statement if it's bigger than the allowed threshold
        if (statementSize > MAX_CACHE_PREPARED_MEMORY)
            throw new InvalidRequestException(String.format("Prepared statement of size %d bytes is larger than allowed maximum of %d bytes.",
                                                            statementSize,
                                                            MAX_CACHE_PREPARED_MEMORY));
        if (forThrift)
        {
            Integer statementId = computeThriftId(queryString, keyspace);
            thriftPreparedStatements.put(statementId, prepared);
            return ResultMessage.Prepared.forThrift(statementId, prepared.boundNames);
        }
        else
        {
            MD5Digest statementId = computeId(queryString, keyspace);
            preparedStatements.put(statementId, prepared);
            return new ResultMessage.Prepared(statementId, prepared);
        }
    }

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

    public ResultMessage processBatch(BatchStatement batch, QueryState queryState, BatchQueryOptions options)
    throws RequestExecutionException, RequestValidationException
    {
        ClientState clientState = queryState.getClientState();
        batch.checkAccess(clientState);
        batch.validate();
        batch.validate(clientState);
        return batch.execute(queryState, options);
    }
#endif

public:
    std::unique_ptr<statements::parsed_statement::prepared> get_statement(const std::experimental::string_view& query,
            service::client_state& client_state);
    static ::shared_ptr<statements::parsed_statement> parse_statement(const std::experimental::string_view& query);

#if 0
    private static long measure(Object key)
    {
        return meter.measureDeep(key);
    }

    private static class MigrationSubscriber implements IMigrationListener
    {
        private void removeInvalidPreparedStatements(String ksName, String cfName)
        {
            removeInvalidPreparedStatements(preparedStatements.values().iterator(), ksName, cfName);
            removeInvalidPreparedStatements(thriftPreparedStatements.values().iterator(), ksName, cfName);
        }

        private void removeInvalidPreparedStatements(Iterator<ParsedStatement.Prepared> iterator, String ksName, String cfName)
        {
            while (iterator.hasNext())
            {
                if (shouldInvalidate(ksName, cfName, iterator.next().statement))
                    iterator.remove();
            }
        }

        private boolean shouldInvalidate(String ksName, String cfName, CQLStatement statement)
        {
            String statementKsName;
            String statementCfName;

            if (statement instanceof ModificationStatement)
            {
                ModificationStatement modificationStatement = ((ModificationStatement) statement);
                statementKsName = modificationStatement.keyspace();
                statementCfName = modificationStatement.columnFamily();
            }
            else if (statement instanceof SelectStatement)
            {
                SelectStatement selectStatement = ((SelectStatement) statement);
                statementKsName = selectStatement.keyspace();
                statementCfName = selectStatement.columnFamily();
            }
            else
            {
                return false;
            }

            return ksName.equals(statementKsName) && (cfName == null || cfName.equals(statementCfName));
        }

        public void onCreateKeyspace(String ksName) { }
        public void onCreateColumnFamily(String ksName, String cfName) { }
        public void onCreateUserType(String ksName, String typeName) { }
        public void onCreateFunction(String ksName, String functionName) {
            if (Functions.getOverloadCount(new FunctionName(ksName, functionName)) > 1)
            {
                // in case there are other overloads, we have to remove all overloads since argument type
                // matching may change (due to type casting)
                removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, functionName);
                removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, functionName);
            }
        }
        public void onCreateAggregate(String ksName, String aggregateName) {
            if (Functions.getOverloadCount(new FunctionName(ksName, aggregateName)) > 1)
            {
                // in case there are other overloads, we have to remove all overloads since argument type
                // matching may change (due to type casting)
                removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, aggregateName);
                removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, aggregateName);
            }
        }

        public void onUpdateKeyspace(String ksName) { }
        public void onUpdateColumnFamily(String ksName, String cfName) { }
        public void onUpdateUserType(String ksName, String typeName) { }
        public void onUpdateFunction(String ksName, String functionName) { }
        public void onUpdateAggregate(String ksName, String aggregateName) { }

        public void onDropKeyspace(String ksName)
        {
            removeInvalidPreparedStatements(ksName, null);
        }

        public void onDropColumnFamily(String ksName, String cfName)
        {
            removeInvalidPreparedStatements(ksName, cfName);
        }

        public void onDropUserType(String ksName, String typeName) { }
        public void onDropFunction(String ksName, String functionName) {
            removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, functionName);
            removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, functionName);
        }
        public void onDropAggregate(String ksName, String aggregateName)
        {
            removeInvalidPreparedStatementsForFunction(preparedStatements.values().iterator(), ksName, aggregateName);
            removeInvalidPreparedStatementsForFunction(thriftPreparedStatements.values().iterator(), ksName, aggregateName);
        }

        private void removeInvalidPreparedStatementsForFunction(Iterator<ParsedStatement.Prepared> iterator,
                                                                String ksName, String functionName)
        {
            while (iterator.hasNext())
                if (iterator.next().statement.usesFunction(ksName, functionName))
                    iterator.remove();
        }
    }
#endif
};

}
