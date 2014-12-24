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
package org.apache.cassandra.cql3.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.IMigrationListener;
import org.apache.cassandra.service.MigrationManager;

public abstract class Functions
{
    // We special case the token function because that's the only function whose argument types actually
    // depend on the table on which the function is called. Because it's the sole exception, it's easier
    // to handle it as a special case.
    private static final FunctionName TOKEN_FUNCTION_NAME = FunctionName.nativeFunction("token");

    private Functions() {}

    private static final ArrayListMultimap<FunctionName, Function> declared = ArrayListMultimap.create();

    static
    {
        declare(AggregateFcts.countRowsFunction);
        declare(TimeuuidFcts.nowFct);
        declare(TimeuuidFcts.minTimeuuidFct);
        declare(TimeuuidFcts.maxTimeuuidFct);
        declare(TimeuuidFcts.dateOfFct);
        declare(TimeuuidFcts.unixTimestampOfFct);
        declare(UuidFcts.uuidFct);

        for (CQL3Type type : CQL3Type.Native.values())
        {
            // Note: because text and varchar ends up being synonimous, our automatic makeToBlobFunction doesn't work
            // for varchar, so we special case it below. We also skip blob for obvious reasons.
            if (type == CQL3Type.Native.VARCHAR || type == CQL3Type.Native.BLOB)
                continue;

            declare(BytesConversionFcts.makeToBlobFunction(type.getType()));
            declare(BytesConversionFcts.makeFromBlobFunction(type.getType()));

            declare(AggregateFcts.makeCountFunction(type.getType()));
            declare(AggregateFcts.makeMaxFunction(type.getType()));
            declare(AggregateFcts.makeMinFunction(type.getType()));
        }
        declare(BytesConversionFcts.VarcharAsBlobFct);
        declare(BytesConversionFcts.BlobAsVarcharFact);
        declare(AggregateFcts.sumFunctionForInt32);
        declare(AggregateFcts.sumFunctionForLong);
        declare(AggregateFcts.sumFunctionForFloat);
        declare(AggregateFcts.sumFunctionForDouble);
        declare(AggregateFcts.sumFunctionForDecimal);
        declare(AggregateFcts.sumFunctionForVarint);
        declare(AggregateFcts.avgFunctionForInt32);
        declare(AggregateFcts.avgFunctionForLong);
        declare(AggregateFcts.avgFunctionForFloat);
        declare(AggregateFcts.avgFunctionForDouble);
        declare(AggregateFcts.avgFunctionForVarint);
        declare(AggregateFcts.avgFunctionForDecimal);

        MigrationManager.instance.register(new FunctionsMigrationListener());
    }

    private static void declare(Function fun)
    {
        declared.put(fun.name(), fun);
    }

    public static ColumnSpecification makeArgSpec(String receiverKs, String receiverCf, Function fun, int i)
    {
        return new ColumnSpecification(receiverKs,
                                       receiverCf,
                                       new ColumnIdentifier("arg" + i +  "(" + fun.name().toString().toLowerCase() + ")", true),
                                       fun.argTypes().get(i));
    }

    public static int getOverloadCount(FunctionName name)
    {
        return declared.get(name).size();
    }

    public static Function get(String keyspace,
                               FunctionName name,
                               List<? extends AssignmentTestable> providedArgs,
                               String receiverKs,
                               String receiverCf)
    throws InvalidRequestException
    {
        if (name.hasKeyspace()
            ? name.equals(TOKEN_FUNCTION_NAME)
            : name.name.equals(TOKEN_FUNCTION_NAME.name))
            return new TokenFct(Schema.instance.getCFMetaData(receiverKs, receiverCf));

        List<Function> candidates;
        if (!name.hasKeyspace())
        {
            // function name not fully qualified
            candidates = new ArrayList<>();
            // add 'SYSTEM' (native) candidates
            candidates.addAll(declared.get(name.asNativeFunction()));
            // add 'current keyspace' candidates
            candidates.addAll(declared.get(new FunctionName(keyspace, name.name)));
        }
        else
            // function name is fully qualified (keyspace + name)
            candidates = declared.get(name);

        if (candidates.isEmpty())
            return null;

        // Fast path if there is only one choice
        if (candidates.size() == 1)
        {
            Function fun = candidates.get(0);
            validateTypes(keyspace, fun, providedArgs, receiverKs, receiverCf);
            return fun;
        }

        List<Function> compatibles = null;
        for (Function toTest : candidates)
        {
            AssignmentTestable.TestResult r = matchAguments(keyspace, toTest, providedArgs, receiverKs, receiverCf);
            switch (r)
            {
                case EXACT_MATCH:
                    // We always favor exact matches
                    return toTest;
                case WEAKLY_ASSIGNABLE:
                    if (compatibles == null)
                        compatibles = new ArrayList<>();
                    compatibles.add(toTest);
                    break;
            }
        }

        if (compatibles == null || compatibles.isEmpty())
            throw new InvalidRequestException(String.format("Invalid call to function %s, none of its type signatures match (known type signatures: %s)",
                                                            name, toString(candidates)));

        if (compatibles.size() > 1)
            throw new InvalidRequestException(String.format("Ambiguous call to function %s (can be matched by following signatures: %s): use type casts to disambiguate",
                        name, toString(compatibles)));

        return compatibles.get(0);
    }

    public static List<Function> find(FunctionName name)
    {
        return declared.get(name);
    }

    public static Function find(FunctionName name, List<AbstractType<?>> argTypes)
    {
        assert name.hasKeyspace() : "function name not fully qualified";
        for (Function f : declared.get(name))
        {
            if (typeEquals(f.argTypes(), argTypes))
                return f;
        }
        return null;
    }

    // This method and matchArguments are somewhat duplicate, but this method allows us to provide more precise errors in the common
    // case where there is no override for a given function. This is thus probably worth the minor code duplication.
    private static void validateTypes(String keyspace,
                                      Function fun,
                                      List<? extends AssignmentTestable> providedArgs,
                                      String receiverKs,
                                      String receiverCf)
    throws InvalidRequestException
    {
        if (providedArgs.size() != fun.argTypes().size())
            throw new InvalidRequestException(String.format("Invalid number of arguments in call to function %s: %d required but %d provided", fun.name(), fun.argTypes().size(), providedArgs.size()));

        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);

            // If the concrete argument is a bind variables, it can have any type.
            // We'll validate the actually provided value at execution time.
            if (provided == null)
                continue;

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            if (!provided.testAssignment(keyspace, expected).isAssignable())
                throw new InvalidRequestException(String.format("Type error: %s cannot be passed as argument %d of function %s of type %s", provided, i, fun.name(), expected.type.asCQL3Type()));
        }
    }

    private static AssignmentTestable.TestResult matchAguments(String keyspace,
                                                               Function fun,
                                                               List<? extends AssignmentTestable> providedArgs,
                                                               String receiverKs,
                                                               String receiverCf)
    {
        if (providedArgs.size() != fun.argTypes().size())
            return AssignmentTestable.TestResult.NOT_ASSIGNABLE;

        // It's an exact match if all are exact match, but is not assignable as soon as any is non assignable.
        AssignmentTestable.TestResult res = AssignmentTestable.TestResult.EXACT_MATCH;
        for (int i = 0; i < providedArgs.size(); i++)
        {
            AssignmentTestable provided = providedArgs.get(i);
            if (provided == null)
            {
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
                continue;
            }

            ColumnSpecification expected = makeArgSpec(receiverKs, receiverCf, fun, i);
            AssignmentTestable.TestResult argRes = provided.testAssignment(keyspace, expected);
            if (argRes == AssignmentTestable.TestResult.NOT_ASSIGNABLE)
                return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
            if (argRes == AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE)
                res = AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
        }
        return res;
    }

    private static String toString(List<Function> funs)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < funs.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(funs.get(i));
        }
        return sb.toString();
    }

    // This is *not* thread safe but is only called in SchemaTables that is synchronized.
    public static void addFunction(AbstractFunction fun)
    {
        // We shouldn't get there unless that function don't exist
        assert find(fun.name(), fun.argTypes()) == null;
        declare(fun);
    }

    // Same remarks than for addFunction
    public static void removeFunction(FunctionName name, List<AbstractType<?>> argsTypes)
    {
        Function old = find(name, argsTypes);
        assert old != null && !old.isNative();
        declared.remove(old.name(), old);
    }

    // Same remarks than for addFunction
    public static void replaceFunction(AbstractFunction fun)
    {
        removeFunction(fun.name(), fun.argTypes());
        addFunction(fun);
    }

    public static List<Function> getReferencesTo(Function old)
    {
        List<Function> references = new ArrayList<>();
        for (Function function : declared.values())
            if (function.hasReferenceTo(old))
                references.add(function);
        return references;
    }

    public static Collection<Function> all()
    {
        return declared.values();
    }

    public static boolean typeEquals(AbstractType<?> t1, AbstractType<?> t2)
    {
        return t1.asCQL3Type().toString().equals(t2.asCQL3Type().toString());
    }

    public static boolean typeEquals(List<AbstractType<?>> t1, List<AbstractType<?>> t2)
    {
        if (t1.size() != t2.size())
            return false;
        for (int i = 0; i < t1.size(); i ++)
            if (!typeEquals(t1.get(i), t2.get(i)))
                return false;
        return true;
    }

    private static class FunctionsMigrationListener implements IMigrationListener
    {
        public void onCreateKeyspace(String ksName) { }
        public void onCreateColumnFamily(String ksName, String cfName) { }
        public void onCreateUserType(String ksName, String typeName) { }
        public void onCreateFunction(String ksName, String functionName) { }
        public void onCreateAggregate(String ksName, String aggregateName) { }

        public void onUpdateKeyspace(String ksName) { }
        public void onUpdateColumnFamily(String ksName, String cfName) { }
        public void onUpdateUserType(String ksName, String typeName) {
            for (Function function : all())
                if (function instanceof UDFunction)
                    ((UDFunction)function).userTypeUpdated(ksName, typeName);
        }
        public void onUpdateFunction(String ksName, String functionName) { }
        public void onUpdateAggregate(String ksName, String aggregateName) { }

        public void onDropKeyspace(String ksName) { }
        public void onDropColumnFamily(String ksName, String cfName) { }
        public void onDropUserType(String ksName, String typeName) { }
        public void onDropFunction(String ksName, String functionName) { }
        public void onDropAggregate(String ksName, String aggregateName) { }
    }
}
