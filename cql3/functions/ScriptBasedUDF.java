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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class ScriptBasedUDF extends UDFunction
{
    static final Map<String, Compilable> scriptEngines = new HashMap<>();

    static {
        ScriptEngineManager scriptEngineManager = new ScriptEngineManager();
        for (ScriptEngineFactory scriptEngineFactory : scriptEngineManager.getEngineFactories())
        {
            ScriptEngine scriptEngine = scriptEngineFactory.getScriptEngine();
            boolean compilable = scriptEngine instanceof Compilable;
            if (compilable)
            {
                logger.info("Found scripting engine {} {} - {} {} - language names: {}",
                            scriptEngineFactory.getEngineName(), scriptEngineFactory.getEngineVersion(),
                            scriptEngineFactory.getLanguageName(), scriptEngineFactory.getLanguageVersion(),
                            scriptEngineFactory.getNames());
                for (String name : scriptEngineFactory.getNames())
                    scriptEngines.put(name, (Compilable) scriptEngine);
            }
        }
    }

    private final CompiledScript script;

    ScriptBasedUDF(FunctionName name,
                   List<ColumnIdentifier> argNames,
                   List<AbstractType<?>> argTypes,
                   AbstractType<?> returnType,
                   String language,
                   String body,
                   boolean deterministic)
    throws InvalidRequestException
    {
        super(name, argNames, argTypes, returnType, language, body, deterministic);

        Compilable scriptEngine = scriptEngines.get(language);
        if (scriptEngine == null)
            throw new InvalidRequestException(String.format("Invalid language '%s' for function '%s'", language, name));

        try
        {
            this.script = scriptEngine.compile(body);
        }
        catch (RuntimeException | ScriptException e)
        {
            logger.info("Failed to compile function '{}' for language {}: ", name, language, e);
            throw new InvalidRequestException(
                    String.format("Failed to compile function '%s' for language %s: %s", name, language, e));
        }
    }

    public ByteBuffer execute(int protocolVersion, List<ByteBuffer> parameters) throws InvalidRequestException
    {
        Object[] params = new Object[argTypes.size()];
        for (int i = 0; i < params.length; i++)
            params[i] = compose(protocolVersion, i, parameters.get(i));

        try
        {
            Bindings bindings = new SimpleBindings();
            for (int i = 0; i < params.length; i++)
                bindings.put(argNames.get(i).toString(), params[i]);

            Object result = script.eval(bindings);
            if (result == null)
                return null;

            Class<?> javaReturnType = returnDataType.asJavaClass();
            Class<?> resultType = result.getClass();
            if (!javaReturnType.isAssignableFrom(resultType))
            {
                if (result instanceof Number)
                {
                    Number rNumber = (Number) result;
                    if (javaReturnType == Integer.class)
                        result = rNumber.intValue();
                    else if (javaReturnType == Long.class)
                        result = rNumber.longValue();
                    else if (javaReturnType == Float.class)
                        result = rNumber.floatValue();
                    else if (javaReturnType == Double.class)
                        result = rNumber.doubleValue();
                    else if (javaReturnType == BigInteger.class)
                    {
                        if (rNumber instanceof BigDecimal)
                            result = ((BigDecimal)rNumber).toBigInteger();
                        else if (rNumber instanceof Double || rNumber instanceof Float)
                            result = new BigDecimal(rNumber.toString()).toBigInteger();
                        else
                            result = BigInteger.valueOf(rNumber.longValue());
                    }
                    else if (javaReturnType == BigDecimal.class)
                        // String c'tor of BigDecimal is more accurate than valueOf(double)
                        result = new BigDecimal(rNumber.toString());
                }
            }

            return decompose(protocolVersion, result);
        }
        catch (RuntimeException | ScriptException e)
        {
            logger.info("Execution of UDF '{}' failed", name, e);
            throw new InvalidRequestException("Execution of user-defined function '" + name + "' failed: " + e);
        }
    }
}
