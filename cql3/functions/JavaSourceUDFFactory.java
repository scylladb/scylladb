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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtNewConstructor;
import javassist.CtNewMethod;
import javassist.NotFoundException;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;

/**
 * Java source UDF code generation.
 */
public final class JavaSourceUDFFactory
{
    private static final String GENERATED_CODE_PACKAGE = "org.apache.cassandra.cql3.udf.gen.";

    protected static final Logger logger = LoggerFactory.getLogger(JavaSourceUDFFactory.class);

    private static final AtomicInteger classSequence = new AtomicInteger();

    static UDFunction buildUDF(FunctionName name,
                               List<ColumnIdentifier> argNames,
                               List<AbstractType<?>> argTypes,
                               AbstractType<?> returnType,
                               String body,
                               boolean deterministic)
    throws InvalidRequestException
    {
        // argDataTypes is just the C* internal argTypes converted to the Java Driver DataType
        DataType[] argDataTypes = UDHelper.driverTypes(argTypes);
        // returnDataType is just the C* internal returnType converted to the Java Driver DataType
        DataType returnDataType = UDHelper.driverType(returnType);
        // javaParamTypes is just the Java representation for argTypes resp. argDataTypes
        Class<?>[] javaParamTypes = UDHelper.javaTypes(argDataTypes);
        // javaReturnType is just the Java representation for returnType resp. returnDataType
        Class<?> javaReturnType = returnDataType.asJavaClass();

        String clsName = generateClassName(name);

        String codeCtor = generateConstructor(clsName);

        // Generate 'execute' method (implements org.apache.cassandra.cql3.functions.Function.execute)
        String codeExec = generateExecuteMethod(argNames, javaParamTypes);

        // Generate the 'executeInternal' method
        // It is separated to allow return type and argument type checks during compile time via javassist.
        String codeExecInt = generateExecuteInternalMethod(argNames, body, javaReturnType, javaParamTypes);

        logger.debug("Generating java source UDF for {} with following c'tor and functions:\n{}\n{}\n{}",
                     name, codeCtor, codeExecInt, codeExec);

        try
        {
            ClassPool classPool = ClassPool.getDefault();

            // get super class
            CtClass base = classPool.get(UDFunction.class.getName());

            // prepare class to generate
            CtClass cc = classPool.makeClass(GENERATED_CODE_PACKAGE + clsName, base);
            cc.setModifiers(cc.getModifiers() | Modifier.FINAL);

            // add c'tor plus methods (order matters)
            cc.addConstructor(CtNewConstructor.make(codeCtor, cc));
            cc.addMethod(CtNewMethod.make(codeExecInt, cc));
            cc.addMethod(CtNewMethod.make(codeExec, cc));

            Constructor ctor =
                cc.toClass().getDeclaredConstructor(
                   FunctionName.class, List.class, List.class, DataType[].class,
                   AbstractType.class, DataType.class,
                   String.class, boolean.class);
            return (UDFunction) ctor.newInstance(
                   name, argNames, argTypes, argDataTypes,
                   returnType, returnDataType,
                   body, deterministic);
        }
        catch (NotFoundException | CannotCompileException | NoSuchMethodException | LinkageError | InstantiationException | IllegalAccessException e)
        {
            throw new InvalidRequestException(String.format("Could not compile function '%s' from Java source: %s", name, e));
        }
        catch (InvocationTargetException e)
        {
            // in case of an ITE, use the cause
            throw new InvalidRequestException(String.format("Could not compile function '%s' from Java source: %s", name, e.getCause()));
        }
    }

    private static String generateClassName(FunctionName name)
    {
        String qualifiedName = name.toString();

        StringBuilder sb = new StringBuilder(qualifiedName.length()+10);
        sb.append('C');
        for (int i = 0; i < qualifiedName.length(); i++)
        {
            char c = qualifiedName.charAt(i);
            if (Character.isJavaIdentifierPart(c))
                sb.append(c);
        }
        sb.append('_');
        sb.append(classSequence.incrementAndGet());
        return sb.toString();
    }

    /**
     * Generates constructor with just a call super class (UDFunction) constructor with constant 'java' as language.
     */
    private static String generateConstructor(String clsName)
    {
        return "public " + clsName +
               "(org.apache.cassandra.cql3.functions.FunctionName name, " +
               "java.util.List argNames, " +
               "java.util.List argTypes, " +
               "com.datastax.driver.core.DataType[] argDataTypes, " +
               "org.apache.cassandra.db.marshal.AbstractType returnType, " +
               "com.datastax.driver.core.DataType returnDataType, " +
               "String body," +
               "boolean deterministic)\n{" +
               "  super(name, argNames, argTypes, argDataTypes, returnType, returnDataType, \"java\", body, deterministic);\n" +
               "}";
    }

    /**
     * Generate executeInternal method (just there to find return and argument type mismatches in UDF body).
     *
     * Generated looks like this:
     * <code><pre>
     * private <JAVA_RETURN_TYPE> executeInternal(<JAVA_ARG_TYPE> paramOne, <JAVA_ARG_TYPE> nextParam)
     * {
     *     <UDF_BODY>
     * }
     * </pre></code>
     */
    private static String generateExecuteInternalMethod(List<ColumnIdentifier> argNames, String body, Class<?> returnType, Class<?>[] paramTypes)
    {
        // initial builder size can just be a guess (prevent temp object allocations)
        StringBuilder codeInt = new StringBuilder(64 + 32*paramTypes.length + body.length());
        codeInt.append("private ").append(returnType.getName()).append(" executeInternal(");
        for (int i = 0; i < paramTypes.length; i++)
        {
            if (i > 0)
                codeInt.append(", ");
            codeInt.append(paramTypes[i].getName()).
                    append(' ').
                    append(argNames.get(i));
        }
        codeInt.append(")\n{").
                append(body).
                append('}');
        return codeInt.toString();
    }

    /**
     *
     * Generate public execute() method implementation.
     *
     * Generated looks like this:
     * <code><pre>
     *
     * public java.nio.ByteBuffer execute(int protocolVersion, java.util.List params)
     * throws org.apache.cassandra.exceptions.InvalidRequestException
     * {
     *     try
     *     {
     *         Object result = executeInternal(
     *             (<cast to JAVA_ARG_TYPE>)compose(protocolVersion, 0, (java.nio.ByteBuffer)params.get(0)),
     *             (<cast to JAVA_ARG_TYPE>)compose(protocolVersion, 1, (java.nio.ByteBuffer)params.get(1)),
     *             ...
     *         );
     *         return decompose(protocolVersion, result);
     *     }
     *     catch (Throwable t)
     *     {
     *         logger.error("Invocation of function '{}' failed", this, t);
     *         if (t instanceof VirtualMachineError)
     *             throw (VirtualMachineError)t;
     *         throw new org.apache.cassandra.exceptions.InvalidRequestException("Invocation of function '" + this + "' failed: " + t);
     *     }
     * }
     * </pre></code>
     */
    private static String generateExecuteMethod(List<ColumnIdentifier> argNames, Class<?>[] paramTypes)
    {
        // usual methods are 700-800 chars long (prevent temp object allocations)
        StringBuilder code = new StringBuilder(1024);
        // overrides org.apache.cassandra.cql3.functions.Function.execute(java.util.List)
        code.append("public java.nio.ByteBuffer execute(int protocolVersion, java.util.List params)\n" +
                    "throws org.apache.cassandra.exceptions.InvalidRequestException\n" +
                    "{\n" +
                    "  try\n" +
                    "  {\n" +
                    "    Object result = executeInternal(");
        for (int i = 0; i < paramTypes.length; i++)
        {
            if (i > 0)
                code.append(',');

            if (logger.isDebugEnabled())
                code.append("\n      /* ").append(argNames.get(i)).append(" */ ");

            code.
                 // cast to Java type
                 append("\n      (").append(paramTypes[i].getName()).append(")").
                 // generate object representation of input parameter (call UDFunction.compose)
                 append("compose(protocolVersion, ").append(i).append(", (java.nio.ByteBuffer)params.get(").append(i).append("))");
        }

        code.append("\n    );\n" +
                    // generate serialized return value (returnType is a field in AbstractFunction class), (call UDFunction.decompose)
                    "    return decompose(protocolVersion, result);\n" +
                    //
                    // error handling ...
                    "  }\n" +
                    "  catch (Throwable t)\n" +
                    "  {\n" +
                    "    logger.error(\"Invocation of function '{}' failed\", this, t);\n" +
                    // handle OutOfMemoryError and other fatals not here!
                    "    if (t instanceof VirtualMachineError)\n" +
                    "      throw (VirtualMachineError)t;\n" +
                    "    throw new org.apache.cassandra.exceptions.InvalidRequestException(\"Invocation of function '\" + this + \"' failed: \" + t);\n" +
                    "  }\n" +
                    "}");

        return code.toString();
    }

}
