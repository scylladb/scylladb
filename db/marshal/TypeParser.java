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
package org.apache.cassandra.db.marshal;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

/**
 * Parse a string containing an Type definition.
 */
public class TypeParser
{
    private final String str;
    private int idx;

    // A cache of parsed string, specially useful for DynamicCompositeType
    private static final Map<String, AbstractType<?>> cache = new HashMap<String, AbstractType<?>>();

    public static final TypeParser EMPTY_PARSER = new TypeParser("", 0);

    private TypeParser(String str, int idx)
    {
        this.str = str;
        this.idx = idx;
    }

    public TypeParser(String str)
    {
        this(str, 0);
    }

    /**
     * Parse a string containing an type definition.
     */
    public static AbstractType<?> parse(String str) throws SyntaxException, ConfigurationException
    {
        if (str == null)
            return BytesType.instance;

        AbstractType<?> type = cache.get(str);

        if (type != null)
            return type;

        // This could be simplier (i.e. new TypeParser(str).parse()) but we avoid creating a TypeParser object if not really necessary.
        int i = 0;
        i = skipBlank(str, i);
        int j = i;
        while (!isEOS(str, i) && isIdentifierChar(str.charAt(i)))
            ++i;

        if (i == j)
            return BytesType.instance;

        String name = str.substring(j, i);
        i = skipBlank(str, i);

        if (!isEOS(str, i) && str.charAt(i) == '(')
            type = getAbstractType(name, new TypeParser(str, i));
        else
            type = getAbstractType(name);

        // We don't really care about concurrency here. Worst case scenario, we do some parsing unnecessarily
        cache.put(str, type);
        return type;
    }

    public static AbstractType<?> parse(CharSequence compareWith) throws SyntaxException, ConfigurationException
    {
        return parse(compareWith == null ? null : compareWith.toString());
    }

    public static String getShortName(AbstractType<?> type)
    {
        return type.getClass().getSimpleName();
    }

    /**
     * Parse an AbstractType from current position of this parser.
     */
    public AbstractType<?> parse() throws SyntaxException, ConfigurationException
    {
        skipBlank();
        String name = readNextIdentifier();

        skipBlank();
        if (!isEOS() && str.charAt(idx) == '(')
            return getAbstractType(name, this);
        else
            return getAbstractType(name);
    }

    public Map<String, String> getKeyValueParameters() throws SyntaxException
    {
        if (isEOS())
            return Collections.emptyMap();

        if (str.charAt(idx) != '(')
            throw new IllegalStateException();

        Map<String, String> map = new HashMap<String, String>();
        ++idx; // skipping '('

        while (skipBlankAndComma())
        {
            if (str.charAt(idx) == ')')
            {
                ++idx;
                return map;
            }

            String k = readNextIdentifier();
            String v = "";
            skipBlank();
            if (str.charAt(idx) == '=')
            {
                ++idx;
                skipBlank();
                v = readNextIdentifier();
            }
            else if (str.charAt(idx) != ',' && str.charAt(idx) != ')')
            {
                throwSyntaxError("unexpected character '" + str.charAt(idx) + "'");
            }
            map.put(k, v);
        }
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    public List<AbstractType<?>> getTypeParameters() throws SyntaxException, ConfigurationException
    {
        List<AbstractType<?>> list = new ArrayList<AbstractType<?>>();

        if (isEOS())
            return list;

        if (str.charAt(idx) != '(')
            throw new IllegalStateException();

        ++idx; // skipping '('

        while (skipBlankAndComma())
        {
            if (str.charAt(idx) == ')')
            {
                ++idx;
                return list;
            }

            try
            {
                list.add(parse());
            }
            catch (SyntaxException e)
            {
                SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", str, idx));
                ex.initCause(e);
                throw ex;
            }
        }
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    public Map<Byte, AbstractType<?>> getAliasParameters() throws SyntaxException, ConfigurationException
    {
        Map<Byte, AbstractType<?>> map = new HashMap<Byte, AbstractType<?>>();

        if (isEOS())
            return map;

        if (str.charAt(idx) != '(')
            throw new IllegalStateException();

        ++idx; // skipping '('


        while (skipBlankAndComma())
        {
            if (str.charAt(idx) == ')')
            {
                ++idx;
                return map;
            }

            String alias = readNextIdentifier();
            if (alias.length() != 1)
                throwSyntaxError("An alias should be a single character");
            char aliasChar = alias.charAt(0);
            if (aliasChar < 33 || aliasChar > 127)
                throwSyntaxError("An alias should be a single character in [0..9a..bA..B-+._&]");

            skipBlank();
            if (!(str.charAt(idx) == '=' && str.charAt(idx+1) == '>'))
                throwSyntaxError("expecting '=>' token");

            idx += 2;
            skipBlank();
            try
            {
                map.put((byte)aliasChar, parse());
            }
            catch (SyntaxException e)
            {
                SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", str, idx));
                ex.initCause(e);
                throw ex;
            }
        }
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    public Map<ByteBuffer, CollectionType> getCollectionsParameters() throws SyntaxException, ConfigurationException
    {
        Map<ByteBuffer, CollectionType> map = new HashMap<>();

        if (isEOS())
            return map;

        if (str.charAt(idx) != '(')
            throw new IllegalStateException();

        ++idx; // skipping '('

        while (skipBlankAndComma())
        {
            if (str.charAt(idx) == ')')
            {
                ++idx;
                return map;
            }

            ByteBuffer bb = fromHex(readNextIdentifier());

            skipBlank();
            if (str.charAt(idx) != ':')
                throwSyntaxError("expecting ':' token");

            ++idx;
            skipBlank();
            try
            {
                AbstractType<?> type = parse();
                if (!(type instanceof CollectionType))
                    throw new SyntaxException(type + " is not a collection type");
                map.put(bb, (CollectionType)type);
            }
            catch (SyntaxException e)
            {
                SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", str, idx));
                ex.initCause(e);
                throw ex;
            }
        }
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    private ByteBuffer fromHex(String hex) throws SyntaxException
    {
        try
        {
            return ByteBufferUtil.hexToBytes(hex);
        }
        catch (NumberFormatException e)
        {
            throwSyntaxError(e.getMessage());
            return null;
        }
    }

    public Pair<Pair<String, ByteBuffer>, List<Pair<ByteBuffer, AbstractType>>> getUserTypeParameters() throws SyntaxException, ConfigurationException
    {

        if (isEOS() || str.charAt(idx) != '(')
            throw new IllegalStateException();

        ++idx; // skipping '('

        skipBlankAndComma();
        String keyspace = readNextIdentifier();
        skipBlankAndComma();
        ByteBuffer typeName = fromHex(readNextIdentifier());
        List<Pair<ByteBuffer, AbstractType>> defs = new ArrayList<>();

        while (skipBlankAndComma())
        {
            if (str.charAt(idx) == ')')
            {
                ++idx;
                return Pair.create(Pair.create(keyspace, typeName), defs);
            }

            ByteBuffer name = fromHex(readNextIdentifier());
            skipBlank();
            if (str.charAt(idx) != ':')
                throwSyntaxError("expecting ':' token");
            ++idx;
            skipBlank();
            try
            {
                AbstractType type = parse();
                defs.add(Pair.create(name, type));
            }
            catch (SyntaxException e)
            {
                SyntaxException ex = new SyntaxException(String.format("Exception while parsing '%s' around char %d", str, idx));
                ex.initCause(e);
                throw ex;
            }
        }
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: unexpected end of string", str, idx));
    }

    private static AbstractType<?> getAbstractType(String compareWith) throws ConfigurationException
    {
        String className = compareWith.contains(".") ? compareWith : "org.apache.cassandra.db.marshal." + compareWith;
        Class<? extends AbstractType<?>> typeClass = FBUtilities.<AbstractType<?>>classForName(className, "abstract-type");
        try
        {
            Field field = typeClass.getDeclaredField("instance");
            return (AbstractType<?>) field.get(null);
        }
        catch (NoSuchFieldException e)
        {
            // Trying with empty parser
            return getRawAbstractType(typeClass, EMPTY_PARSER);
        }
        catch (IllegalAccessException e)
        {
            // Trying with empty parser
            return getRawAbstractType(typeClass, EMPTY_PARSER);
        }
    }

    private static AbstractType<?> getAbstractType(String compareWith, TypeParser parser) throws SyntaxException, ConfigurationException
    {
        String className = compareWith.contains(".") ? compareWith : "org.apache.cassandra.db.marshal." + compareWith;
        Class<? extends AbstractType<?>> typeClass = FBUtilities.<AbstractType<?>>classForName(className, "abstract-type");
        try
        {
            Method method = typeClass.getDeclaredMethod("getInstance", TypeParser.class);
            return (AbstractType<?>) method.invoke(null, parser);
        }
        catch (NoSuchMethodException e)
        {
            // Trying to see if we have an instance field and apply the default parameter to it
            AbstractType<?> type = getRawAbstractType(typeClass);
            return AbstractType.parseDefaultParameters(type, parser);
        }
        catch (IllegalAccessException e)
        {
            // Trying to see if we have an instance field and apply the default parameter to it
            AbstractType<?> type = getRawAbstractType(typeClass);
            return AbstractType.parseDefaultParameters(type, parser);
        }
        catch (InvocationTargetException e)
        {
            ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
            ex.initCause(e.getTargetException());
            throw ex;
        }
    }

    private static AbstractType<?> getRawAbstractType(Class<? extends AbstractType<?>> typeClass) throws ConfigurationException
    {
        try
        {
            Field field = typeClass.getDeclaredField("instance");
            return (AbstractType<?>) field.get(null);
        }
        catch (NoSuchFieldException e)
        {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        }
    }

    private static AbstractType<?> getRawAbstractType(Class<? extends AbstractType<?>> typeClass, TypeParser parser) throws ConfigurationException
    {
        try
        {
            Method method = typeClass.getDeclaredMethod("getInstance", TypeParser.class);
            return (AbstractType<?>) method.invoke(null, parser);
        }
        catch (NoSuchMethodException e)
        {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        }
        catch (IllegalAccessException e)
        {
            throw new ConfigurationException("Invalid comparator class " + typeClass.getName() + ": must define a public static instance field or a public static method getInstance(TypeParser).");
        }
        catch (InvocationTargetException e)
        {
            ConfigurationException ex = new ConfigurationException("Invalid definition for comparator " + typeClass.getName() + ".");
            ex.initCause(e.getTargetException());
            throw ex;
        }
    }

    private void throwSyntaxError(String msg) throws SyntaxException
    {
        throw new SyntaxException(String.format("Syntax error parsing '%s' at char %d: %s", str, idx, msg));
    }

    private boolean isEOS()
    {
        return isEOS(str, idx);
    }

    private static boolean isEOS(String str, int i)
    {
        return i >= str.length();
    }

    private static boolean isBlank(int c)
    {
        return c == ' ' || c == '\t' || c == '\n';
    }

    private void skipBlank()
    {
        idx = skipBlank(str, idx);
    }

    private static int skipBlank(String str, int i)
    {
        while (!isEOS(str, i) && isBlank(str.charAt(i)))
            ++i;

        return i;
    }

    // skip all blank and at best one comma, return true if there not EOS
    private boolean skipBlankAndComma()
    {
        boolean commaFound = false;
        while (!isEOS())
        {
            int c = str.charAt(idx);
            if (c == ',')
            {
                if (commaFound)
                    return true;
                else
                    commaFound = true;
            }
            else if (!isBlank(c))
            {
                return true;
            }
            ++idx;
        }
        return false;
    }

    /*
     * [0..9a..bA..B-+._&]
     */
    private static boolean isIdentifierChar(int c)
    {
        return (c >= '0' && c <= '9')
            || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
            || c == '-' || c == '+' || c == '.' || c == '_' || c == '&';
    }

    // left idx positioned on the character stopping the read
    public String readNextIdentifier()
    {
        int i = idx;
        while (!isEOS() && isIdentifierChar(str.charAt(idx)))
            ++idx;

        return str.substring(i, idx);
    }

    public char readNextChar()
    {
        skipBlank();
        return str.charAt(idx++);
    }

    /**
     * Helper function to ease the writing of AbstractType.toString() methods.
     */
    public static String stringifyAliasesParameters(Map<Byte, AbstractType<?>> aliases)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        Iterator<Map.Entry<Byte, AbstractType<?>>> iter = aliases.entrySet().iterator();
        if (iter.hasNext())
        {
            Map.Entry<Byte, AbstractType<?>> entry = iter.next();
            sb.append((char)(byte)entry.getKey()).append("=>").append(entry.getValue());
        }
        while (iter.hasNext())
        {
            Map.Entry<Byte, AbstractType<?>> entry = iter.next();
            sb.append(',').append((char)(byte)entry.getKey()).append("=>").append(entry.getValue());
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * Helper function to ease the writing of AbstractType.toString() methods.
     */
    public static String stringifyTypeParameters(List<AbstractType<?>> types)
    {
        return stringifyTypeParameters(types, false);
    }

    /**
     * Helper function to ease the writing of AbstractType.toString() methods.
     */
    public static String stringifyTypeParameters(List<AbstractType<?>> types, boolean ignoreFreezing)
    {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < types.size(); i++)
        {
            if (i > 0)
                sb.append(",");
            sb.append(types.get(i).toString(ignoreFreezing));
        }
        return sb.append(')').toString();
    }

    public static String stringifyCollectionsParameters(Map<ByteBuffer, ? extends CollectionType> collections)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        boolean first = true;
        for (Map.Entry<ByteBuffer, ? extends CollectionType> entry : collections.entrySet())
        {
            if (!first)
                sb.append(',');

            first = false;
            sb.append(ByteBufferUtil.bytesToHex(entry.getKey())).append(":");
            sb.append(entry.getValue());
        }
        sb.append(')');
        return sb.toString();
    }

    public static String stringifyUserTypeParameters(String keysace, ByteBuffer typeName, List<ByteBuffer> columnNames, List<AbstractType<?>> columnTypes)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('(').append(keysace).append(",").append(ByteBufferUtil.bytesToHex(typeName));

        for (int i = 0; i < columnNames.size(); i++)
        {
            sb.append(',');
            sb.append(ByteBufferUtil.bytesToHex(columnNames.get(i))).append(":");
            // omit FrozenType(...) from fields because it is currently implicit
            sb.append(columnTypes.get(i).toString(true));
        }
        sb.append(')');
        return sb.toString();
    }
}
