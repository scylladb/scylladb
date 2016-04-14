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
 * Copyright (C) 2015 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include "types.hh"

#include "core/sstring.hh"

namespace db {

namespace marshal {

/**
 * Parse a string containing an Type definition.
 */
class type_parser {
    sstring _str;
    size_t _idx;

#if 0
    // A cache of parsed string, specially useful for DynamicCompositeType
    private static final Map<String, AbstractType<?>> cache = new HashMap<String, AbstractType<?>>();

    public static final TypeParser EMPTY_PARSER = new TypeParser("", 0);
#endif
    type_parser(sstring_view str, size_t idx);
public:
    explicit type_parser(sstring_view str);

    /**
     * Parse a string containing an type definition.
     */
    static data_type parse(const sstring& str);
    static data_type parse(sstring_view str);

#if 0
    public static AbstractType<?> parse(CharSequence compareWith) throws SyntaxException, ConfigurationException
    {
        return parse(compareWith == null ? null : compareWith.toString());
    }

    public static String getShortName(AbstractType<?> type)
    {
        return type.getClass().getSimpleName();
    }
#endif

    /**
     * Parse an AbstractType from current position of this parser.
     */
    data_type parse();

#if 0
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
#endif
    std::vector<data_type> get_type_parameters(bool multicell=true);
    std::tuple<sstring, bytes, std::vector<bytes>, std::vector<data_type>> get_user_type_parameters();
    data_type do_parse(bool multicell = true);

#if 0
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
#endif

    static data_type get_abstract_type(const sstring& compare_with);

    static data_type get_abstract_type(const sstring& compare_with, type_parser& parser, bool multicell = true);

#if 0
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
#endif

    bool is_eos() const;

    static bool is_eos(const sstring& str, size_t i);

    static bool is_blank(char c);

    void skip_blank();

    static size_t skip_blank(const sstring& str, size_t i);

    // skip all blank and at best one comma, return true if there not EOS
    bool skip_blank_and_comma();

    /*
     * [0..9a..bA..B-+._&]
     */
    static bool is_identifier_char(char c);

    // left idx positioned on the character stopping the read
    sstring read_next_identifier();

#if 0
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
#endif
};

}

}
