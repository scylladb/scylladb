/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

grammar Cql;

options {
    language = Java;
}

@header {
    package org.apache.cassandra.cql3;

    import java.util.ArrayList;
    import java.util.Arrays;
    import java.util.Collections;
    import java.util.EnumSet;
    import java.util.HashSet;
    import java.util.HashMap;
    import java.util.LinkedHashMap;
    import java.util.List;
    import java.util.Map;
    import java.util.Set;

    import org.apache.cassandra.auth.Permission;
    import org.apache.cassandra.auth.DataResource;
    import org.apache.cassandra.auth.IResource;
    import org.apache.cassandra.cql3.*;
    import org.apache.cassandra.cql3.statements.*;
    import org.apache.cassandra.cql3.selection.*;
    import org.apache.cassandra.cql3.functions.*;
    import org.apache.cassandra.db.marshal.CollectionType;
    import org.apache.cassandra.exceptions.ConfigurationException;
    import org.apache.cassandra.exceptions.InvalidRequestException;
    import org.apache.cassandra.exceptions.SyntaxException;
    import org.apache.cassandra.utils.Pair;
}

@members {
    private final List<ErrorListener> listeners = new ArrayList<ErrorListener>();
    private final List<ColumnIdentifier> bindVariables = new ArrayList<ColumnIdentifier>();

    public static final Set<String> reservedTypeNames = new HashSet<String>()
    {{
        add("byte");
        add("smallint");
        add("complex");
        add("enum");
        add("date");
        add("interval");
        add("macaddr");
        add("bitstring");
    }};

    public AbstractMarker.Raw newBindVariables(ColumnIdentifier name)
    {
        AbstractMarker.Raw marker = new AbstractMarker.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public AbstractMarker.INRaw newINBindVariables(ColumnIdentifier name)
    {
        AbstractMarker.INRaw marker = new AbstractMarker.INRaw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public Tuples.Raw newTupleBindVariables(ColumnIdentifier name)
    {
        Tuples.Raw marker = new Tuples.Raw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public Tuples.INRaw newTupleINBindVariables(ColumnIdentifier name)
    {
        Tuples.INRaw marker = new Tuples.INRaw(bindVariables.size());
        bindVariables.add(name);
        return marker;
    }

    public void addErrorListener(ErrorListener listener)
    {
        this.listeners.add(listener);
    }

    public void removeErrorListener(ErrorListener listener)
    {
        this.listeners.remove(listener);
    }

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        for (int i = 0, m = listeners.size(); i < m; i++)
            listeners.get(i).syntaxError(this, tokenNames, e);
    }

    private void addRecognitionError(String msg)
    {
        for (int i = 0, m = listeners.size(); i < m; i++)
            listeners.get(i).syntaxError(this, msg);
    }

    public Map<String, String> convertPropertyMap(Maps.Literal map)
    {
        if (map == null || map.entries == null || map.entries.isEmpty())
            return Collections.<String, String>emptyMap();

        Map<String, String> res = new HashMap<String, String>(map.entries.size());

        for (Pair<Term.Raw, Term.Raw> entry : map.entries)
        {
            // Because the parser tries to be smart and recover on error (to
            // allow displaying more than one error I suppose), we have null
            // entries in there. Just skip those, a proper error will be thrown in the end.
            if (entry.left == null || entry.right == null)
                break;

            if (!(entry.left instanceof Constants.Literal))
            {
                String msg = "Invalid property name: " + entry.left;
                if (entry.left instanceof AbstractMarker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }
            if (!(entry.right instanceof Constants.Literal))
            {
                String msg = "Invalid property value: " + entry.right + " for property: " + entry.left;
                if (entry.right instanceof AbstractMarker.Raw)
                    msg += " (bind variables are not supported in DDL queries)";
                addRecognitionError(msg);
                break;
            }

            res.put(((Constants.Literal)entry.left).getRawText(), ((Constants.Literal)entry.right).getRawText());
        }

        return res;
    }

    public void addRawUpdate(List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key, Operation.RawUpdate update)
    {
        for (Pair<ColumnIdentifier.Raw, Operation.RawUpdate> p : operations)
        {
            if (p.left.equals(key) && !p.right.isCompatibleWith(update))
                addRecognitionError("Multiple incompatible setting of column " + key);
        }
        operations.add(Pair.create(key, update));
    }
}

@lexer::header {
    package org.apache.cassandra.cql3;

    import org.apache.cassandra.exceptions.SyntaxException;
}

@lexer::members {
    List<Token> tokens = new ArrayList<Token>();

    public void emit(Token token)
    {
        state.token = token;
        tokens.add(token);
    }

    public Token nextToken()
    {
        super.nextToken();
        if (tokens.size() == 0)
            return new CommonToken(Token.EOF);
        return tokens.remove(0);
    }

    private final List<ErrorListener> listeners = new ArrayList<ErrorListener>();

    public void addErrorListener(ErrorListener listener)
    {
        this.listeners.add(listener);
    }

    public void removeErrorListener(ErrorListener listener)
    {
        this.listeners.remove(listener);
    }

    public void displayRecognitionError(String[] tokenNames, RecognitionException e)
    {
        for (int i = 0, m = listeners.size(); i < m; i++)
            listeners.get(i).syntaxError(this, tokenNames, e);
    }
}

/** STATEMENTS **/

query returns [ParsedStatement stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = st; }
    ;

cqlStatement returns [ParsedStatement stmt]
    @after{ if (stmt != null) stmt.setBoundVariables(bindVariables); }
    : st1= selectStatement             { $stmt = st1; }
    | st2= insertStatement             { $stmt = st2; }
    | st3= updateStatement             { $stmt = st3; }
    | st4= batchStatement              { $stmt = st4; }
    | st5= deleteStatement             { $stmt = st5; }
    | st6= useStatement                { $stmt = st6; }
    | st7= truncateStatement           { $stmt = st7; }
    | st8= createKeyspaceStatement     { $stmt = st8; }
    | st9= createTableStatement        { $stmt = st9; }
    | st10=createIndexStatement        { $stmt = st10; }
    | st11=dropKeyspaceStatement       { $stmt = st11; }
    | st12=dropTableStatement          { $stmt = st12; }
    | st13=dropIndexStatement          { $stmt = st13; }
    | st14=alterTableStatement         { $stmt = st14; }
    | st15=alterKeyspaceStatement      { $stmt = st15; }
    | st16=grantStatement              { $stmt = st16; }
    | st17=revokeStatement             { $stmt = st17; }
    | st18=listPermissionsStatement    { $stmt = st18; }
    | st19=createUserStatement         { $stmt = st19; }
    | st20=alterUserStatement          { $stmt = st20; }
    | st21=dropUserStatement           { $stmt = st21; }
    | st22=listUsersStatement          { $stmt = st22; }
    | st23=createTriggerStatement      { $stmt = st23; }
    | st24=dropTriggerStatement        { $stmt = st24; }
    | st25=createTypeStatement         { $stmt = st25; }
    | st26=alterTypeStatement          { $stmt = st26; }
    | st27=dropTypeStatement           { $stmt = st27; }
    | st28=createFunctionStatement     { $stmt = st28; }
    | st29=dropFunctionStatement       { $stmt = st29; }
    | st30=createAggregateStatement    { $stmt = st30; }
    | st31=dropAggregateStatement      { $stmt = st31; }
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement returns [UseStatement stmt]
    : K_USE ks=keyspaceName { $stmt = new UseStatement(ks); }
    ;

/**
 * SELECT <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [SelectStatement.RawStatement expr]
    @init {
        boolean isDistinct = false;
        Term.Raw limit = null;
        Map<ColumnIdentifier.Raw, Boolean> orderings = new LinkedHashMap<ColumnIdentifier.Raw, Boolean>();
        boolean allowFiltering = false;
    }
    : K_SELECT ( ( K_DISTINCT { isDistinct = true; } )? sclause=selectClause
               | sclause=selectCountClause )
      K_FROM cf=columnFamilyName
      ( K_WHERE wclause=whereClause )?
      ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
      ( K_LIMIT rows=intValue { limit = rows; } )?
      ( K_ALLOW K_FILTERING  { allowFiltering = true; } )?
      {
          SelectStatement.Parameters params = new SelectStatement.Parameters(orderings,
                                                                             isDistinct,
                                                                             allowFiltering);
          $expr = new SelectStatement.RawStatement(cf, params, sclause, wclause, limit);
      }
    ;

selectClause returns [List<RawSelector> expr]
    : t1=selector { $expr = new ArrayList<RawSelector>(); $expr.add(t1); } (',' tN=selector { $expr.add(tN); })*
    | '\*' { $expr = Collections.<RawSelector>emptyList();}
    ;

selector returns [RawSelector s]
    @init{ ColumnIdentifier alias = null; }
    : us=unaliasedSelector (K_AS c=ident { alias = c; })? { $s = new RawSelector(us, alias); }
    ;

unaliasedSelector returns [Selectable.Raw s]
    @init { Selectable.Raw tmp = null; }
    :  ( c=cident                                  { tmp = c; }
       | K_WRITETIME '(' c=cident ')'              { tmp = new Selectable.WritetimeOrTTL.Raw(c, true); }
       | K_TTL       '(' c=cident ')'              { tmp = new Selectable.WritetimeOrTTL.Raw(c, false); }
       | f=functionName args=selectionFunctionArgs { tmp = new Selectable.WithFunction.Raw(f, args); }
       ) ( '.' fi=cident { tmp = new Selectable.WithFieldSelection.Raw(tmp, fi); } )* { $s = tmp; }
    ;

selectionFunctionArgs returns [List<Selectable.Raw> a]
    : '(' ')' { $a = Collections.emptyList(); }
    | '(' s1=unaliasedSelector { List<Selectable.Raw> args = new ArrayList<Selectable.Raw>(); args.add(s1); }
          ( ',' sn=unaliasedSelector { args.add(sn); } )*
      ')' { $a = args; }
    ;

selectCountClause returns [List<RawSelector> expr]
    @init{ ColumnIdentifier alias = new ColumnIdentifier("count", false); }
    : K_COUNT '(' countArgument ')' (K_AS c=ident { alias = c; })? { $expr = new ArrayList<RawSelector>(); $expr.add( new RawSelector(new Selectable.WithFunction.Raw(FunctionName.nativeFunction("countRows"), Collections.<Selectable.Raw>emptyList()), alias));}
    ;

countArgument
    : '\*'
    | i=INTEGER { if (!i.getText().equals("1")) addRecognitionError("Only COUNT(1) is supported, got COUNT(" + i.getText() + ")");}
    ;

whereClause returns [List<Relation> clause]
    @init{ $clause = new ArrayList<Relation>(); }
    : relation[$clause] (K_AND relation[$clause])*
    ;

orderByClause[Map<ColumnIdentifier.Raw, Boolean> orderings]
    @init{
        boolean reversed = false;
    }
    : c=cident (K_ASC | K_DESC { reversed = true; })? { orderings.put(c, reversed); }
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement returns [UpdateStatement.ParsedInsert expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<ColumnIdentifier.Raw> columnNames  = new ArrayList<ColumnIdentifier.Raw>();
        List<Term.Raw> values = new ArrayList<Term.Raw>();
        boolean ifNotExists = false;
    }
    : K_INSERT K_INTO cf=columnFamilyName
          '(' c1=cident { columnNames.add(c1); }  ( ',' cn=cident { columnNames.add(cn); } )* ')'
        K_VALUES
          '(' v1=term { values.add(v1); } ( ',' vn=term { values.add(vn); } )* ')'

        ( K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
        ( usingClause[attrs] )?
      {
          $expr = new UpdateStatement.ParsedInsert(cf,
                                                   attrs,
                                                   columnNames,
                                                   values,
                                                   ifNotExists);
      }
    ;

usingClause[Attributes.Raw attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
    ;

usingClauseObjective[Attributes.Raw attrs]
    : K_TIMESTAMP ts=intValue { attrs.timestamp = ts; }
    | K_TTL t=intValue { attrs.timeToLive = t; }
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 */
updateStatement returns [UpdateStatement.ParsedUpdate expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations = new ArrayList<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>>();
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET columnOperation[operations] (',' columnOperation[operations])*
      K_WHERE wclause=whereClause
      ( K_IF conditions=updateConditions )?
      {
          return new UpdateStatement.ParsedUpdate(cf,
                                                  attrs,
                                                  operations,
                                                  wclause,
                                                  conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions);
     }
    ;

updateConditions returns [List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions]
    @init { conditions = new ArrayList<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>(); }
    : columnCondition[conditions] ( K_AND columnCondition[conditions] )*
    ;


/**
 * DELETE name1, name2
 * FROM <CF>
 * USING TIMESTAMP <long>
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement returns [DeleteStatement.Parsed expr]
    @init {
        Attributes.Raw attrs = new Attributes.Raw();
        List<Operation.RawDeletion> columnDeletions = Collections.emptyList();
        boolean ifExists = false;
    }
    : K_DELETE ( dels=deleteSelection { columnDeletions = dels; } )?
      K_FROM cf=columnFamilyName
      ( usingClauseDelete[attrs] )?
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { ifExists = true; } | conditions=updateConditions ))?
      {
          return new DeleteStatement.Parsed(cf,
                                            attrs,
                                            columnDeletions,
                                            wclause,
                                            conditions == null ? Collections.<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>>emptyList() : conditions,
                                            ifExists);
      }
    ;

deleteSelection returns [List<Operation.RawDeletion> operations]
    : { $operations = new ArrayList<Operation.RawDeletion>(); }
          t1=deleteOp { $operations.add(t1); }
          (',' tN=deleteOp { $operations.add(tN); })*
    ;

deleteOp returns [Operation.RawDeletion op]
    : c=cident                { $op = new Operation.ColumnDeletion(c); }
    | c=cident '[' t=term ']' { $op = new Operation.ElementDeletion(c, t); }
    ;

usingClauseDelete[Attributes.Raw attrs]
    : K_USING K_TIMESTAMP ts=intValue { attrs.timestamp = ts; }
    ;

/**
 * BEGIN BATCH
 *   UPDATE <CF> SET name1 = value1 WHERE KEY = keyname1;
 *   UPDATE <CF> SET name2 = value2 WHERE KEY = keyname2;
 *   UPDATE <CF> SET name3 = value3 WHERE KEY = keyname3;
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   INSERT INTO <CF> (KEY, <name>) VALUES ('<key>', '<value>');
 *   ...
 * APPLY BATCH
 *
 * OR
 *
 * BEGIN BATCH
 *   DELETE name1, name2 FROM <CF> WHERE key = <key>
 *   DELETE name3, name4 FROM <CF> WHERE key = <key>
 *   ...
 * APPLY BATCH
 */
batchStatement returns [BatchStatement.Parsed expr]
    @init {
        BatchStatement.Type type = BatchStatement.Type.LOGGED;
        List<ModificationStatement.Parsed> statements = new ArrayList<ModificationStatement.Parsed>();
        Attributes.Raw attrs = new Attributes.Raw();
    }
    : K_BEGIN
      ( K_UNLOGGED { type = BatchStatement.Type.UNLOGGED; } | K_COUNTER { type = BatchStatement.Type.COUNTER; } )?
      K_BATCH ( usingClause[attrs] )?
          ( s=batchStatementObjective ';'? { statements.add(s); } )*
      K_APPLY K_BATCH
      {
          return new BatchStatement.Parsed(type, attrs, statements);
      }
    ;

batchStatementObjective returns [ModificationStatement.Parsed statement]
    : i=insertStatement  { $statement = i; }
    | u=updateStatement  { $statement = u; }
    | d=deleteStatement  { $statement = d; }
    ;

createAggregateStatement returns [CreateAggregateStatement expr]
    @init {
        boolean orReplace = false;
        boolean ifNotExists = false;

        List<CQL3Type.Raw> argsTypes = new ArrayList<>();
    }
    : K_CREATE (K_OR K_REPLACE { orReplace = true; })?
      K_AGGREGATE
      (K_IF K_NOT K_EXISTS { ifNotExists = true; })?
      fn=functionName
      '('
        (
          v=comparatorType { argsTypes.add(v); }
          ( ',' v=comparatorType { argsTypes.add(v); } )*
        )?
      ')'
      K_SFUNC sfunc = allowedFunctionName
      K_STYPE stype = comparatorType
      (
        K_FINALFUNC ffunc = allowedFunctionName
      )?
      (
        K_INITCOND ival = term
      )?
      { $expr = new CreateAggregateStatement(fn, argsTypes, sfunc, stype, ffunc, ival, orReplace, ifNotExists); }
    ;

dropAggregateStatement returns [DropAggregateStatement expr]
    @init {
        boolean ifExists = false;
        List<CQL3Type.Raw> argsTypes = new ArrayList<>();
        boolean argsPresent = false;
    }
    : K_DROP K_AGGREGATE
      (K_IF K_EXISTS { ifExists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { argsTypes.add(v); }
            ( ',' v=comparatorType { argsTypes.add(v); } )*
          )?
        ')'
        { argsPresent = true; }
      )?
      { $expr = new DropAggregateStatement(fn, argsTypes, argsPresent, ifExists); }
    ;

createFunctionStatement returns [CreateFunctionStatement expr]
    @init {
        boolean orReplace = false;
        boolean ifNotExists = false;

        boolean deterministic = true;
        List<ColumnIdentifier> argsNames = new ArrayList<>();
        List<CQL3Type.Raw> argsTypes = new ArrayList<>();
    }
    : K_CREATE (K_OR K_REPLACE { orReplace = true; })?
      ((K_NON { deterministic = false; })? K_DETERMINISTIC)?
      K_FUNCTION
      (K_IF K_NOT K_EXISTS { ifNotExists = true; })?
      fn=functionName
      '('
        (
          k=ident v=comparatorType { argsNames.add(k); argsTypes.add(v); }
          ( ',' k=ident v=comparatorType { argsNames.add(k); argsTypes.add(v); } )*
        )?
      ')'
      K_RETURNS rt = comparatorType
      K_LANGUAGE language = IDENT
      K_AS body = STRING_LITERAL
      { $expr = new CreateFunctionStatement(fn, $language.text.toLowerCase(), $body.text, deterministic, argsNames, argsTypes, rt, orReplace, ifNotExists); }
    ;

dropFunctionStatement returns [DropFunctionStatement expr]
    @init {
        boolean ifExists = false;
        List<CQL3Type.Raw> argsTypes = new ArrayList<>();
        boolean argsPresent = false;
    }
    : K_DROP K_FUNCTION
      (K_IF K_EXISTS { ifExists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { argsTypes.add(v); }
            ( ',' v=comparatorType { argsTypes.add(v); } )*
          )?
        ')'
        { argsPresent = true; }
      )?
      { $expr = new DropFunctionStatement(fn, argsTypes, argsPresent, ifExists); }
    ;

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [CreateKeyspaceStatement expr]
    @init {
        KSPropDefs attrs = new KSPropDefs();
        boolean ifNotExists = false;
    }
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? ks=keyspaceName
      K_WITH properties[attrs] { $expr = new CreateKeyspaceStatement(ks, attrs, ifNotExists); }
    ;

/**
 * CREATE COLUMNFAMILY [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement returns [CreateTableStatement.RawStatement expr]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
      cf=columnFamilyName { $expr = new CreateTableStatement.RawStatement(cf, ifNotExists); }
      cfamDefinition[expr]
    ;

cfamDefinition[CreateTableStatement.RawStatement expr]
    : '(' cfamColumns[expr] ( ',' cfamColumns[expr]? )* ')'
      ( K_WITH cfamProperty[expr] ( K_AND cfamProperty[expr] )*)?
    ;

cfamColumns[CreateTableStatement.RawStatement expr]
    : k=ident v=comparatorType { boolean isStatic=false; } (K_STATIC {isStatic = true;})? { $expr.addDefinition(k, v, isStatic); }
        (K_PRIMARY K_KEY { $expr.addKeyAliases(Collections.singletonList(k)); })?
    | K_PRIMARY K_KEY '(' pkDef[expr] (',' c=ident { $expr.addColumnAlias(c); } )* ')'
    ;

pkDef[CreateTableStatement.RawStatement expr]
    : k=ident { $expr.addKeyAliases(Collections.singletonList(k)); }
    | '(' { List<ColumnIdentifier> l = new ArrayList<ColumnIdentifier>(); } k1=ident { l.add(k1); } ( ',' kn=ident { l.add(kn); } )* ')' { $expr.addKeyAliases(l); }
    ;

cfamProperty[CreateTableStatement.RawStatement expr]
    : property[expr.properties]
    | K_COMPACT K_STORAGE { $expr.setCompactStorage(); }
    | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] (',' cfamOrdering[expr])* ')'
    ;

cfamOrdering[CreateTableStatement.RawStatement expr]
    @init{ boolean reversed=false; }
    : k=ident (K_ASC | K_DESC { reversed=true;} ) { $expr.setOrdering(k, reversed); }
    ;


/**
 * CREATE TYPE foo (
 *    <name1> <type1>,
 *    <name2> <type2>,
 *    ....
 * )
 */
createTypeStatement returns [CreateTypeStatement expr]
    @init { boolean ifNotExists = false; }
    : K_CREATE K_TYPE (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
         tn=userTypeName { $expr = new CreateTypeStatement(tn, ifNotExists); }
         '(' typeColumns[expr] ( ',' typeColumns[expr]? )* ')'
    ;

typeColumns[CreateTypeStatement expr]
    : k=ident v=comparatorType { $expr.addDefinition(k, v); }
    ;


/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement returns [CreateIndexStatement expr]
    @init {
        IndexPropDefs props = new IndexPropDefs();
        boolean ifNotExists = false;
    }
    : K_CREATE (K_CUSTOM { props.isCustom = true; })? K_INDEX (K_IF K_NOT K_EXISTS { ifNotExists = true; } )?
        (idxName=IDENT)? K_ON cf=columnFamilyName '(' id=indexIdent ')'
        (K_USING cls=STRING_LITERAL { props.customClass = $cls.text; })?
        (K_WITH properties[props])?
      { $expr = new CreateIndexStatement(cf, $idxName.text, id, props, ifNotExists); }
    ;

indexIdent returns [IndexTarget.Raw id]
    : c=cident                   { $id = IndexTarget.Raw.valuesOf(c); }
    | K_KEYS '(' c=cident ')'    { $id = IndexTarget.Raw.keysOf(c); }
    | K_ENTRIES '(' c=cident ')' { $id = IndexTarget.Raw.keysAndValuesOf(c); }
    | K_FULL '(' c=cident ')'    { $id = IndexTarget.Raw.fullCollection(c); }
    ;


/**
 * CREATE TRIGGER triggerName ON columnFamily USING 'triggerClass';
 */
createTriggerStatement returns [CreateTriggerStatement expr]
    @init {
        boolean ifNotExists = false;
    }
    : K_CREATE K_TRIGGER (K_IF K_NOT K_EXISTS { ifNotExists = true; } )? (name=cident)
        K_ON cf=columnFamilyName K_USING cls=STRING_LITERAL
      { $expr = new CreateTriggerStatement(cf, name.toString(), $cls.text, ifNotExists); }
    ;

/**
 * DROP TRIGGER [IF EXISTS] triggerName ON columnFamily;
 */
dropTriggerStatement returns [DropTriggerStatement expr]
     @init { boolean ifExists = false; }
    : K_DROP K_TRIGGER (K_IF K_EXISTS { ifExists = true; } )? (name=cident) K_ON cf=columnFamilyName
      { $expr = new DropTriggerStatement(cf, name.toString(), ifExists); }
    ;

/**
 * ALTER KEYSPACE <KS> WITH <property> = <value>;
 */
alterKeyspaceStatement returns [AlterKeyspaceStatement expr]
    @init { KSPropDefs attrs = new KSPropDefs(); }
    : K_ALTER K_KEYSPACE ks=keyspaceName
        K_WITH properties[attrs] { $expr = new AlterKeyspaceStatement(ks, attrs); }
    ;


/**
 * ALTER COLUMN FAMILY <CF> ALTER <column> TYPE <newtype>;
 * ALTER COLUMN FAMILY <CF> ADD <column> <newtype>;
 * ALTER COLUMN FAMILY <CF> DROP <column>;
 * ALTER COLUMN FAMILY <CF> WITH <property> = <value>;
 * ALTER COLUMN FAMILY <CF> RENAME <column> TO <column>;
 */
alterTableStatement returns [AlterTableStatement expr]
    @init {
        AlterTableStatement.Type type = null;
        CFPropDefs props = new CFPropDefs();
        Map<ColumnIdentifier.Raw, ColumnIdentifier.Raw> renames = new HashMap<ColumnIdentifier.Raw, ColumnIdentifier.Raw>();
        boolean isStatic = false;
    }
    : K_ALTER K_COLUMNFAMILY cf=columnFamilyName
          ( K_ALTER id=cident K_TYPE v=comparatorType { type = AlterTableStatement.Type.ALTER; }
          | K_ADD   id=cident v=comparatorType ({ isStatic=true; } K_STATIC)? { type = AlterTableStatement.Type.ADD; }
          | K_DROP  id=cident                         { type = AlterTableStatement.Type.DROP; }
          | K_WITH  properties[props]                 { type = AlterTableStatement.Type.OPTS; }
          | K_RENAME                                  { type = AlterTableStatement.Type.RENAME; }
               id1=cident K_TO toId1=cident { renames.put(id1, toId1); }
               ( K_AND idn=cident K_TO toIdn=cident { renames.put(idn, toIdn); } )*
          )
    {
        $expr = new AlterTableStatement(cf, type, id, v, props, renames, isStatic);
    }
    ;

/**
 * ALTER TYPE <name> ALTER <field> TYPE <newtype>;
 * ALTER TYPE <name> ADD <field> <newtype>;
 * ALTER TYPE <name> RENAME <field> TO <newtype> AND ...;
 */
alterTypeStatement returns [AlterTypeStatement expr]
    : K_ALTER K_TYPE name=userTypeName
          ( K_ALTER f=ident K_TYPE v=comparatorType { $expr = AlterTypeStatement.alter(name, f, v); }
          | K_ADD   f=ident v=comparatorType        { $expr = AlterTypeStatement.addition(name, f, v); }
          | K_RENAME
               { Map<ColumnIdentifier, ColumnIdentifier> renames = new HashMap<ColumnIdentifier, ColumnIdentifier>(); }
                 id1=ident K_TO toId1=ident { renames.put(id1, toId1); }
                 ( K_AND idn=ident K_TO toIdn=ident { renames.put(idn, toIdn); } )*
               { $expr = AlterTypeStatement.renames(name, renames); }
          )
    ;


/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement returns [DropKeyspaceStatement ksp]
    @init { boolean ifExists = false; }
    : K_DROP K_KEYSPACE (K_IF K_EXISTS { ifExists = true; } )? ks=keyspaceName { $ksp = new DropKeyspaceStatement(ks, ifExists); }
    ;

/**
 * DROP COLUMNFAMILY [IF EXISTS] <CF>;
 */
dropTableStatement returns [DropTableStatement stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS { ifExists = true; } )? cf=columnFamilyName { $stmt = new DropTableStatement(cf, ifExists); }
    ;

/**
 * DROP TYPE <name>;
 */
dropTypeStatement returns [DropTypeStatement stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_TYPE (K_IF K_EXISTS { ifExists = true; } )? name=userTypeName { $stmt = new DropTypeStatement(name, ifExists); }
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement returns [DropIndexStatement expr]
    @init { boolean ifExists = false; }
    : K_DROP K_INDEX (K_IF K_EXISTS { ifExists = true; } )? index=indexName
      { $expr = new DropIndexStatement(index, ifExists); }
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement returns [TruncateStatement stmt]
    : K_TRUNCATE cf=columnFamilyName { $stmt = new TruncateStatement(cf); }
    ;

/**
 * GRANT <permission> ON <resource> TO <username>
 */
grantStatement returns [GrantStatement stmt]
    : K_GRANT
          permissionOrAll
      K_ON
          resource
      K_TO
          username
      { $stmt = new GrantStatement($permissionOrAll.perms, $resource.res, $username.text); }
    ;

/**
 * REVOKE <permission> ON <resource> FROM <username>
 */
revokeStatement returns [RevokeStatement stmt]
    : K_REVOKE
          permissionOrAll
      K_ON
          resource
      K_FROM
          username
      { $stmt = new RevokeStatement($permissionOrAll.perms, $resource.res, $username.text); }
    ;

listPermissionsStatement returns [ListPermissionsStatement stmt]
    @init {
        IResource resource = null;
        String username = null;
        boolean recursive = true;
    }
    : K_LIST
          permissionOrAll
      ( K_ON resource { resource = $resource.res; } )?
      ( K_OF username { username = $username.text; } )?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = new ListPermissionsStatement($permissionOrAll.perms, resource, username, recursive); }
    ;

permission returns [Permission perm]
    : p=(K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE)
    { $perm = Permission.valueOf($p.text.toUpperCase()); }
    ;

permissionOrAll returns [Set<Permission> perms]
    : K_ALL ( K_PERMISSIONS )?       { $perms = Permission.ALL_DATA; }
    | p=permission ( K_PERMISSION )? { $perms = EnumSet.of($p.perm); }
    ;

resource returns [IResource res]
    : r=dataResource { $res = $r.res; }
    ;

dataResource returns [DataResource res]
    : K_ALL K_KEYSPACES { $res = DataResource.root(); }
    | K_KEYSPACE ks = keyspaceName { $res = DataResource.keyspace($ks.id); }
    | ( K_COLUMNFAMILY )? cf = columnFamilyName
      { $res = DataResource.columnFamily($cf.name.getKeyspace(), $cf.name.getColumnFamily()); }
    ;

/**
 * CREATE USER [IF NOT EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement returns [CreateUserStatement stmt]
    @init {
        UserOptions opts = new UserOptions();
        boolean superuser = false;
        boolean ifNotExists = false;
    }
    : K_CREATE K_USER (K_IF K_NOT K_EXISTS { ifNotExists = true; })? username
      ( K_WITH userOptions[opts] )?
      ( K_SUPERUSER { superuser = true; } | K_NOSUPERUSER { superuser = false; } )?
      { $stmt = new CreateUserStatement($username.text, opts, superuser, ifNotExists); }
    ;

/**
 * ALTER USER <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement returns [AlterUserStatement stmt]
    @init {
        UserOptions opts = new UserOptions();
        Boolean superuser = null;
    }
    : K_ALTER K_USER username
      ( K_WITH userOptions[opts] )?
      ( K_SUPERUSER { superuser = true; } | K_NOSUPERUSER { superuser = false; } )?
      { $stmt = new AlterUserStatement($username.text, opts, superuser); }
    ;

/**
 * DROP USER [IF EXISTS] <username>
 */
dropUserStatement returns [DropUserStatement stmt]
    @init { boolean ifExists = false; }
    : K_DROP K_USER (K_IF K_EXISTS { ifExists = true; })? username { $stmt = new DropUserStatement($username.text, ifExists); }
    ;

/**
 * LIST USERS
 */
listUsersStatement returns [ListUsersStatement stmt]
    : K_LIST K_USERS { $stmt = new ListUsersStatement(); }
    ;

userOptions[UserOptions opts]
    : userOption[opts]
    ;

userOption[UserOptions opts]
    : k=K_PASSWORD v=STRING_LITERAL { opts.put($k.text, $v.text); }
    ;

/** DEFINITIONS **/

// Column Identifiers.  These need to be treated differently from other
// identifiers because the underlying comparator is not necessarily text. See
// CASSANDRA-8178 for details.
cident returns [ColumnIdentifier.Raw id]
    : t=IDENT              { $id = new ColumnIdentifier.Raw($t.text, false); }
    | t=QUOTED_NAME        { $id = new ColumnIdentifier.Raw($t.text, true); }
    | k=unreserved_keyword { $id = new ColumnIdentifier.Raw(k, false); }
    ;

// Identifiers that do not refer to columns or where the comparator is known to be text
ident returns [ColumnIdentifier id]
    : t=IDENT              { $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME        { $id = new ColumnIdentifier($t.text, true); }
    | k=unreserved_keyword { $id = new ColumnIdentifier(k, false); }
    ;

// Keyspace & Column family names
keyspaceName returns [String id]
    @init { CFName name = new CFName(); }
    : cfOrKsName[name, true] { $id = name.getKeyspace(); }
    ;

indexName returns [IndexName name]
    @init { $name = new IndexName(); }
    : (idxOrKsName[name, true] '.')? idxOrKsName[name, false]
    ;

idxOrKsName[IndexName name, boolean isKs]
    : t=IDENT              { if (isKs) $name.setKeyspace($t.text, false); else $name.setIndex($t.text, false); }
    | t=QUOTED_NAME        { if (isKs) $name.setKeyspace($t.text, true); else $name.setIndex($t.text, true); }
    | k=unreserved_keyword { if (isKs) $name.setKeyspace(k, false); else $name.setIndex(k, false); }
    ;

columnFamilyName returns [CFName name]
    @init { $name = new CFName(); }
    : (cfOrKsName[name, true] '.')? cfOrKsName[name, false]
    ;

userTypeName returns [UTName name]
    : (ks=ident '.')? ut=non_type_ident { return new UTName(ks, ut); }
    ;

cfOrKsName[CFName name, boolean isKs]
    : t=IDENT              { if (isKs) $name.setKeyspace($t.text, false); else $name.setColumnFamily($t.text, false); }
    | t=QUOTED_NAME        { if (isKs) $name.setKeyspace($t.text, true); else $name.setColumnFamily($t.text, true); }
    | k=unreserved_keyword { if (isKs) $name.setKeyspace(k, false); else $name.setColumnFamily(k, false); }
    | QMARK {addRecognitionError("Bind variables cannot be used for keyspace or table names");}
    ;

constant returns [Constants.Literal constant]
    : t=STRING_LITERAL { $constant = Constants.Literal.string($t.text); }
    | t=INTEGER        { $constant = Constants.Literal.integer($t.text); }
    | t=FLOAT          { $constant = Constants.Literal.floatingPoint($t.text); }
    | t=BOOLEAN        { $constant = Constants.Literal.bool($t.text); }
    | t=UUID           { $constant = Constants.Literal.uuid($t.text); }
    | t=HEXNUMBER      { $constant = Constants.Literal.hex($t.text); }
    | { String sign=""; } ('-' {sign = "-"; } )? t=(K_NAN | K_INFINITY) { $constant = Constants.Literal.floatingPoint(sign + $t.text); }
    ;

mapLiteral returns [Maps.Literal map]
    : '{' { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); }
          ( k1=term ':' v1=term { m.add(Pair.create(k1, v1)); } ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )* )?
      '}' { $map = new Maps.Literal(m); }
    ;

setOrMapLiteral[Term.Raw t] returns [Term.Raw value]
    : ':' v=term { List<Pair<Term.Raw, Term.Raw>> m = new ArrayList<Pair<Term.Raw, Term.Raw>>(); m.add(Pair.create(t, v)); }
          ( ',' kn=term ':' vn=term { m.add(Pair.create(kn, vn)); } )*
      { $value = new Maps.Literal(m); }
    | { List<Term.Raw> s = new ArrayList<Term.Raw>(); s.add(t); }
          ( ',' tn=term { s.add(tn); } )*
      { $value = new Sets.Literal(s); }
    ;

collectionLiteral returns [Term.Raw value]
    : '[' { List<Term.Raw> l = new ArrayList<Term.Raw>(); }
          ( t1=term { l.add(t1); } ( ',' tn=term { l.add(tn); } )* )?
      ']' { $value = new Lists.Literal(l); }
    | '{' t=term v=setOrMapLiteral[t] { $value = v; } '}'
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}' { $value = new Sets.Literal(Collections.<Term.Raw>emptyList()); }
    ;

usertypeLiteral returns [UserTypes.Literal ut]
    @init{ Map<ColumnIdentifier, Term.Raw> m = new HashMap<ColumnIdentifier, Term.Raw>(); }
    @after{ $ut = new UserTypes.Literal(m); }
    // We don't allow empty literals because that conflicts with sets/maps and is currently useless since we don't allow empty user types
    : '{' k1=ident ':' v1=term { m.put(k1, v1); } ( ',' kn=ident ':' vn=term { m.put(kn, vn); } )* '}'
    ;

tupleLiteral returns [Tuples.Literal tt]
    @init{ List<Term.Raw> l = new ArrayList<Term.Raw>(); }
    @after{ $tt = new Tuples.Literal(l); }
    : '(' t1=term { l.add(t1); } ( ',' tn=term { l.add(tn); } )* ')'
    ;

value returns [Term.Raw value]
    : c=constant           { $value = c; }
    | l=collectionLiteral  { $value = l; }
    | u=usertypeLiteral    { $value = u; }
    | t=tupleLiteral       { $value = t; }
    | K_NULL               { $value = Constants.NULL_LITERAL; }
    | ':' id=ident         { $value = newBindVariables(id); }
    | QMARK                { $value = newBindVariables(null); }
    ;

intValue returns [Term.Raw value]
    :
    | t=INTEGER     { $value = Constants.Literal.integer($t.text); }
    | ':' id=ident  { $value = newBindVariables(id); }
    | QMARK         { $value = newBindVariables(null); }
    ;

functionName returns [FunctionName s]
    : (ks=keyspaceName '.')? f=allowedFunctionName   { $s = new FunctionName(ks, f); }
    ;

allowedFunctionName returns [String s]
    : f=IDENT                       { $s = $f.text.toLowerCase(); }
    | f=QUOTED_NAME                 { $s = $f.text; }
    | u=unreserved_function_keyword { $s = u; }
    | K_TOKEN                       { $s = "token"; }
    | K_COUNT                       { $s = "count"; }
    ;

functionArgs returns [List<Term.Raw> a]
    : '(' ')' { $a = Collections.emptyList(); }
    | '(' t1=term { List<Term.Raw> args = new ArrayList<Term.Raw>(); args.add(t1); }
          ( ',' tn=term { args.add(tn); } )*
       ')' { $a = args; }
    ;

term returns [Term.Raw term]
    : v=value                          { $term = v; }
    | f=functionName args=functionArgs { $term = new FunctionCall.Raw(f, args); }
    | '(' c=comparatorType ')' t=term  { $term = new TypeCast(c, t); }
    ;

columnOperation[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations]
    : key=cident columnOperationDifferentiator[operations, key]
    ;

columnOperationDifferentiator[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key]
    : '=' normalColumnOperation[operations, key]
    | '[' k=term ']' specializedColumnOperation[operations, key, k]
    ;

normalColumnOperation[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key]
    : t=term ('+' c=cident )?
      {
          if (c == null)
          {
              addRawUpdate(operations, key, new Operation.SetValue(t));
          }
          else
          {
              if (!key.equals(c))
                  addRecognitionError("Only expressions of the form X = <value> + X are supported.");
              addRawUpdate(operations, key, new Operation.Prepend(t));
          }
      }
    | c=cident sig=('+' | '-') t=term
      {
          if (!key.equals(c))
              addRecognitionError("Only expressions of the form X = X " + $sig.text + "<value> are supported.");
          addRawUpdate(operations, key, $sig.text.equals("+") ? new Operation.Addition(t) : new Operation.Substraction(t));
      }
    | c=cident i=INTEGER
      {
          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
          if (!key.equals(c))
              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
              addRecognitionError("Only expressions of the form X = X " + ($i.text.charAt(0) == '-' ? '-' : '+') + " <value> are supported.");
          addRawUpdate(operations, key, new Operation.Addition(Constants.Literal.integer($i.text)));
      }
    ;

specializedColumnOperation[List<Pair<ColumnIdentifier.Raw, Operation.RawUpdate>> operations, ColumnIdentifier.Raw key, Term.Raw k]
    : '=' t=term
      {
          addRawUpdate(operations, key, new Operation.SetElement(k, t));
      }
    ;

columnCondition[List<Pair<ColumnIdentifier.Raw, ColumnCondition.Raw>> conditions]
    // Note: we'll reject duplicates later
    : key=cident
        ( op=relationType t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleCondition(t, op))); }
        | K_IN
            ( values=singleColumnInValues { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleInCondition(values))); }
            | marker=inMarker { conditions.add(Pair.create(key, ColumnCondition.Raw.simpleInCondition(marker))); }
            )
        | '[' element=term ']'
            ( op=relationType t=term { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionCondition(t, element, op))); }
            | K_IN
                ( values=singleColumnInValues { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionInCondition(element, values))); }
                | marker=inMarker { conditions.add(Pair.create(key, ColumnCondition.Raw.collectionInCondition(element, marker))); }
                )
            )
        )
    ;

properties[PropertyDefinitions props]
    : property[props] (K_AND property[props])*
    ;

property[PropertyDefinitions props]
    : k=ident '=' (simple=propertyValue { try { $props.addProperty(k.toString(), simple); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } }
                  |   map=mapLiteral    { try { $props.addProperty(k.toString(), convertPropertyMap(map)); } catch (SyntaxException e) { addRecognitionError(e.getMessage()); } })
    ;

propertyValue returns [String str]
    : c=constant           { $str = c.getRawText(); }
    | u=unreserved_keyword { $str = u; }
    ;

relationType returns [Operator op]
    : '='  { $op = Operator.EQ; }
    | '<'  { $op = Operator.LT; }
    | '<=' { $op = Operator.LTE; }
    | '>'  { $op = Operator.GT; }
    | '>=' { $op = Operator.GTE; }
    | '!=' { $op = Operator.NEQ; }
    ;

relation[List<Relation> clauses]
    : name=cident type=relationType t=term { $clauses.add(new SingleColumnRelation(name, type, t)); }
    | K_TOKEN l=tupleOfIdentifiers type=relationType t=term
        { $clauses.add(new TokenRelation(l, type, t)); }
    | name=cident K_IN marker=inMarker
        { $clauses.add(new SingleColumnRelation(name, Operator.IN, marker)); }
    | name=cident K_IN inValues=singleColumnInValues
        { $clauses.add(SingleColumnRelation.createInRelation($name.id, inValues)); }
    | name=cident K_CONTAINS { Operator rt = Operator.CONTAINS; } (K_KEY { rt = Operator.CONTAINS_KEY; })?
        t=term { $clauses.add(new SingleColumnRelation(name, rt, t)); }
    | name=cident '[' key=term ']' type=relationType t=term { $clauses.add(new SingleColumnRelation(name, key, type, t)); }
    | ids=tupleOfIdentifiers
      ( K_IN
          ( '(' ')'
              { $clauses.add(MultiColumnRelation.createInRelation(ids, new ArrayList<Tuples.Literal>())); }
          | tupleInMarker=inMarkerForTuple /* (a, b, c) IN ? */
              { $clauses.add(MultiColumnRelation.createSingleMarkerInRelation(ids, tupleInMarker)); }
          | literals=tupleOfTupleLiterals /* (a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) */
              {
                  $clauses.add(MultiColumnRelation.createInRelation(ids, literals));
              }
          | markers=tupleOfMarkersForTuples /* (a, b, c) IN (?, ?, ...) */
              { $clauses.add(MultiColumnRelation.createInRelation(ids, markers)); }
          )
      | type=relationType literal=tupleLiteral /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
          {
              $clauses.add(MultiColumnRelation.createNonInRelation(ids, type, literal));
          }
      | type=relationType tupleMarker=markerForTuple /* (a, b, c) >= ? */
          { $clauses.add(MultiColumnRelation.createNonInRelation(ids, type, tupleMarker)); }
      )
    | '(' relation[$clauses] ')'
    ;

inMarker returns [AbstractMarker.INRaw marker]
    : QMARK { $marker = newINBindVariables(null); }
    | ':' name=ident { $marker = newINBindVariables(name); }
    ;

tupleOfIdentifiers returns [List<ColumnIdentifier.Raw> ids]
    @init { $ids = new ArrayList<ColumnIdentifier.Raw>(); }
    : '(' n1=cident { $ids.add(n1); } (',' ni=cident { $ids.add(ni); })* ')'
    ;

singleColumnInValues returns [List<Term.Raw> terms]
    @init { $terms = new ArrayList<Term.Raw>(); }
    : '(' ( t1 = term { $terms.add(t1); } (',' ti=term { $terms.add(ti); })* )? ')'
    ;

tupleOfTupleLiterals returns [List<Tuples.Literal> literals]
    @init { $literals = new ArrayList<>(); }
    : '(' t1=tupleLiteral { $literals.add(t1); } (',' ti=tupleLiteral { $literals.add(ti); })* ')'
    ;

markerForTuple returns [Tuples.Raw marker]
    : QMARK { $marker = newTupleBindVariables(null); }
    | ':' name=ident { $marker = newTupleBindVariables(name); }
    ;

tupleOfMarkersForTuples returns [List<Tuples.Raw> markers]
    @init { $markers = new ArrayList<Tuples.Raw>(); }
    : '(' m1=markerForTuple { $markers.add(m1); } (',' mi=markerForTuple { $markers.add(mi); })* ')'
    ;

inMarkerForTuple returns [Tuples.INRaw marker]
    : QMARK { $marker = newTupleINBindVariables(null); }
    | ':' name=ident { $marker = newTupleINBindVariables(name); }
    ;

comparatorType returns [CQL3Type.Raw t]
    : n=native_type     { $t = CQL3Type.Raw.from(n); }
    | c=collection_type { $t = c; }
    | tt=tuple_type     { $t = tt; }
    | id=userTypeName   { $t = CQL3Type.Raw.userType(id); }
    | K_FROZEN '<' f=comparatorType '>'
      {
        try {
            $t = CQL3Type.Raw.frozen(f);
        } catch (InvalidRequestException e) {
            addRecognitionError(e.getMessage());
        }
      }
    | s=STRING_LITERAL
      {
        try {
            $t = CQL3Type.Raw.from(new CQL3Type.Custom($s.text));
        } catch (SyntaxException e) {
            addRecognitionError("Cannot parse type " + $s.text + ": " + e.getMessage());
        } catch (ConfigurationException e) {
            addRecognitionError("Error setting type " + $s.text + ": " + e.getMessage());
        }
      }
    ;

native_type returns [CQL3Type t]
    : K_ASCII     { $t = CQL3Type.Native.ASCII; }
    | K_BIGINT    { $t = CQL3Type.Native.BIGINT; }
    | K_BLOB      { $t = CQL3Type.Native.BLOB; }
    | K_BOOLEAN   { $t = CQL3Type.Native.BOOLEAN; }
    | K_COUNTER   { $t = CQL3Type.Native.COUNTER; }
    | K_DECIMAL   { $t = CQL3Type.Native.DECIMAL; }
    | K_DOUBLE    { $t = CQL3Type.Native.DOUBLE; }
    | K_FLOAT     { $t = CQL3Type.Native.FLOAT; }
    | K_INET      { $t = CQL3Type.Native.INET;}
    | K_INT       { $t = CQL3Type.Native.INT; }
    | K_TEXT      { $t = CQL3Type.Native.TEXT; }
    | K_TIMESTAMP { $t = CQL3Type.Native.TIMESTAMP; }
    | K_UUID      { $t = CQL3Type.Native.UUID; }
    | K_VARCHAR   { $t = CQL3Type.Native.VARCHAR; }
    | K_VARINT    { $t = CQL3Type.Native.VARINT; }
    | K_TIMEUUID  { $t = CQL3Type.Native.TIMEUUID; }
    ;

collection_type returns [CQL3Type.Raw pt]
    : K_MAP  '<' t1=comparatorType ',' t2=comparatorType '>'
        {
            // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
            if (t1 != null && t2 != null)
                $pt = CQL3Type.Raw.map(t1, t2);
        }
    | K_LIST '<' t=comparatorType '>'
        { if (t != null) $pt = CQL3Type.Raw.list(t); }
    | K_SET  '<' t=comparatorType '>'
        { if (t != null) $pt = CQL3Type.Raw.set(t); }
    ;

tuple_type returns [CQL3Type.Raw t]
    : K_TUPLE '<' { List<CQL3Type.Raw> types = new ArrayList<>(); }
         t1=comparatorType { types.add(t1); } (',' tn=comparatorType { types.add(tn); })*
      '>' { $t = CQL3Type.Raw.tuple(types); }
    ;

username
    : IDENT
    | STRING_LITERAL
    ;

// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
non_type_ident returns [ColumnIdentifier id]
    : t=IDENT                    { if (reservedTypeNames.contains($t.text)) addRecognitionError("Invalid (reserved) user type name " + $t.text); $id = new ColumnIdentifier($t.text, false); }
    | t=QUOTED_NAME              { $id = new ColumnIdentifier($t.text, true); }
    | k=basic_unreserved_keyword { $id = new ColumnIdentifier(k, false); }
    | kk=K_KEY                   { $id = new ColumnIdentifier($kk.text, false); }
    ;

unreserved_keyword returns [String str]
    : u=unreserved_function_keyword     { $str = u; }
    | k=(K_TTL | K_COUNT | K_WRITETIME | K_KEY) { $str = $k.text; }
    ;

unreserved_function_keyword returns [String str]
    : u=basic_unreserved_keyword { $str = u; }
    | t=native_type              { $str = t.toString(); }
    ;

basic_unreserved_keyword returns [String str]
    : k=( K_KEYS
        | K_AS
        | K_CLUSTERING
        | K_COMPACT
        | K_STORAGE
        | K_TYPE
        | K_VALUES
        | K_MAP
        | K_LIST
        | K_FILTERING
        | K_PERMISSION
        | K_PERMISSIONS
        | K_KEYSPACES
        | K_ALL
        | K_USER
        | K_USERS
        | K_SUPERUSER
        | K_NOSUPERUSER
        | K_PASSWORD
        | K_EXISTS
        | K_CUSTOM
        | K_TRIGGER
        | K_DISTINCT
        | K_CONTAINS
        | K_STATIC
        | K_FUNCTION
        | K_AGGREGATE
        | K_SFUNC
        | K_STYPE
        | K_FINALFUNC
        | K_INITCOND
        | K_RETURNS
        | K_LANGUAGE
        | K_NON
        | K_DETERMINISTIC
        ) { $str = $k.text; }
    ;

// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_AS:          A S;
K_WHERE:       W H E R E;
K_AND:         A N D;
K_KEY:         K E Y;
K_KEYS:        K E Y S;
K_ENTRIES:     E N T R I E S;
K_FULL:        F U L L;
K_INSERT:      I N S E R T;
K_UPDATE:      U P D A T E;
K_WITH:        W I T H;
K_LIMIT:       L I M I T;
K_USING:       U S I N G;
K_USE:         U S E;
K_DISTINCT:    D I S T I N C T;
K_COUNT:       C O U N T;
K_SET:         S E T;
K_BEGIN:       B E G I N;
K_UNLOGGED:    U N L O G G E D;
K_BATCH:       B A T C H;
K_APPLY:       A P P L Y;
K_TRUNCATE:    T R U N C A T E;
K_DELETE:      D E L E T E;
K_IN:          I N;
K_CREATE:      C R E A T E;
K_KEYSPACE:    ( K E Y S P A C E
                 | S C H E M A );
K_KEYSPACES:   K E Y S P A C E S;
K_COLUMNFAMILY:( C O L U M N F A M I L Y
                 | T A B L E );
K_INDEX:       I N D E X;
K_CUSTOM:      C U S T O M;
K_ON:          O N;
K_TO:          T O;
K_DROP:        D R O P;
K_PRIMARY:     P R I M A R Y;
K_INTO:        I N T O;
K_VALUES:      V A L U E S;
K_TIMESTAMP:   T I M E S T A M P;
K_TTL:         T T L;
K_ALTER:       A L T E R;
K_RENAME:      R E N A M E;
K_ADD:         A D D;
K_TYPE:        T Y P E;
K_COMPACT:     C O M P A C T;
K_STORAGE:     S T O R A G E;
K_ORDER:       O R D E R;
K_BY:          B Y;
K_ASC:         A S C;
K_DESC:        D E S C;
K_ALLOW:       A L L O W;
K_FILTERING:   F I L T E R I N G;
K_IF:          I F;
K_CONTAINS:    C O N T A I N S;

K_GRANT:       G R A N T;
K_ALL:         A L L;
K_PERMISSION:  P E R M I S S I O N;
K_PERMISSIONS: P E R M I S S I O N S;
K_OF:          O F;
K_REVOKE:      R E V O K E;
K_MODIFY:      M O D I F Y;
K_AUTHORIZE:   A U T H O R I Z E;
K_NORECURSIVE: N O R E C U R S I V E;

K_USER:        U S E R;
K_USERS:       U S E R S;
K_SUPERUSER:   S U P E R U S E R;
K_NOSUPERUSER: N O S U P E R U S E R;
K_PASSWORD:    P A S S W O R D;

K_CLUSTERING:  C L U S T E R I N G;
K_ASCII:       A S C I I;
K_BIGINT:      B I G I N T;
K_BLOB:        B L O B;
K_BOOLEAN:     B O O L E A N;
K_COUNTER:     C O U N T E R;
K_DECIMAL:     D E C I M A L;
K_DOUBLE:      D O U B L E;
K_FLOAT:       F L O A T;
K_INET:        I N E T;
K_INT:         I N T;
K_TEXT:        T E X T;
K_UUID:        U U I D;
K_VARCHAR:     V A R C H A R;
K_VARINT:      V A R I N T;
K_TIMEUUID:    T I M E U U I D;
K_TOKEN:       T O K E N;
K_WRITETIME:   W R I T E T I M E;

K_NULL:        N U L L;
K_NOT:         N O T;
K_EXISTS:      E X I S T S;

K_MAP:         M A P;
K_LIST:        L I S T;
K_NAN:         N A N;
K_INFINITY:    I N F I N I T Y;
K_TUPLE:       T U P L E;

K_TRIGGER:     T R I G G E R;
K_STATIC:      S T A T I C;
K_FROZEN:      F R O Z E N;

K_FUNCTION:    F U N C T I O N;
K_AGGREGATE:   A G G R E G A T E;
K_SFUNC:       S F U N C;
K_STYPE:       S T Y P E;
K_FINALFUNC:   F I N A L F U N C;
K_INITCOND:    I N I T C O N D;
K_RETURNS:     R E T U R N S;
K_LANGUAGE:    L A N G U A G E;
K_NON:         N O N;
K_OR:          O R;
K_REPLACE:     R E P L A C E;
K_DETERMINISTIC: D E T E R M I N I S T I C;

// Case-insensitive alpha characters
fragment A: ('a'|'A');
fragment B: ('b'|'B');
fragment C: ('c'|'C');
fragment D: ('d'|'D');
fragment E: ('e'|'E');
fragment F: ('f'|'F');
fragment G: ('g'|'G');
fragment H: ('h'|'H');
fragment I: ('i'|'I');
fragment J: ('j'|'J');
fragment K: ('k'|'K');
fragment L: ('l'|'L');
fragment M: ('m'|'M');
fragment N: ('n'|'N');
fragment O: ('o'|'O');
fragment P: ('p'|'P');
fragment Q: ('q'|'Q');
fragment R: ('r'|'R');
fragment S: ('s'|'S');
fragment T: ('t'|'T');
fragment U: ('u'|'U');
fragment V: ('v'|'V');
fragment W: ('w'|'W');
fragment X: ('x'|'X');
fragment Y: ('y'|'Y');
fragment Z: ('z'|'Z');

STRING_LITERAL
    @init{
        StringBuilder txt = new StringBuilder(); // temporary to build pg-style-string
    }
    @after{ setText(txt.toString()); }
    :
      /* pg-style string literal */
      (
        '\$' '\$'
        ( /* collect all input until '$$' is reached again */
          {  (input.size() - input.index() > 1)
               && !"$$".equals(input.substring(input.index(), input.index() + 1)) }?
             => c=. { txt.appendCodePoint(c); }
        )*
        '\$' '\$'
      )
      |
      /* conventional quoted string literal */
      (
        '\'' (c=~('\'') { txt.appendCodePoint(c);} | '\'' '\'' { txt.appendCodePoint('\''); })* '\''
      )
    ;

QUOTED_NAME
    @init{ StringBuilder b = new StringBuilder(); }
    @after{ setText(b.toString()); }
    : '\"' (c=~('\"') { b.appendCodePoint(c); } | '\"' '\"' { b.appendCodePoint('\"'); })+ '\"'
    ;

fragment DIGIT
    : '0'..'9'
    ;

fragment LETTER
    : ('A'..'Z' | 'a'..'z')
    ;

fragment HEX
    : ('A'..'F' | 'a'..'f' | '0'..'9')
    ;

fragment EXPONENT
    : E ('+' | '-')? DIGIT+
    ;

INTEGER
    : '-'? DIGIT+
    ;

QMARK
    : '?'
    ;

/*
 * Normally a lexer only emits one token at a time, but ours is tricked out
 * to support multiple (see @lexer::members near the top of the grammar).
 */
FLOAT
    : INTEGER EXPONENT
    | INTEGER '.' DIGIT* EXPONENT?
    ;

/*
 * This has to be before IDENT so it takes precendence over it.
 */
BOOLEAN
    : T R U E | F A L S E
    ;

IDENT
    : LETTER (LETTER | DIGIT | '_')*
    ;

HEXNUMBER
    : '0' X HEX*
    ;

UUID
    : HEX HEX HEX HEX HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX '-'
      HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX HEX
    ;

WS
    : (' ' | '\t' | '\n' | '\r')+ { $channel = HIDDEN; }
    ;

COMMENT
    : ('--' | '//') .* ('\n'|'\r') { $channel = HIDDEN; }
    ;

MULTILINE_COMMENT
    : '/*' .* '*/' { $channel = HIDDEN; }
    ;
