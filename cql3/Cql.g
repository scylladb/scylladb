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
    language = Cpp;
}

@parser::namespace{cql3_parser}

@lexer::includes {
#include "cql3/error_collector.hh"
#include "cql3/error_listener.hh"
}

@parser::includes {
#include "cql3/selection/writetime_or_ttl.hh"
#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/statements/alter_keyspace_statement.hh"
#include "cql3/statements/alter_table_statement.hh"
#include "cql3/statements/alter_view_statement.hh"
#include "cql3/statements/create_keyspace_statement.hh"
#include "cql3/statements/drop_keyspace_statement.hh"
#include "cql3/statements/create_index_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/create_view_statement.hh"
#include "cql3/statements/create_type_statement.hh"
#include "cql3/statements/drop_type_statement.hh"
#include "cql3/statements/alter_type_statement.hh"
#include "cql3/statements/property_definitions.hh"
#include "cql3/statements/drop_index_statement.hh"
#include "cql3/statements/drop_table_statement.hh"
#include "cql3/statements/drop_view_statement.hh"
#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/raw/update_statement.hh"
#include "cql3/statements/raw/insert_statement.hh"
#include "cql3/statements/raw/delete_statement.hh"
#include "cql3/statements/index_prop_defs.hh"
#include "cql3/statements/raw/use_statement.hh"
#include "cql3/statements/raw/batch_statement.hh"
#include "cql3/statements/list_users_statement.hh"
#include "cql3/statements/grant_statement.hh"
#include "cql3/statements/revoke_statement.hh"
#include "cql3/statements/list_permissions_statement.hh"
#include "cql3/statements/alter_role_statement.hh"
#include "cql3/statements/list_roles_statement.hh"
#include "cql3/statements/grant_role_statement.hh"
#include "cql3/statements/revoke_role_statement.hh"
#include "cql3/statements/drop_role_statement.hh"
#include "cql3/statements/create_role_statement.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/keyspace_element_name.hh"
#include "cql3/selection/selectable_with_field_selection.hh"
#include "cql3/constants.hh"
#include "cql3/operation_impl.hh"
#include "cql3/error_listener.hh"
#include "cql3/multi_column_relation.hh"
#include "cql3/single_column_relation.hh"
#include "cql3/token_relation.hh"
#include "cql3/index_name.hh"
#include "cql3/cql3_type.hh"
#include "cql3/cf_name.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/lists.hh"
#include "cql3/role_name.hh"
#include "cql3/role_options.hh"
#include "cql3/type_cast.hh"
#include "cql3/tuples.hh"
#include "cql3/user_types.hh"
#include "cql3/ut_name.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/functions/function_call.hh"
#include "core/sstring.hh"
#include "CqlLexer.hpp"

#include <algorithm>
#include <unordered_map>
#include <map>
}

@parser::traits {
using namespace cql3::statements;
using namespace cql3::selection;
using cql3::cql3_type;
using conditions_type = std::vector<std::pair<::shared_ptr<cql3::column_identifier::raw>,::shared_ptr<cql3::column_condition::raw>>>;
using operations_type = std::vector<std::pair<::shared_ptr<cql3::column_identifier::raw>,::shared_ptr<cql3::operation::raw_update>>>;

// ANTLR forces us to define a default-initialized return value
// for every rule (e.g. [returns ut_name name]), but not every type
// can be naturally zero-initialized.
//
// The uninitialized<T> wrapper can be zero-initialized, and is convertible
// to T (after checking that it was assigned to) implicitly, eliminating the
// problem.  It is up to the user to ensure it is actually assigned to. 
template <typename T>
struct uninitialized {
    std::experimental::optional<T> _val;
    uninitialized() = default;
    uninitialized(const uninitialized&) = default;
    uninitialized(uninitialized&&) = default;
    uninitialized(const T& val) : _val(val) {}
    uninitialized(T&& val) : _val(std::move(val)) {}
    uninitialized& operator=(const uninitialized&) = default;
    uninitialized& operator=(uninitialized&&) = default;
    operator const T&() const & { return check(), *_val; }
    operator T&&() && { return check(), std::move(*_val); }
    void check() const { if (!_val) { throw std::runtime_error("not intitialized"); } }
};

}

@context {
    using collector_type = cql3::error_collector<ComponentType, ExceptionBaseType::TokenType, ExceptionBaseType>;
    using listener_type = cql3::error_listener<ComponentType, ExceptionBaseType>;

    listener_type* listener;

    std::vector<::shared_ptr<cql3::column_identifier>> _bind_variables;
    std::vector<std::unique_ptr<TokenType>> _missing_tokens;

    // Can't use static variable, since it needs to be defined out-of-line
    static const std::unordered_set<sstring>& _reserved_type_names() {
        static std::unordered_set<sstring> s = {
            "byte",
            "smallint",
            "complex",
            "enum",
            "date",
            "interval",
            "macaddr",
            "bitstring",
        };
        return s;
    }

    shared_ptr<cql3::abstract_marker::raw> new_bind_variables(shared_ptr<cql3::column_identifier> name)
    {
        auto marker = make_shared<cql3::abstract_marker::raw>(_bind_variables.size());
        _bind_variables.push_back(name);
        return marker;
    }

    shared_ptr<cql3::abstract_marker::in_raw> new_in_bind_variables(shared_ptr<cql3::column_identifier> name) {
        auto marker = make_shared<cql3::abstract_marker::in_raw>(_bind_variables.size());
        _bind_variables.push_back(std::move(name));
        return marker;
    }

    shared_ptr<cql3::tuples::raw> new_tuple_bind_variables(shared_ptr<cql3::column_identifier> name)
    {
        auto marker = make_shared<cql3::tuples::raw>(_bind_variables.size());
        _bind_variables.push_back(std::move(name));
        return marker;
    }

    shared_ptr<cql3::tuples::in_raw> new_tuple_in_bind_variables(shared_ptr<cql3::column_identifier> name)
    {
        auto marker = make_shared<cql3::tuples::in_raw>(_bind_variables.size());
        _bind_variables.push_back(std::move(name));
        return marker;
    }

    void set_error_listener(listener_type& listener) {
        this->listener = &listener;
    }

    void displayRecognitionError(ANTLR_UINT8** token_names, ExceptionBaseType* ex)
    {
        listener->syntax_error(*this, token_names, ex);
    }

    void add_recognition_error(const sstring& msg) {
        listener->syntax_error(*this, msg);
    }

    bool is_eof_token(CommonTokenType token) const
    {
        return token == CommonTokenType::TOKEN_EOF;
    }

    std::string token_text(const TokenType* token)
    {
        if (!token) {
            return "";
        }
        return token->getText();
    }

    std::map<sstring, sstring> convert_property_map(shared_ptr<cql3::maps::literal> map) {
        if (!map || map->entries.empty()) {
            return std::map<sstring, sstring>{};
        }
        std::map<sstring, sstring> res;
        for (auto&& entry : map->entries) {
            // Because the parser tries to be smart and recover on error (to
            // allow displaying more than one error I suppose), we have null
            // entries in there. Just skip those, a proper error will be thrown in the end.
            if (!entry.first || !entry.second) {
                break;
            }
            auto left = dynamic_pointer_cast<cql3::constants::literal>(entry.first);
            if (!left) {
                sstring msg = "Invalid property name: " + entry.first->to_string();
                if (dynamic_pointer_cast<cql3::abstract_marker::raw>(entry.first)) {
                    msg += " (bind variables are not supported in DDL queries)";
                }
                add_recognition_error(msg);
                break;
            }
            auto right = dynamic_pointer_cast<cql3::constants::literal>(entry.second);
            if (!right) {
                sstring msg = "Invalid property value: " + entry.first->to_string() + " for property: " + entry.second->to_string();
                if (dynamic_pointer_cast<cql3::abstract_marker::raw>(entry.second)) {
                    msg += " (bind variables are not supported in DDL queries)";
                }
                add_recognition_error(msg);
                break;
            }
            res.emplace(left->get_raw_text(), right->get_raw_text());
        }
        return res;
    }

    bool convert_boolean_literal(stdx::string_view s) {
        std::string lower_s(s.size(), '\0');
        std::transform(s.cbegin(), s.cend(), lower_s.begin(), &::tolower);
        return lower_s == "true";
    }

    void add_raw_update(std::vector<std::pair<::shared_ptr<cql3::column_identifier::raw>,::shared_ptr<cql3::operation::raw_update>>>& operations,
        ::shared_ptr<cql3::column_identifier::raw> key, ::shared_ptr<cql3::operation::raw_update> update)
    {
        for (auto&& p : operations) {
            if (*p.first == *key && !p.second->is_compatible_with(update)) {
                // \%s is escaped for antlr
                add_recognition_error(sprint("Multiple incompatible setting of column \%s", *key));
            }
        }
        operations.emplace_back(std::move(key), std::move(update));
    }

    TokenType* getMissingSymbol(IntStreamType* istream, ExceptionBaseType* e,
                                ANTLR_UINT32 expectedTokenType, BitsetListType* follow) {
        auto token = BaseType::getMissingSymbol(istream, e, expectedTokenType, follow);
        _missing_tokens.emplace_back(token);
        return token;
    }
}

@lexer::namespace{cql3_parser}

@lexer::traits {
    class CqlLexer;
    class CqlParser;
    typedef antlr3::Traits<CqlLexer, CqlParser> CqlLexerTraits;
    typedef CqlLexerTraits CqlParserTraits;
}

@lexer::header {
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-function"
}

@lexer::context {
    using collector_type = cql3::error_collector<ComponentType, ExceptionBaseType::TokenType, ExceptionBaseType>;
    using listener_type = cql3::error_listener<ComponentType, ExceptionBaseType>;

    listener_type* listener;

    void set_error_listener(listener_type& listener) {
        this->listener = &listener;
    }

    void displayRecognitionError(ANTLR_UINT8** token_names, ExceptionBaseType* ex)
    {
        listener->syntax_error(*this, token_names, ex);
    }

    bool is_eof_token(CommonTokenType token) const
    {
        return token == CommonTokenType::TOKEN_EOF;
    }

    std::string token_text(const TokenType* token) const
    {
        if (!token) {
            return "";
        }
        return std::to_string(int(*token));
    }
}

/** STATEMENTS **/

query returns [shared_ptr<raw::parsed_statement> stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = st; }
    ;

cqlStatement returns [shared_ptr<raw::parsed_statement> stmt]
    @after{ if (stmt) { stmt->set_bound_variables(_bind_variables); } }
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
#if 0
    | st23=createTriggerStatement      { $stmt = st23; }
    | st24=dropTriggerStatement        { $stmt = st24; }
#endif
    | st25=createTypeStatement         { $stmt = st25; }
    | st26=alterTypeStatement          { $stmt = st26; }
    | st27=dropTypeStatement           { $stmt = st27; }
#if 0
    | st28=createFunctionStatement     { $stmt = st28; }
    | st29=dropFunctionStatement       { $stmt = st29; }
    | st30=createAggregateStatement    { $stmt = st30; }
    | st31=dropAggregateStatement      { $stmt = st31; }
#endif
    | st32=createViewStatement         { $stmt = st32; }
    | st33=alterViewStatement          { $stmt = st33; }
    | st34=dropViewStatement           { $stmt = st34; }
    | st35=listRolesStatement          { $stmt = st35; }
    | st36=grantRoleStatement          { $stmt = st36; }
    | st37=revokeRoleStatement         { $stmt = st37; }
    | st38=dropRoleStatement           { $stmt = st38; }
    | st39=createRoleStatement         { $stmt = st39; }
    | st40=alterRoleStatement          { $stmt = st40; }
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement returns [::shared_ptr<raw::use_statement> stmt]
    : K_USE ks=keyspaceName { $stmt = ::make_shared<raw::use_statement>(ks); }
    ;

/**
 * SELECT [JSON] <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>;
 */
selectStatement returns [shared_ptr<raw::select_statement> expr]
    @init {
        bool is_distinct = false;
        ::shared_ptr<cql3::term::raw> limit;
        raw::select_statement::parameters::orderings_type orderings;
        bool allow_filtering = false;
        bool is_json = false;
    }
    : K_SELECT (
                ( K_JSON { is_json = true; } )?
                ( K_DISTINCT { is_distinct = true; } )?
                sclause=selectClause
               )
      K_FROM cf=columnFamilyName
      ( K_WHERE wclause=whereClause )?
      ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
      ( K_LIMIT rows=intValue { limit = rows; } )?
      ( K_ALLOW K_FILTERING  { allow_filtering = true; } )?
      {
          auto params = ::make_shared<raw::select_statement::parameters>(std::move(orderings), is_distinct, allow_filtering, is_json);
          $expr = ::make_shared<raw::select_statement>(std::move(cf), std::move(params),
            std::move(sclause), std::move(wclause), std::move(limit));
      }
    ;

selectClause returns [std::vector<shared_ptr<raw_selector>> expr]
    : t1=selector { $expr.push_back(t1); } (',' tN=selector { $expr.push_back(tN); })*
    | '*' { }
    ;

selector returns [shared_ptr<raw_selector> s]
    @init{ shared_ptr<cql3::column_identifier> alias; }
    : us=unaliasedSelector (K_AS c=ident { alias = c; })? { $s = make_shared<raw_selector>(us, alias); }
    ;

unaliasedSelector returns [shared_ptr<selectable::raw> s]
    @init { shared_ptr<selectable::raw> tmp; }
    :  ( c=cident                                  { tmp = c; }
       | K_COUNT '(' countArgument ')'             { tmp = selectable::with_function::raw::make_count_rows_function(); }
       | K_WRITETIME '(' c=cident ')'              { tmp = make_shared<selectable::writetime_or_ttl::raw>(c, true); }
       | K_TTL       '(' c=cident ')'              { tmp = make_shared<selectable::writetime_or_ttl::raw>(c, false); }
       | f=functionName args=selectionFunctionArgs { tmp = ::make_shared<selectable::with_function::raw>(std::move(f), std::move(args)); }
       | K_CAST      '(' arg=unaliasedSelector K_AS t=native_type ')'  { tmp = ::make_shared<selectable::with_cast::raw>(std::move(arg), std::move(t)); }
       )
       ( '.' fi=cident { tmp = make_shared<selectable::with_field_selection::raw>(std::move(tmp), std::move(fi)); } )*
    { $s = tmp; }
    ;

selectionFunctionArgs returns [std::vector<shared_ptr<selectable::raw>> a]
    : '(' ')'
    | '(' s1=unaliasedSelector { a.push_back(std::move(s1)); }
          ( ',' sn=unaliasedSelector { a.push_back(std::move(sn)); } )*
      ')'
    ;

countArgument
    : '*'
    | i=INTEGER { if (i->getText() != "1") {
                    add_recognition_error("Only COUNT(1) is supported, got COUNT(" + i->getText() + ")");
                } }
    ;

whereClause returns [std::vector<cql3::relation_ptr> clause]
    : relation[$clause] (K_AND relation[$clause])*
    ;

orderByClause[raw::select_statement::parameters::orderings_type& orderings]
    @init{
        bool reversed = false;
    }
    : c=cident (K_ASC | K_DESC { reversed = true; })? { orderings.emplace_back(c, reversed); }
    ;

jsonValue returns [::shared_ptr<cql3::term::raw> value]
    :
    | s=STRING_LITERAL { $value = cql3::constants::literal::string(sstring{$s.text}); }
    | ':' id=ident     { $value = new_bind_variables(id); }
    | QMARK            { $value = new_bind_variables(shared_ptr<cql3::column_identifier>{}); }
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement returns [::shared_ptr<raw::modification_statement> expr]
    @init {
        auto attrs = ::make_shared<cql3::attributes::raw>();
        std::vector<::shared_ptr<cql3::column_identifier::raw>> column_names;
        std::vector<::shared_ptr<cql3::term::raw>> values;
        bool if_not_exists = false;
        bool default_unset = false;
        ::shared_ptr<cql3::term::raw> json_value;
    }
    : K_INSERT K_INTO cf=columnFamilyName
        ('(' c1=cident { column_names.push_back(c1); }  ( ',' cn=cident { column_names.push_back(cn); } )* ')'
            K_VALUES
            '(' v1=term { values.push_back(v1); } ( ',' vn=term { values.push_back(vn); } )* ')'
            ( K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
            ( usingClause[attrs] )?
              {
              $expr = ::make_shared<raw::insert_statement>(std::move(cf),
                                                       std::move(attrs),
                                                       std::move(column_names),
                                                       std::move(values),
                                                       if_not_exists);
              }
        | K_JSON
          json_token=jsonValue { json_value = $json_token.value; }
            ( K_DEFAULT K_UNSET { default_unset = true; } | K_DEFAULT K_NULL )?
            ( K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
            ( usingClause[attrs] )?
              {
              $expr = ::make_shared<raw::insert_json_statement>(std::move(cf),
                                                       std::move(attrs),
                                                       std::move(json_value),
                                                       if_not_exists,
                                                       default_unset);
              }
        )
    ;

usingClause[::shared_ptr<cql3::attributes::raw> attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
    ;

usingClauseObjective[::shared_ptr<cql3::attributes::raw> attrs]
    : K_TIMESTAMP ts=intValue { attrs->timestamp = ts; }
    | K_TTL t=intValue { attrs->time_to_live = t; }
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 */
updateStatement returns [::shared_ptr<raw::update_statement> expr]
    @init {
        auto attrs = ::make_shared<cql3::attributes::raw>();
        std::vector<std::pair<::shared_ptr<cql3::column_identifier::raw>, ::shared_ptr<cql3::operation::raw_update>>> operations;
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET columnOperation[operations] (',' columnOperation[operations])*
      K_WHERE wclause=whereClause
      ( K_IF conditions=updateConditions )?
      {
          return ::make_shared<raw::update_statement>(std::move(cf),
                                                  std::move(attrs),
                                                  std::move(operations),
                                                  std::move(wclause),
                                                  std::move(conditions));
     }
    ;

updateConditions returns [conditions_type conditions]
    : columnCondition[conditions] ( K_AND columnCondition[conditions] )*
    ;

/**
 * DELETE name1, name2
 * FROM <CF>
 * USING TIMESTAMP <long>
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement returns [::shared_ptr<raw::delete_statement> expr]
    @init {
        auto attrs = ::make_shared<cql3::attributes::raw>();
        std::vector<::shared_ptr<cql3::operation::raw_deletion>> column_deletions;
        bool if_exists = false;
    }
    : K_DELETE ( dels=deleteSelection { column_deletions = std::move(dels); } )?
      K_FROM cf=columnFamilyName
      ( usingClauseDelete[attrs] )?
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { if_exists = true; } | conditions=updateConditions ))?
      {
          return ::make_shared<raw::delete_statement>(cf,
                                            std::move(attrs),
                                            std::move(column_deletions),
                                            std::move(wclause),
                                            std::move(conditions),
                                            if_exists);
      }
    ;

deleteSelection returns [std::vector<::shared_ptr<cql3::operation::raw_deletion>> operations]
    : t1=deleteOp { $operations.emplace_back(std::move(t1)); }
      (',' tN=deleteOp { $operations.emplace_back(std::move(tN)); })*
    ;

deleteOp returns [::shared_ptr<cql3::operation::raw_deletion> op]
    : c=cident                { $op = ::make_shared<cql3::operation::column_deletion>(std::move(c)); }
    | c=cident '[' t=term ']' { $op = ::make_shared<cql3::operation::element_deletion>(std::move(c), std::move(t)); }
    ;

usingClauseDelete[::shared_ptr<cql3::attributes::raw> attrs]
    : K_USING K_TIMESTAMP ts=intValue { attrs->timestamp = ts; }
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
batchStatement returns [shared_ptr<cql3::statements::raw::batch_statement> expr]
    @init {
        using btype = cql3::statements::raw::batch_statement::type; 
        btype type = btype::LOGGED;
        std::vector<shared_ptr<cql3::statements::raw::modification_statement>> statements;
        auto attrs = make_shared<cql3::attributes::raw>();
    }
    : K_BEGIN
      ( K_UNLOGGED { type = btype::UNLOGGED; } | K_COUNTER { type = btype::COUNTER; } )?
      K_BATCH ( usingClause[attrs] )?
          ( s=batchStatementObjective ';'? { statements.push_back(std::move(s)); } )*
      K_APPLY K_BATCH
      {
          $expr = ::make_shared<cql3::statements::raw::batch_statement>(type, std::move(attrs), std::move(statements));
      }
    ;

batchStatementObjective returns [shared_ptr<cql3::statements::raw::modification_statement> statement]
    : i=insertStatement  { $statement = i; }
    | u=updateStatement  { $statement = u; }
    | d=deleteStatement  { $statement = d; }
    ;

#if 0
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
#endif

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [shared_ptr<cql3::statements::create_keyspace_statement> expr]
    @init {
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
        bool if_not_exists = false;
    }
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS { if_not_exists = true; } )? ks=keyspaceName
      K_WITH properties[attrs] { $expr = make_shared<cql3::statements::create_keyspace_statement>(ks, attrs, if_not_exists); }
    ;

/**
 * CREATE COLUMNFAMILY [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement returns [shared_ptr<cql3::statements::create_table_statement::raw_statement> expr]
    @init { bool if_not_exists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
      cf=columnFamilyName { $expr = make_shared<cql3::statements::create_table_statement::raw_statement>(cf, if_not_exists); }
      cfamDefinition[expr]
    ;

cfamDefinition[shared_ptr<cql3::statements::create_table_statement::raw_statement> expr]
    : '(' cfamColumns[expr] ( ',' cfamColumns[expr]? )* ')'
      ( K_WITH cfamProperty[$expr->properties()] ( K_AND cfamProperty[$expr->properties()] )*)?
    ;

cfamColumns[shared_ptr<cql3::statements::create_table_statement::raw_statement> expr]
    @init { bool is_static=false; }
    : k=ident v=comparatorType (K_STATIC {is_static = true;})? { $expr->add_definition(k, v, is_static); }
        (K_PRIMARY K_KEY { $expr->add_key_aliases(std::vector<shared_ptr<cql3::column_identifier>>{k}); })?
    | K_PRIMARY K_KEY '(' pkDef[expr] (',' c=ident { $expr->add_column_alias(c); } )* ')'
    ;

pkDef[shared_ptr<cql3::statements::create_table_statement::raw_statement> expr]
    @init { std::vector<shared_ptr<cql3::column_identifier>> l; }
    : k=ident { $expr->add_key_aliases(std::vector<shared_ptr<cql3::column_identifier>>{k}); }
    | '(' k1=ident { l.push_back(k1); } ( ',' kn=ident { l.push_back(kn); } )* ')' { $expr->add_key_aliases(l); }
    ;

cfamProperty[cql3::statements::cf_properties& expr]
    : property[$expr.properties()]
    | K_COMPACT K_STORAGE { $expr.set_compact_storage(); }
    | K_CLUSTERING K_ORDER K_BY '(' cfamOrdering[expr] (',' cfamOrdering[expr])* ')'
    ;

cfamOrdering[cql3::statements::cf_properties& expr]
    @init{ bool reversed=false; }
    : k=ident (K_ASC | K_DESC { reversed=true;} ) { $expr.set_ordering(k, reversed); }
    ;


/**
 * CREATE TYPE foo (
 *    <name1> <type1>,
 *    <name2> <type2>,
 *    ....
 * )
 */
createTypeStatement returns [::shared_ptr<create_type_statement> expr]
    @init { bool if_not_exists = false; }
    : K_CREATE K_TYPE (K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
         tn=userTypeName { $expr = ::make_shared<create_type_statement>(tn, if_not_exists); }
         '(' typeColumns[expr] ( ',' typeColumns[expr]? )* ')'
    ;

typeColumns[::shared_ptr<create_type_statement> expr]
    : k=ident v=comparatorType { $expr->add_definition(k, v); }
    ;


/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement returns [::shared_ptr<create_index_statement> expr]
    @init {
        auto props = make_shared<index_prop_defs>();
        bool if_not_exists = false;
        auto name = ::make_shared<cql3::index_name>();
        std::vector<::shared_ptr<index_target::raw>> targets;
    }
    : K_CREATE (K_CUSTOM { props->is_custom = true; })? K_INDEX (K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
        (idxName[name])? K_ON cf=columnFamilyName '(' (target1=indexIdent { targets.emplace_back(target1); } (',' target2=indexIdent { targets.emplace_back(target2); } )*)? ')'
        (K_USING cls=STRING_LITERAL { props->custom_class = sstring{$cls.text}; })?
        (K_WITH properties[props])?
      { $expr = ::make_shared<create_index_statement>(cf, name, targets, props, if_not_exists); }
    ;

indexIdent returns [::shared_ptr<index_target::raw> id]
    : c=cident                   { $id = index_target::raw::values_of(c); }
    | K_KEYS '(' c=cident ')'    { $id = index_target::raw::keys_of(c); }
    | K_ENTRIES '(' c=cident ')' { $id = index_target::raw::keys_and_values_of(c); }
    | K_FULL '(' c=cident ')'    { $id = index_target::raw::full_collection(c); }
    ;

/**
 * CREATE MATERIALIZED VIEW <viewName> AS
 *  SELECT <columns>
 *  FROM <CF>
 *  WHERE <pkColumns> IS NOT NULL
 *  PRIMARY KEY (<pkColumns>)
 *  WITH <property> = <value> AND ...;
 */
createViewStatement returns [::shared_ptr<create_view_statement> expr]
    @init {
        bool if_not_exists = false;
        std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys;
        std::vector<::shared_ptr<cql3::column_identifier::raw>> composite_keys;
    }
    : K_CREATE K_MATERIALIZED K_VIEW (K_IF K_NOT K_EXISTS { if_not_exists = true; })? cf=columnFamilyName K_AS
        K_SELECT sclause=selectClause K_FROM basecf=columnFamilyName
        (K_WHERE wclause=whereClause)?
        K_PRIMARY K_KEY (
        '(' '(' k1=cident { partition_keys.push_back(k1); } ( ',' kn=cident { partition_keys.push_back(kn); } )* ')' ( ',' c1=cident { composite_keys.push_back(c1); } )* ')'
    |   '(' k1=cident { partition_keys.push_back(k1); } ( ',' cn=cident { composite_keys.push_back(cn); } )* ')'
        )
        {
             $expr = ::make_shared<create_view_statement>(
                std::move(cf),
                std::move(basecf),
                std::move(sclause),
                std::move(wclause),
                std::move(partition_keys),
                std::move(composite_keys),
                if_not_exists);
        }
        ( K_WITH cfamProperty[{ $expr->properties() }] ( K_AND cfamProperty[{ $expr->properties() }] )*)?
    ;

#if 0
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

#endif

/**
 * ALTER KEYSPACE <KS> WITH <property> = <value>;
 */
alterKeyspaceStatement returns [shared_ptr<cql3::statements::alter_keyspace_statement> expr]
    @init {
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
    }
    : K_ALTER K_KEYSPACE ks=keyspaceName
        K_WITH properties[attrs] { $expr = make_shared<cql3::statements::alter_keyspace_statement>(ks, attrs); }
    ;

/**
 * ALTER COLUMN FAMILY <CF> ALTER <column> TYPE <newtype>;
 * ALTER COLUMN FAMILY <CF> ADD <column> <newtype>;
 * ALTER COLUMN FAMILY <CF> DROP <column>;
 * ALTER COLUMN FAMILY <CF> WITH <property> = <value>;
 * ALTER COLUMN FAMILY <CF> RENAME <column> TO <column>;
 */
alterTableStatement returns [shared_ptr<alter_table_statement> expr]
    @init {
        alter_table_statement::type type;
        auto props = make_shared<cql3::statements::cf_prop_defs>();
        std::vector<std::pair<shared_ptr<cql3::column_identifier::raw>, shared_ptr<cql3::column_identifier::raw>>> renames;
        bool is_static = false;
    }
    : K_ALTER K_COLUMNFAMILY cf=columnFamilyName
          ( K_ALTER id=cident K_TYPE v=comparatorType { type = alter_table_statement::type::alter; }
          | K_ADD   id=cident v=comparatorType ({ is_static=true; } K_STATIC)? { type = alter_table_statement::type::add; }
          | K_DROP  id=cident                         { type = alter_table_statement::type::drop; }
          | K_WITH  properties[props]                 { type = alter_table_statement::type::opts; }
          | K_RENAME                                  { type = alter_table_statement::type::rename; }
               id1=cident K_TO toId1=cident { renames.emplace_back(id1, toId1); }
               ( K_AND idn=cident K_TO toIdn=cident { renames.emplace_back(idn, toIdn); } )*
          )
    {
        $expr = ::make_shared<alter_table_statement>(std::move(cf), type, std::move(id),
            std::move(v), std::move(props), std::move(renames), is_static);
    }
    ;

/**
 * ALTER TYPE <name> ALTER <field> TYPE <newtype>;
 * ALTER TYPE <name> ADD <field> <newtype>;
 * ALTER TYPE <name> RENAME <field> TO <newtype> AND ...;
 */
alterTypeStatement returns [::shared_ptr<alter_type_statement> expr]
    : K_ALTER K_TYPE name=userTypeName
          ( K_ALTER f=ident K_TYPE v=comparatorType { $expr = ::make_shared<alter_type_statement::add_or_alter>(name, false, f, v); }
          | K_ADD   f=ident v=comparatorType        { $expr = ::make_shared<alter_type_statement::add_or_alter>(name, true, f, v); }
          | K_RENAME
               { $expr = ::make_shared<alter_type_statement::renames>(name); }
               renames[{ static_pointer_cast<alter_type_statement::renames>($expr) }]
          )
    ;

/**
 * ALTER MATERIALIZED VIEW <CF> WITH <property> = <value>;
 */
alterViewStatement returns [::shared_ptr<alter_view_statement> expr]
    @init {
        auto props = make_shared<cql3::statements::cf_prop_defs>();
    }
    : K_ALTER K_MATERIALIZED K_VIEW cf=columnFamilyName K_WITH properties[props]
    {
        $expr = ::make_shared<alter_view_statement>(std::move(cf), std::move(props));
    }
    ;

renames[::shared_ptr<alter_type_statement::renames> expr]
    : fromId=ident K_TO toId=ident { $expr->add_rename(fromId, toId); }
      ( K_AND renames[$expr] )?
    ;

/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement returns [::shared_ptr<drop_keyspace_statement> ksp]
    @init { bool if_exists = false; }
    : K_DROP K_KEYSPACE (K_IF K_EXISTS { if_exists = true; } )? ks=keyspaceName { $ksp = ::make_shared<drop_keyspace_statement>(ks, if_exists); }
    ;

/**
 * DROP COLUMNFAMILY [IF EXISTS] <CF>;
 */
dropTableStatement returns [::shared_ptr<drop_table_statement> stmt]
    @init { bool if_exists = false; }
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS { if_exists = true; } )? cf=columnFamilyName { $stmt = ::make_shared<drop_table_statement>(cf, if_exists); }
    ;

/**
 * DROP TYPE <name>;
 */
dropTypeStatement returns [::shared_ptr<drop_type_statement> stmt]
    @init { bool if_exists = false; }
    : K_DROP K_TYPE (K_IF K_EXISTS { if_exists = true; } )? name=userTypeName { $stmt = ::make_shared<drop_type_statement>(name, if_exists); }
    ;

/**
 * DROP MATERIALIZED VIEW [IF EXISTS] <view_name>
 */
dropViewStatement returns [::shared_ptr<drop_view_statement> stmt]
    @init { bool if_exists = false; }
    : K_DROP K_MATERIALIZED K_VIEW (K_IF K_EXISTS { if_exists = true; } )? cf=columnFamilyName
      { $stmt = ::make_shared<drop_view_statement>(cf, if_exists); }
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement returns [::shared_ptr<drop_index_statement> expr]
    @init { bool if_exists = false; }
    : K_DROP K_INDEX (K_IF K_EXISTS { if_exists = true; } )? index=indexName
      { $expr = ::make_shared<drop_index_statement>(index, if_exists); }
    ;

/**
  * TRUNCATE <CF>;
  */
truncateStatement returns [::shared_ptr<truncate_statement> stmt]
    : K_TRUNCATE (K_COLUMNFAMILY)? cf=columnFamilyName { $stmt = ::make_shared<truncate_statement>(cf); }
    ;

/**
 * GRANT <permission> ON <resource> TO <grantee>
 */
grantStatement returns [::shared_ptr<grant_statement> stmt]
    : K_GRANT
          permissionOrAll
      K_ON
          resource
      K_TO
          grantee=userOrRoleName
      { $stmt = ::make_shared<grant_statement>($permissionOrAll.perms, $resource.res, std::move(grantee)); } 
    ;

/**
 * REVOKE <permission> ON <resource> FROM <revokee>
 */
revokeStatement returns [::shared_ptr<revoke_statement> stmt]
    : K_REVOKE
          permissionOrAll
      K_ON
          resource
      K_FROM
          revokee=userOrRoleName
      { $stmt = ::make_shared<revoke_statement>($permissionOrAll.perms, $resource.res, std::move(revokee)); } 
    ;

/**
 * GRANT <rolename> to <grantee>
 */
grantRoleStatement returns [::shared_ptr<grant_role_statement> stmt]
    : K_GRANT role=userOrRoleName K_TO grantee=userOrRoleName
      { $stmt = ::make_shared<grant_role_statement>(std::move(role), std::move(grantee));  }
    ;

/**
 * REVOKE <rolename> FROM <revokee>
 */
revokeRoleStatement returns [::shared_ptr<revoke_role_statement> stmt]
    : K_REVOKE role=userOrRoleName K_FROM revokee=userOrRoleName
      { $stmt = ::make_shared<revoke_role_statement>(std::move(role), std::move(revokee)); }
    ;

listPermissionsStatement returns [::shared_ptr<list_permissions_statement> stmt]
    @init {
		std::optional<auth::resource> r;
		std::optional<sstring> role;
		bool recursive = true;
    }
    : K_LIST
          permissionOrAll
      ( K_ON resource { r = $resource.res; } )?
      ( K_OF rn=userOrRoleName { role = sstring(static_cast<cql3::role_name>(rn).to_string()); } )?
      ( K_NORECURSIVE { recursive = false; } )?
      { $stmt = ::make_shared<list_permissions_statement>($permissionOrAll.perms, std::move(r), std::move(role), recursive); } 
    ;

permission returns [auth::permission perm]
    : p=(K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE | K_DESCRIBE)
    { $perm = auth::permissions::from_string($p.text); }
    ;

permissionOrAll returns [auth::permission_set perms]
    : K_ALL ( K_PERMISSIONS )?       { $perms = auth::permissions::ALL; }
    | p=permission ( K_PERMISSION )? { $perms = auth::permission_set::from_mask(auth::permission_set::mask_for($p.perm)); }
    ;

resource returns [uninitialized<auth::resource> res]
    : d=dataResource { $res = std::move(d); }
    | r=roleResource { $res = std::move(r); }
    ;

dataResource returns [uninitialized<auth::resource> res]
    : K_ALL K_KEYSPACES { $res = auth::resource(auth::resource_kind::data); }
    | K_KEYSPACE ks = keyspaceName { $res = auth::make_data_resource($ks.id); }
    | ( K_COLUMNFAMILY )? cf = columnFamilyName
      { $res = auth::make_data_resource($cf.name->get_keyspace(), $cf.name->get_column_family()); }
    ;

roleResource returns [uninitialized<auth::resource> res]
    : K_ALL K_ROLES { $res = auth::resource(auth::resource_kind::role); }
    | K_ROLE role = userOrRoleName { $res = auth::make_role_resource(static_cast<const cql3::role_name&>(role).to_string()); }
    ;

/**
 * CREATE USER [IF NOT EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement returns [::shared_ptr<create_role_statement> stmt]
    @init {
        cql3::role_options opts;
        opts.is_superuser = false;
        opts.can_login = true;

        bool ifNotExists = false;
    }
    : K_CREATE K_USER (K_IF K_NOT K_EXISTS { ifNotExists = true; })? username
      ( K_WITH K_PASSWORD v=STRING_LITERAL { opts.password = $v.text; })?
      ( K_SUPERUSER { opts.is_superuser = true; } | K_NOSUPERUSER { opts.is_superuser = false; } )?
      { $stmt = ::make_shared<create_role_statement>(cql3::role_name($username.text, cql3::preserve_role_case::yes), std::move(opts), ifNotExists); }
    ;

/**
 * ALTER USER <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement returns [::shared_ptr<alter_role_statement> stmt]
    @init {
        cql3::role_options opts;
    }
    : K_ALTER K_USER username
      ( K_WITH K_PASSWORD v=STRING_LITERAL { opts.password = $v.text; })?
      ( K_SUPERUSER { opts.is_superuser = true; } | K_NOSUPERUSER { opts.is_superuser = false; } )?
      { $stmt = ::make_shared<alter_role_statement>(cql3::role_name($username.text, cql3::preserve_role_case::yes), std::move(opts)); }
    ;

/**
 * DROP USER [IF EXISTS] <username>
 */
dropUserStatement returns [::shared_ptr<drop_role_statement> stmt]
    @init { bool ifExists = false; }
    : K_DROP K_USER (K_IF K_EXISTS { ifExists = true; })? username
      { $stmt = ::make_shared<drop_role_statement>(cql3::role_name($username.text, cql3::preserve_role_case::yes), ifExists); }
    ;

/**
 * LIST USERS
 */
listUsersStatement returns [::shared_ptr<list_users_statement> stmt]
    : K_LIST K_USERS { $stmt = ::make_shared<list_users_statement>(); }
    ;

/**
 * CREATE ROLE [IF NOT EXISTS] <role_name> [WITH <roleOption> [AND <roleOption>]*]
 */
createRoleStatement returns [::shared_ptr<create_role_statement> stmt]
    @init {
        cql3::role_options opts;
        opts.is_superuser = false;
        opts.can_login = false;
        bool if_not_exists = false;
    }
    : K_CREATE K_ROLE (K_IF K_NOT K_EXISTS { if_not_exists = true; })? name=userOrRoleName
      (K_WITH roleOptions[opts])?
      { $stmt = ::make_shared<create_role_statement>(name, std::move(opts), if_not_exists); }
    ;

/**
 * ALTER ROLE <rolename> [WITH <roleOption> [AND <roleOption>]*]
 */
alterRoleStatement returns [::shared_ptr<alter_role_statement> stmt]
    @init {
        cql3::role_options opts;
    }
    : K_ALTER K_ROLE name=userOrRoleName
      (K_WITH roleOptions[opts])?
      { $stmt = ::make_shared<alter_role_statement>(name, std::move(opts)); }
    ;

/**
 * DROP ROLE [IF EXISTS] <rolename>
 */
dropRoleStatement returns [::shared_ptr<drop_role_statement> stmt]
    @init {
        bool if_exists = false;
    }
    : K_DROP K_ROLE (K_IF K_EXISTS { if_exists = true; })? name=userOrRoleName
      { $stmt = ::make_shared<drop_role_statement>(name, if_exists); }
    ;

/**
 * LIST ROLES [OF <rolename>] [NORECURSIVE]
 */
listRolesStatement returns [::shared_ptr<list_roles_statement> stmt]
    @init {
        bool recursive = true;
        std::optional<cql3::role_name> grantee;
    }
    : K_LIST K_ROLES
        (K_OF g=userOrRoleName { grantee = std::move(g); })?
        (K_NORECURSIVE { recursive = false; })?
        { $stmt = ::make_shared<list_roles_statement>(grantee, recursive); }
    ;

roleOptions[cql3::role_options& opts]
    : roleOption[opts] (K_AND roleOption[opts])*
    ;

roleOption[cql3::role_options& opts]
    : K_PASSWORD '=' v=STRING_LITERAL { opts.password = $v.text; }
    | K_OPTIONS '=' m=mapLiteral { opts.options = convert_property_map(m); }
    | K_SUPERUSER '=' b=BOOLEAN { opts.is_superuser = convert_boolean_literal($b.text); }
    | K_LOGIN '=' b=BOOLEAN { opts.can_login = convert_boolean_literal($b.text); }
    ;

/** DEFINITIONS **/

// Column Identifiers.  These need to be treated differently from other
// identifiers because the underlying comparator is not necessarily text. See
// CASSANDRA-8178 for details.
cident returns [shared_ptr<cql3::column_identifier::raw> id]
    : t=IDENT              { $id = make_shared<cql3::column_identifier::raw>(sstring{$t.text}, false); }
    | t=QUOTED_NAME        { $id = make_shared<cql3::column_identifier::raw>(sstring{$t.text}, true); }
    | k=unreserved_keyword { $id = make_shared<cql3::column_identifier::raw>(k, false); }
    ;

// Identifiers that do not refer to columns or where the comparator is known to be text
ident returns [shared_ptr<cql3::column_identifier> id]
    : t=IDENT              { $id = make_shared<cql3::column_identifier>(sstring{$t.text}, false); }
    | t=QUOTED_NAME        { $id = make_shared<cql3::column_identifier>(sstring{$t.text}, true); }
    | k=unreserved_keyword { $id = make_shared<cql3::column_identifier>(k, false); }
    ;

// Keyspace & Column family names
keyspaceName returns [sstring id]
    @init { auto name = make_shared<cql3::cf_name>(); }
    : ksName[name] { $id = name->get_keyspace(); }
    ;

indexName returns [::shared_ptr<cql3::index_name> name]
    @init { $name = ::make_shared<cql3::index_name>(); }
    : (ksName[name] '.')? idxName[name]
    ;

columnFamilyName returns [::shared_ptr<cql3::cf_name> name]
    @init { $name = ::make_shared<cql3::cf_name>(); }
    : (ksName[name] '.')? cfName[name]
    ;

userTypeName returns [uninitialized<cql3::ut_name> name]
    : (ks=ident '.')? ut=non_type_ident { $name = cql3::ut_name(ks, ut); }
    ;

userOrRoleName returns [uninitialized<cql3::role_name> name]
    : t=IDENT              { $name = cql3::role_name($t.text, cql3::preserve_role_case::no); }
    | t=STRING_LITERAL     { $name = cql3::role_name($t.text, cql3::preserve_role_case::yes); }
    | t=QUOTED_NAME        { $name = cql3::role_name($t.text, cql3::preserve_role_case::yes); }
    | k=unreserved_keyword { $name = cql3::role_name(k, cql3::preserve_role_case::no); }
    | QMARK {add_recognition_error("Bind variables cannot be used for role names");}
    ;

ksName[::shared_ptr<cql3::keyspace_element_name> name]
    : t=IDENT              { $name->set_keyspace($t.text, false);}
    | t=QUOTED_NAME        { $name->set_keyspace($t.text, true);}
    | k=unreserved_keyword { $name->set_keyspace(k, false);}
    | QMARK {add_recognition_error("Bind variables cannot be used for keyspace names");}
    ;

cfName[::shared_ptr<cql3::cf_name> name]
    : t=IDENT              { $name->set_column_family($t.text, false); }
    | t=QUOTED_NAME        { $name->set_column_family($t.text, true); }
    | k=unreserved_keyword { $name->set_column_family(k, false); }
    | QMARK {add_recognition_error("Bind variables cannot be used for table names");}
    ;

idxName[::shared_ptr<cql3::index_name> name]
    : t=IDENT              { $name->set_index($t.text, false); }
    | t=QUOTED_NAME        { $name->set_index($t.text, true);}
    | k=unreserved_keyword { $name->set_index(k, false); }
    | QMARK {add_recognition_error("Bind variables cannot be used for index names");}
    ;

constant returns [shared_ptr<cql3::constants::literal> constant]
    @init{std::string sign;}
    : t=STRING_LITERAL { $constant = cql3::constants::literal::string(sstring{$t.text}); }
    | t=INTEGER        { $constant = cql3::constants::literal::integer(sstring{$t.text}); }
    | t=FLOAT          { $constant = cql3::constants::literal::floating_point(sstring{$t.text}); }
    | t=BOOLEAN        { $constant = cql3::constants::literal::bool_(sstring{$t.text}); }
    | t=DURATION       { $constant = cql3::constants::literal::duration(sstring{$t.text}); }
    | t=UUID           { $constant = cql3::constants::literal::uuid(sstring{$t.text}); }
    | t=HEXNUMBER      { $constant = cql3::constants::literal::hex(sstring{$t.text}); }
    | { sign=""; } ('-' {sign = "-"; } )? t=(K_NAN | K_INFINITY) { $constant = cql3::constants::literal::floating_point(sstring{sign + $t.text}); }
    ;

mapLiteral returns [shared_ptr<cql3::maps::literal> map]
    @init{std::vector<std::pair<::shared_ptr<cql3::term::raw>, ::shared_ptr<cql3::term::raw>>> m;}
    : '{' { }
          ( k1=term ':' v1=term { m.push_back(std::pair<shared_ptr<cql3::term::raw>, shared_ptr<cql3::term::raw>>{k1, v1}); } ( ',' kn=term ':' vn=term { m.push_back(std::pair<shared_ptr<cql3::term::raw>, shared_ptr<cql3::term::raw>>{kn, vn}); } )* )?
      '}' { $map = ::make_shared<cql3::maps::literal>(m); }
    ;

setOrMapLiteral[shared_ptr<cql3::term::raw> t] returns [shared_ptr<cql3::term::raw> value]
	@init{ std::vector<std::pair<shared_ptr<cql3::term::raw>, shared_ptr<cql3::term::raw>>> m;
	       std::vector<shared_ptr<cql3::term::raw>> s;
	}
    : ':' v=term { m.push_back({t, v}); }
          ( ',' kn=term ':' vn=term { m.push_back({kn, vn}); } )*
      { $value = ::make_shared<cql3::maps::literal>(std::move(m)); }
    | { s.push_back(t); }
          ( ',' tn=term { s.push_back(tn); } )*
      { $value = make_shared(cql3::sets::literal(std::move(s))); }
    ;

collectionLiteral returns [shared_ptr<cql3::term::raw> value]
	@init{ std::vector<shared_ptr<cql3::term::raw>> l; }
    : '['
          ( t1=term { l.push_back(t1); } ( ',' tn=term { l.push_back(tn); } )* )?
      ']' { $value = ::make_shared<cql3::lists::literal>(std::move(l)); }
    | '{' t=term v=setOrMapLiteral[t] { $value = v; } '}'
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}' { $value = make_shared(cql3::sets::literal({})); }
    ;

usertypeLiteral returns [shared_ptr<cql3::user_types::literal> ut]
    @init{ cql3::user_types::literal::elements_map_type m; }
    @after{ $ut = ::make_shared<cql3::user_types::literal>(std::move(m)); }
    // We don't allow empty literals because that conflicts with sets/maps and is currently useless since we don't allow empty user types
    : '{' k1=ident ':' v1=term { m.emplace(std::move(*k1), std::move(v1)); } ( ',' kn=ident ':' vn=term { m.emplace(std::move(*kn), std::move(vn)); } )* '}'
    ;

tupleLiteral returns [shared_ptr<cql3::tuples::literal> tt]
    @init{ std::vector<shared_ptr<cql3::term::raw>> l; }
    @after{ $tt = ::make_shared<cql3::tuples::literal>(std::move(l)); }
    : '(' t1=term { l.push_back(t1); } ( ',' tn=term { l.push_back(tn); } )* ')'
    ;

value returns [::shared_ptr<cql3::term::raw> value]
    : c=constant           { $value = c; }
    | l=collectionLiteral  { $value = l; }
    | u=usertypeLiteral    { $value = u; }
    | t=tupleLiteral       { $value = t; }
    | K_NULL               { $value = cql3::constants::NULL_LITERAL; }
    | ':' id=ident         { $value = new_bind_variables(id); }
    | QMARK                { $value = new_bind_variables(shared_ptr<cql3::column_identifier>{}); }
    ;

intValue returns [::shared_ptr<cql3::term::raw> value]
    :
    | t=INTEGER     { $value = cql3::constants::literal::integer(sstring{$t.text}); }
    | ':' id=ident  { $value = new_bind_variables(id); }
    | QMARK         { $value = new_bind_variables(shared_ptr<cql3::column_identifier>{}); }
    ;

functionName returns [cql3::functions::function_name s]
    : (ks=keyspaceName '.')? f=allowedFunctionName   { $s.keyspace = std::move(ks); $s.name = std::move(f); }
    ;

allowedFunctionName returns [sstring s]
    : f=IDENT                       { $s = $f.text; std::transform(s.begin(), s.end(), s.begin(), ::tolower); }
    | f=QUOTED_NAME                 { $s = $f.text; }
    | u=unreserved_function_keyword { $s = u; }
    | K_TOKEN                       { $s = "token"; }
    | K_COUNT                       { $s = "count"; }
    ;

functionArgs returns [std::vector<shared_ptr<cql3::term::raw>> a]
    : '(' ')'
    | '(' t1=term { a.push_back(std::move(t1)); }
          ( ',' tn=term { a.push_back(std::move(tn)); } )*
       ')'
    ;

term returns [::shared_ptr<cql3::term::raw> term1]
    : v=value                          { $term1 = v; }
    | f=functionName args=functionArgs { $term1 = ::make_shared<cql3::functions::function_call::raw>(std::move(f), std::move(args)); }
    | '(' c=comparatorType ')' t=term  { $term1 = make_shared<cql3::type_cast>(c, t); }
    ;

columnOperation[operations_type& operations]
    : key=cident columnOperationDifferentiator[operations, key]
    ;

columnOperationDifferentiator[operations_type& operations, ::shared_ptr<cql3::column_identifier::raw> key]
    : '=' normalColumnOperation[operations, key]
    | '[' k=term ']' specializedColumnOperation[operations, key, k, false]
    | '[' K_SCYLLA_TIMEUUID_LIST_INDEX '(' k=term ')' ']' specializedColumnOperation[operations, key, k, true]
    ;

normalColumnOperation[operations_type& operations, ::shared_ptr<cql3::column_identifier::raw> key]
    : t=term ('+' c=cident )?
      {
          if (!c) {
              add_raw_update(operations, key, ::make_shared<cql3::operation::set_value>(t));
          } else {
              if (*key != *c) {
                add_recognition_error("Only expressions of the form X = <value> + X are supported.");
              }
              add_raw_update(operations, key, ::make_shared<cql3::operation::prepend>(t));
          }
      }
    | c=cident sig=('+' | '-') t=term
      {
          if (*key != *c) {
              add_recognition_error("Only expressions of the form X = X " + $sig.text + "<value> are supported.");
          }
          shared_ptr<cql3::operation::raw_update> op;
          if ($sig.text == "+") {
              op = make_shared<cql3::operation::addition>(t);
          } else {
              op = make_shared<cql3::operation::subtraction>(t);
          }
          add_raw_update(operations, key, std::move(op));
      }
    | c=cident i=INTEGER
      {
          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
          if (*key != *c) {
              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
              add_recognition_error("Only expressions of the form X = X " + sstring($i.text[0] == '-' ? "-" : "+") + " <value> are supported.");
          }
          add_raw_update(operations, key, make_shared<cql3::operation::addition>(cql3::constants::literal::integer($i.text)));
      }
    | K_SCYLLA_COUNTER_SHARD_LIST '(' t=term ')'
      {
          add_raw_update(operations, key, ::make_shared<cql3::operation::set_counter_value_from_tuple_list>(t));      
      }
    ;

specializedColumnOperation[std::vector<std::pair<shared_ptr<cql3::column_identifier::raw>,
                                                 shared_ptr<cql3::operation::raw_update>>>& operations,
                           shared_ptr<cql3::column_identifier::raw> key,
                           shared_ptr<cql3::term::raw> k,
                           bool by_uuid]

    : '=' t=term
      {
          add_raw_update(operations, key, make_shared<cql3::operation::set_element>(k, t, by_uuid));
      }
    ;

columnCondition[conditions_type& conditions]
    // Note: we'll reject duplicates later
    : key=cident
        ( op=relationType t=term { conditions.emplace_back(key, cql3::column_condition::raw::simple_condition(t, *op)); }
        | K_IN
            ( values=singleColumnInValues { conditions.emplace_back(key, cql3::column_condition::raw::simple_in_condition(values)); }
            | marker=inMarker { conditions.emplace_back(key, cql3::column_condition::raw::simple_in_condition(marker)); }
            )
        | '[' element=term ']'
            ( op=relationType t=term { conditions.emplace_back(key, cql3::column_condition::raw::collection_condition(t, element, *op)); }
            | K_IN
                ( values=singleColumnInValues { conditions.emplace_back(key, cql3::column_condition::raw::collection_in_condition(element, values)); }
                | marker=inMarker { conditions.emplace_back(key, cql3::column_condition::raw::collection_in_condition(element, marker)); }
                )
            )
        )
    ;

properties[::shared_ptr<cql3::statements::property_definitions> props]
    : property[props] (K_AND property[props])*
    ;

property[::shared_ptr<cql3::statements::property_definitions> props]
    : k=ident '=' simple=propertyValue { try { $props->add_property(k->to_string(), simple); } catch (exceptions::syntax_exception e) { add_recognition_error(e.what()); } }
    | k=ident '=' map=mapLiteral { try { $props->add_property(k->to_string(), convert_property_map(map)); } catch (exceptions::syntax_exception e) { add_recognition_error(e.what()); } }
    ;

propertyValue returns [sstring str]
    : c=constant           { $str = c->get_raw_text(); }
    | u=unreserved_keyword { $str = u; }
    ;

relationType returns [const cql3::operator_type* op = nullptr]
    : '='  { $op = &cql3::operator_type::EQ; }
    | '<'  { $op = &cql3::operator_type::LT; }
    | '<=' { $op = &cql3::operator_type::LTE; }
    | '>'  { $op = &cql3::operator_type::GT; }
    | '>=' { $op = &cql3::operator_type::GTE; }
    | '!=' { $op = &cql3::operator_type::NEQ; }
    ;

relation[std::vector<cql3::relation_ptr>& clauses]
    @init{ const cql3::operator_type* rt = nullptr; }
    : name=cident type=relationType t=term { $clauses.emplace_back(::make_shared<cql3::single_column_relation>(std::move(name), *type, std::move(t))); }

    | K_TOKEN l=tupleOfIdentifiers type=relationType t=term
        { $clauses.emplace_back(::make_shared<cql3::token_relation>(std::move(l), *type, std::move(t))); }
    | name=cident K_IS K_NOT K_NULL {
          $clauses.emplace_back(make_shared<cql3::single_column_relation>(std::move(name), cql3::operator_type::IS_NOT, cql3::constants::NULL_LITERAL)); }
    | name=cident K_IN marker=inMarker
        { $clauses.emplace_back(make_shared<cql3::single_column_relation>(std::move(name), cql3::operator_type::IN, std::move(marker))); }
    | name=cident K_IN in_values=singleColumnInValues
        { $clauses.emplace_back(cql3::single_column_relation::create_in_relation(std::move(name), std::move(in_values))); }
    | name=cident K_CONTAINS { rt = &cql3::operator_type::CONTAINS; } (K_KEY { rt = &cql3::operator_type::CONTAINS_KEY; })?
        t=term { $clauses.emplace_back(make_shared<cql3::single_column_relation>(std::move(name), *rt, std::move(t))); }
    | name=cident '[' key=term ']' type=relationType t=term { $clauses.emplace_back(make_shared<cql3::single_column_relation>(std::move(name), std::move(key), *type, std::move(t))); }
    | ids=tupleOfIdentifiers
      ( K_IN
          ( '(' ')'
              { $clauses.emplace_back(cql3::multi_column_relation::create_in_relation(ids, std::vector<shared_ptr<cql3::tuples::literal>>())); }
          | tupleInMarker=inMarkerForTuple /* (a, b, c) IN ? */
              { $clauses.emplace_back(cql3::multi_column_relation::create_single_marker_in_relation(ids, tupleInMarker)); }
          | literals=tupleOfTupleLiterals /* (a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) */
              {
                  $clauses.emplace_back(cql3::multi_column_relation::create_in_relation(ids, literals));
              }
          | markers=tupleOfMarkersForTuples /* (a, b, c) IN (?, ?, ...) */
              { $clauses.emplace_back(cql3::multi_column_relation::create_in_relation(ids, markers)); }
          )
      | type=relationType literal=tupleLiteral /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
          {
              $clauses.emplace_back(cql3::multi_column_relation::create_non_in_relation(ids, *type, literal));
          }
      | type=relationType tupleMarker=markerForTuple /* (a, b, c) >= ? */
          { $clauses.emplace_back(cql3::multi_column_relation::create_non_in_relation(ids, *type, tupleMarker)); }
      )
    | '(' relation[$clauses] ')'
    ;

inMarker returns [shared_ptr<cql3::abstract_marker::in_raw> marker]
    : QMARK { $marker = new_in_bind_variables(nullptr); }
    | ':' name=ident { $marker = new_in_bind_variables(name); }
    ;

tupleOfIdentifiers returns [std::vector<::shared_ptr<cql3::column_identifier::raw>> ids]
    : '(' n1=cident { $ids.push_back(n1); } (',' ni=cident { $ids.push_back(ni); })* ')'
    ;

singleColumnInValues returns [std::vector<::shared_ptr<cql3::term::raw>> terms]
    : '(' ( t1 = term { $terms.push_back(t1); } (',' ti=term { $terms.push_back(ti); })* )? ')'
    ;

tupleOfTupleLiterals returns [std::vector<::shared_ptr<cql3::tuples::literal>> literals]
    : '(' t1=tupleLiteral { $literals.emplace_back(t1); } (',' ti=tupleLiteral { $literals.emplace_back(ti); })* ')'
    ;

markerForTuple returns [shared_ptr<cql3::tuples::raw> marker]
    : QMARK { $marker = new_tuple_bind_variables(nullptr); }
    | ':' name=ident { $marker = new_tuple_bind_variables(name); }
    ;

tupleOfMarkersForTuples returns [std::vector<::shared_ptr<cql3::tuples::raw>> markers]
    : '(' m1=markerForTuple { $markers.emplace_back(m1); } (',' mi=markerForTuple { $markers.emplace_back(mi); })* ')'
    ;

inMarkerForTuple returns [shared_ptr<cql3::tuples::in_raw> marker]
    : QMARK { $marker = new_tuple_in_bind_variables(nullptr); }
    | ':' name=ident { $marker = new_tuple_in_bind_variables(name); }
    ;

// The comparator_type rule is used for users' queries (internal=false)
// and for internal calls from db::cql_type_parser::parse() (internal=true).
// The latter is used for reading schemas stored in the system tables, and
// may support additional column types that cannot be created through CQL,
// but only internally through code. Today the only such type is "empty":
// Scylla code internally creates columns with type "empty" or collections
// "empty" to represent unselected columns in materialized views.
// If a user (internal=false) tries to use "empty" as a type, it is treated -
// as do all unknown types - as an attempt to use a user-defined type, and
// we report this name is reserved (as for _reserved_type_names()).
comparator_type [bool internal] returns [shared_ptr<cql3_type::raw> t]
    : n=native_or_internal_type[internal]     { $t = cql3_type::raw::from(n); }
    | c=collection_type[internal]   { $t = c; }
    | tt=tuple_type[internal]       { $t = tt; }
    | id=userTypeName   { $t = cql3::cql3_type::raw::user_type(id); }
    | K_FROZEN '<' f=comparator_type[internal] '>'
      {
        try {
            $t = cql3::cql3_type::raw::frozen(f);
        } catch (exceptions::invalid_request_exception& e) {
            add_recognition_error(e.what());
        }
      }
#if 0
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
#endif
    ;

native_or_internal_type [bool internal] returns [shared_ptr<cql3_type> t]
    : n=native_type     { $t = n; }
    // The "internal" types, only supported when internal==true:
    | K_EMPTY   {
        if (internal) {
            $t = cql3_type::empty;
        } else {
            add_recognition_error("Invalid (reserved) user type name empty");
        }
      }
    ;

comparatorType returns [shared_ptr<cql3_type::raw> t]
    : tt=comparator_type[false]    { $t = tt; }
    ;

native_type returns [shared_ptr<cql3_type> t]
    : K_ASCII     { $t = cql3_type::ascii; }
    | K_BIGINT    { $t = cql3_type::bigint; }
    | K_BLOB      { $t = cql3_type::blob; }
    | K_BOOLEAN   { $t = cql3_type::boolean; }
    | K_COUNTER   { $t = cql3_type::counter; }
    | K_DECIMAL   { $t = cql3_type::decimal; }
    | K_DOUBLE    { $t = cql3_type::double_; }
    | K_DURATION  { $t = cql3_type::duration; }
    | K_FLOAT     { $t = cql3_type::float_; }
    | K_INET      { $t = cql3_type::inet; }
    | K_INT       { $t = cql3_type::int_; }
    | K_SMALLINT  { $t = cql3_type::smallint; }
    | K_TEXT      { $t = cql3_type::text; }
    | K_TIMESTAMP { $t = cql3_type::timestamp; }
    | K_TINYINT   { $t = cql3_type::tinyint; }
    | K_UUID      { $t = cql3_type::uuid; }
    | K_VARCHAR   { $t = cql3_type::varchar; }
    | K_VARINT    { $t = cql3_type::varint; }
    | K_TIMEUUID  { $t = cql3_type::timeuuid; }
    | K_DATE      { $t = cql3_type::date; }
    | K_TIME      { $t = cql3_type::time; }
    ;

collection_type [bool internal] returns [shared_ptr<cql3::cql3_type::raw> pt]
    : K_MAP  '<' t1=comparator_type[internal] ',' t2=comparator_type[internal] '>'
        {
            // if we can't parse either t1 or t2, antlr will "recover" and we may have t1 or t2 null.
            if (t1 && t2) {
                $pt = cql3::cql3_type::raw::map(t1, t2);
            }
        }
    | K_LIST '<' t=comparator_type[internal] '>'
        { if (t) { $pt = cql3::cql3_type::raw::list(t); } }
    | K_SET  '<' t=comparator_type[internal] '>'
        { if (t) { $pt = cql3::cql3_type::raw::set(t); } }
    ;

tuple_type [bool internal] returns [shared_ptr<cql3::cql3_type::raw> t]
        @init{ std::vector<shared_ptr<cql3::cql3_type::raw>> types; }
    : K_TUPLE '<'
         t1=comparator_type[internal] { types.push_back(t1); } (',' tn=comparator_type[internal] { types.push_back(tn); })*
      '>' { $t = cql3::cql3_type::raw::tuple(std::move(types)); }
    ;

username
    : IDENT
    | STRING_LITERAL
    | QUOTED_NAME { add_recognition_error("Quoted strings are not supported for user names"); }
    ;

// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
non_type_ident returns [shared_ptr<cql3::column_identifier> id]
    : t=IDENT                    { if (_reserved_type_names().count($t.text)) { add_recognition_error("Invalid (reserved) user type name " + $t.text); } $id = ::make_shared<cql3::column_identifier>($t.text, false); }
    | t=QUOTED_NAME              { $id = ::make_shared<cql3::column_identifier>($t.text, true); }
    | k=basic_unreserved_keyword { $id = ::make_shared<cql3::column_identifier>(k, false); }
    | kk=K_KEY                   { $id = ::make_shared<cql3::column_identifier>($kk.text, false); }
    ;

unreserved_keyword returns [sstring str]
    : u=unreserved_function_keyword     { $str = u; }
    | k=(K_TTL | K_COUNT | K_WRITETIME | K_KEY) { $str = $k.text; }
    ;

unreserved_function_keyword returns [sstring str]
    : u=basic_unreserved_keyword { $str = u; }
    | t=native_or_internal_type[true]   { $str = t->to_string(); }
    ;

basic_unreserved_keyword returns [sstring str]
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
        | K_ROLE
        | K_ROLES
        | K_SUPERUSER
        | K_NOSUPERUSER
        | K_LOGIN
        | K_NOLOGIN
        | K_OPTIONS
        | K_PASSWORD
        | K_EXISTS
        | K_CUSTOM
        | K_TRIGGER
        | K_DISTINCT
        | K_CONTAINS
        | K_STATIC
        | K_FROZEN
        | K_TUPLE
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
        | K_JSON
        ) { $str = $k.text; }
    ;

// Case-insensitive keywords
K_SELECT:      S E L E C T;
K_FROM:        F R O M;
K_AS:          A S;
K_CAST:        C A S T;
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
K_MATERIALIZED:M A T E R I A L I Z E D;
K_VIEW:        V I E W;
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
K_IS:          I S;
K_CONTAINS:    C O N T A I N S;

K_GRANT:       G R A N T;
K_ALL:         A L L;
K_PERMISSION:  P E R M I S S I O N;
K_PERMISSIONS: P E R M I S S I O N S;
K_OF:          O F;
K_REVOKE:      R E V O K E;
K_MODIFY:      M O D I F Y;
K_AUTHORIZE:   A U T H O R I Z E;
K_DESCRIBE:    D E S C R I B E;
K_NORECURSIVE: N O R E C U R S I V E;

K_USER:        U S E R;
K_USERS:       U S E R S;
K_ROLE:        R O L E;
K_ROLES:       R O L E S;
K_SUPERUSER:   S U P E R U S E R;
K_NOSUPERUSER: N O S U P E R U S E R;
K_PASSWORD:    P A S S W O R D;
K_LOGIN:       L O G I N;
K_NOLOGIN:     N O L O G I N;
K_OPTIONS:     O P T I O N S;

K_CLUSTERING:  C L U S T E R I N G;
K_ASCII:       A S C I I;
K_BIGINT:      B I G I N T;
K_BLOB:        B L O B;
K_BOOLEAN:     B O O L E A N;
K_COUNTER:     C O U N T E R;
K_DECIMAL:     D E C I M A L;
K_DOUBLE:      D O U B L E;
K_DURATION:    D U R A T I O N;
K_FLOAT:       F L O A T;
K_INET:        I N E T;
K_INT:         I N T;
K_SMALLINT:    S M A L L I N T;
K_TINYINT:     T I N Y I N T;
K_TEXT:        T E X T;
K_UUID:        U U I D;
K_VARCHAR:     V A R C H A R;
K_VARINT:      V A R I N T;
K_TIMEUUID:    T I M E U U I D;
K_TOKEN:       T O K E N;
K_WRITETIME:   W R I T E T I M E;
K_DATE:        D A T E;
K_TIME:        T I M E;

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
K_JSON:        J S O N;
K_DEFAULT:     D E F A U L T;
K_UNSET:       U N S E T;

K_EMPTY:       E M P T Y;

K_SCYLLA_TIMEUUID_LIST_INDEX: S C Y L L A '_' T I M E U U I D '_' L I S T '_' I N D E X;
K_SCYLLA_COUNTER_SHARD_LIST: S C Y L L A '_' C O U N T E R '_' S H A R D '_' L I S T; 

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
        std::string txt; // temporary to build pg-style-string
    }
    @after{ 
        // This is an ugly hack that allows returning empty string literals.
        // If setText() was called with an empty string antlr3 would decide
        // that setText() was never called and just return the unmodified
        // token value. To prevent that we call setText() with non-empty string
        // that is not valid utf8 which will be later changed to an empty
        // string once it leaves antlr3 code.
        if (txt.empty()) {
            txt.push_back(-1);
        }
        setText(txt);
    }
    :
      /* pg-style string literal */
      (
        '$' '$'
        (
          (c=~('$') { txt.push_back(c); })
          |
          ('$' (c=~('$') { txt.push_back('$'); txt.push_back(c); }))
        )*
        '$' '$'
      )
      |
      /* conventional quoted string literal */
      (
        '\'' (c=~('\'') { txt.push_back(c);} | '\'' '\'' { txt.push_back('\''); })* '\''
      )
    ;

QUOTED_NAME
    @init{ std::string b; }
    @after{ setText(b); }
    : '\"' (c=~('\"') { b.push_back(c); } | '\"' '\"' { b.push_back('\"'); })+ '\"'
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

fragment DURATION_UNIT
    : Y
    | M O
    | W
    | D
    | H
    | M
    | S
    | M S
    | U S
    | '\u00B5' S
    | N S
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

DURATION
    : '-'? DIGIT+ DURATION_UNIT (DIGIT+ DURATION_UNIT)*
    | '-'? 'P' (DIGIT+ 'Y')? (DIGIT+ 'M')? (DIGIT+ 'D')? ('T' (DIGIT+ 'H')? (DIGIT+ 'M')? (DIGIT+ 'S')?)? // ISO 8601 "format with designators"
    | '-'? 'P' DIGIT+ 'W'
    | '-'? 'P' DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT 'T' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT // ISO 8601 "alternative format"
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
