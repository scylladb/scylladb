/*
 * SPDX-License-Identifier: Apache-2.0
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
#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/statements/raw/select_statement.hh"
#include "cql3/statements/alter_keyspace_statement.hh"
#include "cql3/statements/alter_table_statement.hh"
#include "cql3/statements/alter_view_statement.hh"
#include "cql3/statements/alter_service_level_statement.hh"
#include "cql3/statements/create_keyspace_statement.hh"
#include "cql3/statements/drop_keyspace_statement.hh"
#include "cql3/statements/create_index_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/create_view_statement.hh"
#include "cql3/statements/create_type_statement.hh"
#include "cql3/statements/create_function_statement.hh"
#include "cql3/statements/create_aggregate_statement.hh"
#include "cql3/statements/create_service_level_statement.hh"
#include "cql3/statements/sl_prop_defs.hh"
#include "cql3/statements/attach_service_level_statement.hh"
#include "cql3/statements/drop_type_statement.hh"
#include "cql3/statements/alter_type_statement.hh"
#include "cql3/statements/property_definitions.hh"
#include "cql3/statements/drop_index_statement.hh"
#include "cql3/statements/drop_table_statement.hh"
#include "cql3/statements/drop_view_statement.hh"
#include "cql3/statements/drop_function_statement.hh"
#include "cql3/statements/drop_aggregate_statement.hh"
#include "cql3/statements/drop_service_level_statement.hh"
#include "cql3/statements/detach_service_level_statement.hh"
#include "cql3/statements/raw/truncate_statement.hh"
#include "cql3/statements/raw/update_statement.hh"
#include "cql3/statements/raw/insert_statement.hh"
#include "cql3/statements/raw/delete_statement.hh"
#include "cql3/statements/index_prop_defs.hh"
#include "cql3/statements/raw/use_statement.hh"
#include "cql3/statements/raw/batch_statement.hh"
#include "cql3/statements/raw/describe_statement.hh"
#include "cql3/statements/list_users_statement.hh"
#include "cql3/statements/grant_statement.hh"
#include "cql3/statements/revoke_statement.hh"
#include "cql3/statements/list_permissions_statement.hh"
#include "cql3/statements/alter_role_statement.hh"
#include "cql3/statements/list_roles_statement.hh"
#include "cql3/statements/list_service_level_statement.hh"
#include "cql3/statements/list_service_level_attachments_statement.hh"
#include "cql3/statements/grant_role_statement.hh"
#include "cql3/statements/revoke_role_statement.hh"
#include "cql3/statements/drop_role_statement.hh"
#include "cql3/statements/create_role_statement.hh"
#include "cql3/statements/index_target.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "cql3/selection/raw_selector.hh"
#include "cql3/selection/selectable-expr.hh"
#include "cql3/keyspace_element_name.hh"
#include "cql3/constants.hh"
#include "cql3/operation_impl.hh"
#include "cql3/error_listener.hh"
#include "cql3/index_name.hh"
#include "cql3/cql3_type.hh"
#include "cql3/cf_name.hh"
#include "cql3/maps.hh"
#include "cql3/sets.hh"
#include "cql3/lists.hh"
#include "cql3/role_name.hh"
#include "cql3/role_options.hh"
#include "cql3/user_types.hh"
#include "cql3/ut_name.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/expr/expression.hh"
#include <seastar/core/sstring.hh>
#include "CqlLexer.hpp"

#include <algorithm>
#include <unordered_map>
#include <map>
}

@parser::traits {

using namespace cql3;
using namespace cql3::statements;
using namespace cql3::selection;
using namespace cql3::expr;

using cql3::cql3_type;
using operations_type = std::vector<std::pair<::shared_ptr<cql3::column_identifier::raw>, std::unique_ptr<cql3::operation::raw_update>>>;

// ANTLR forces us to define a default-initialized return value
// for every rule (e.g. [returns ut_name name]), but not every type
// can be naturally zero-initialized.
//
// The uninitialized<T> wrapper can be zero-initialized, and is convertible
// to T (after checking that it was assigned to) implicitly, eliminating the
// problem.  It is up to the user to ensure it is actually assigned to. 
template <typename T>
struct uninitialized {
    std::optional<T> _val;
    uninitialized() = default;
    uninitialized(const uninitialized&) = default;
    uninitialized(uninitialized&&) = default;
    uninitialized(const T& val) : _val(val) {}
    uninitialized(T&& val) : _val(std::move(val)) {}
    uninitialized& operator=(const uninitialized&) = default;
    uninitialized& operator=(uninitialized&&) = default;
    operator const T&() const & { return check(), *_val; }
    operator T&&() && { return check(), std::move(*_val); }
    operator std::optional<T>&&() && { return check(), std::move(_val); }
    void check() const { if (!_val) { throw std::runtime_error("not intitialized"); } }
};

}

@context {
    using collector_type = cql3::error_collector<ComponentType, ExceptionBaseType::TokenType, ExceptionBaseType>;
    using listener_type = cql3::error_listener<ComponentType, ExceptionBaseType>;

    listener_type* listener;

    std::vector<::shared_ptr<cql3::column_identifier>> _bind_variables;
    // index into _bind_variables
    std::unordered_map<cql3::column_identifier, size_t> _named_bind_variables_indexes;
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

    bind_variable new_bind_variables(shared_ptr<cql3::column_identifier> name)
    {
        if (name && _named_bind_variables_indexes.contains(*name)) {
            return bind_variable{_named_bind_variables_indexes[*name]};
        }
        auto marker = bind_variable{_bind_variables.size()};
        _bind_variables.push_back(name);
        if (name) {
            _named_bind_variables_indexes[*name] = marker.bind_index;
        }
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

    std::map<sstring, sstring> convert_property_map(const collection_constructor& map) {
        if (map.elements.empty()) {
            return std::map<sstring, sstring>{};
        }
        std::map<sstring, sstring> res;
        for (auto&& entry : map.elements) {
            auto entry_tuple = expr::as_if<tuple_constructor>(&entry);
            // Because the parser tries to be smart and recover on error (to
            // allow displaying more than one error I suppose), we have default-constructed
            // entries in map.elements. Just skip those, a proper error will be thrown in the end.
            if (!entry_tuple || entry_tuple->elements.size() != 2) {
                break;
            }
            auto left = expr::as_if<untyped_constant>(&entry_tuple->elements[0]);
            if (!left) {
                sstring msg = fmt::format("Invalid property name: {}", entry_tuple->elements[0]);
                if (expr::is<bind_variable>(entry_tuple->elements[0])) {
                    msg += " (bind variables are not supported in DDL queries)";
                }
                add_recognition_error(msg);
                break;
            }
            auto right = expr::as_if<untyped_constant>(&entry_tuple->elements[1]);
            if (!right) {
                sstring msg = fmt::format("Invalid property value: {} for property: {}", entry_tuple->elements[0], entry_tuple->elements[1]);
                if (expr::is<bind_variable>(entry_tuple->elements[1])) {
                    msg += " (bind variables are not supported in DDL queries)";
                }
                add_recognition_error(msg);
                break;
            }
            if (!res.emplace(left->raw_text, right->raw_text).second) {
                sstring msg = fmt::format("Multiple definition for property {}", left->raw_text);
                add_recognition_error(msg);
                break;
            }
        }
        return res;
    }

    sstring to_lower(std::string_view s) {
        sstring lower_s(s.size(), '\0');
        std::transform(s.cbegin(), s.cend(), lower_s.begin(), &::tolower);
        return lower_s;
    }

    bool convert_boolean_literal(std::string_view s) {
        return to_lower(s) == "true";
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

queries returns [std::vector<std::unique_ptr<raw::parsed_statement>> stmts]
    : st=cqlStatement { $stmts.emplace_back(std::move(st)); }
      (';' st=cqlStatement { $stmts.emplace_back(std::move(st)); })*
      (';')* EOF
    ;

query returns [std::unique_ptr<raw::parsed_statement> stmnt]
    : st=cqlStatement (';')* EOF { $stmnt = std::move(st); }
    ;

cqlStatement returns [std::unique_ptr<raw::parsed_statement> stmt]
    @after{ if (stmt) { stmt->set_bound_variables(_bind_variables); } }
    : st1= selectStatement             { $stmt = std::move(st1); }
    | st2= insertStatement             { $stmt = std::move(st2); }
    | st3= updateStatement             { $stmt = std::move(st3); }
    | st4= batchStatement              { $stmt = std::move(st4); }
    | st5= deleteStatement             { $stmt = std::move(st5); }
    | st6= useStatement                { $stmt = std::move(st6); }
    | st7= truncateStatement           { $stmt = std::move(st7); }
    | st8= createKeyspaceStatement     { $stmt = std::move(st8); }
    | st9= createTableStatement        { $stmt = std::move(st9); }
    | st10=createIndexStatement        { $stmt = std::move(st10); }
    | st11=dropKeyspaceStatement       { $stmt = std::move(st11); }
    | st12=dropTableStatement          { $stmt = std::move(st12); }
    | st13=dropIndexStatement          { $stmt = std::move(st13); }
    | st14=alterTableStatement         { $stmt = std::move(st14); }
    | st15=alterKeyspaceStatement      { $stmt = std::move(st15); }
    | st16=grantStatement              { $stmt = std::move(st16); }
    | st17=revokeStatement             { $stmt = std::move(st17); }
    | st18=listPermissionsStatement    { $stmt = std::move(st18); }
    | st19=createUserStatement         { $stmt = std::move(st19); }
    | st20=alterUserStatement          { $stmt = std::move(st20); }
    | st21=dropUserStatement           { $stmt = std::move(st21); }
    | st22=listUsersStatement          { $stmt = std::move(st22); }
#if 0
    | st23=createTriggerStatement      { $stmt = st23; }
    | st24=dropTriggerStatement        { $stmt = st24; }
#endif
    | st25=createTypeStatement         { $stmt = std::move(st25); }
    | st26=alterTypeStatement          { $stmt = std::move(st26); }
    | st27=dropTypeStatement           { $stmt = std::move(st27); }
    | st28=createFunctionStatement     { $stmt = std::move(st28); }
    | st29=dropFunctionStatement       { $stmt = std::move(st29); }
    | st30=createAggregateStatement    { $stmt = std::move(st30); }
    | st31=dropAggregateStatement      { $stmt = std::move(st31); }
    | st32=createViewStatement         { $stmt = std::move(st32); }
    | st33=alterViewStatement          { $stmt = std::move(st33); }
    | st34=dropViewStatement           { $stmt = std::move(st34); }
    | st35=listRolesStatement          { $stmt = std::move(st35); }
    | st36=grantRoleStatement          { $stmt = std::move(st36); }
    | st37=revokeRoleStatement         { $stmt = std::move(st37); }
    | st38=dropRoleStatement           { $stmt = std::move(st38); }
    | st39=createRoleStatement         { $stmt = std::move(st39); }
    | st40=alterRoleStatement          { $stmt = std::move(st40); }
    | st41=createServiceLevelStatement { $stmt = std::move(st41); }
    | st42=alterServiceLevelStatement  { $stmt = std::move(st42); }
    | st43=dropServiceLevelStatement   { $stmt = std::move(st43); }
    | st44=attachServiceLevelStatement { $stmt = std::move(st44); }
    | st45=detachServiceLevelStatement { $stmt = std::move(st45); }
    | st46=listServiceLevelStatement { $stmt = std::move(st46); }
    | st47=listServiceLevelAttachStatement { $stmt = std::move(st47); }
    | st48=pruneMaterializedViewStatement  { $stmt = std::move(st48); }
    | st49=describeStatement           { $stmt = std::move(st49); }
    ;

/*
 * USE <KEYSPACE>;
 */
useStatement returns [std::unique_ptr<raw::use_statement> stmt]
    : K_USE ks=keyspaceName { $stmt = std::make_unique<raw::use_statement>(ks); }
    ;

/**
 * SELECT [JSON] <expression>
 * FROM <CF>
 * WHERE KEY = "key1" AND COL > 1 AND COL < 100
 * LIMIT <NUMBER>
 * [USING TIMEOUT <duration>];
 */
selectStatement returns [std::unique_ptr<raw::select_statement> expr]
    @init {
        bool is_distinct = false;
        std::optional<expression> limit;
        std::optional<expression> per_partition_limit;
        raw::select_statement::parameters::orderings_type orderings;
        bool allow_filtering = false;
        raw::select_statement::parameters::statement_subtype statement_subtype = raw::select_statement::parameters::statement_subtype::REGULAR;
        bool bypass_cache = false;
        auto attrs = std::make_unique<cql3::attributes::raw>();
        expression wclause = conjunction{};
    }
    : K_SELECT (
                ( K_JSON { statement_subtype = raw::select_statement::parameters::statement_subtype::JSON; } )?
                ( K_DISTINCT { is_distinct = true; } )?
                sclause=selectClause
               )
      K_FROM (
                cf=columnFamilyName
                | K_MUTATION_FRAGMENTS '(' cf=columnFamilyName ')' { statement_subtype = raw::select_statement::parameters::statement_subtype::MUTATION_FRAGMENTS; }
             )
      ( K_WHERE w=whereClause { wclause = std::move(w); } )?
      ( K_GROUP K_BY gbcolumns=listOfIdentifiers)?
      ( K_ORDER K_BY orderByClause[orderings] ( ',' orderByClause[orderings] )* )?
      ( K_PER K_PARTITION K_LIMIT rows=intValue { per_partition_limit = rows; } )?
      ( K_LIMIT rows=intValue { limit = rows; } )?
      ( K_ALLOW K_FILTERING  { allow_filtering = true; } )?
      ( K_BYPASS K_CACHE { bypass_cache = true; })?
      ( usingTimeoutClause[attrs] )?
      {
          auto params = make_lw_shared<raw::select_statement::parameters>(std::move(orderings), is_distinct, allow_filtering, statement_subtype, bypass_cache);
          $expr = std::make_unique<raw::select_statement>(std::move(cf), std::move(params),
            std::move(sclause), std::move(wclause), std::move(limit), std::move(per_partition_limit),
            std::move(gbcolumns), std::move(attrs));
      }
    ;

selectClause returns [std::vector<shared_ptr<raw_selector>> expr]
    : t1=selector { $expr.push_back(t1); } (',' tN=selector { $expr.push_back(tN); })*
    | '*' { }
    ;

selector returns [shared_ptr<raw_selector> s]
    @init{ shared_ptr<cql3::column_identifier> alias; }
    : us=unaliasedSelector (K_AS c=ident { alias = c; })? { $s = ::make_shared<raw_selector>(us, alias); }
    ;

unaliasedSelector returns [expression s]
    @init { expression tmp; }
    :  ( c=cident                                  { tmp = unresolved_identifier{std::move(c)}; }
       | K_COUNT '(' countArgument ')'             { tmp = make_count_rows_function_expression(); }
       | K_WRITETIME '(' c=cident ')'              { tmp = column_mutation_attribute{column_mutation_attribute::attribute_kind::writetime,
                                                                                              unresolved_identifier{std::move(c)}}; }
       | K_TTL       '(' c=cident ')'              { tmp = column_mutation_attribute{column_mutation_attribute::attribute_kind::ttl,
                                                                                              unresolved_identifier{std::move(c)}}; }
       | f=functionName args=selectionFunctionArgs { tmp = function_call{std::move(f), std::move(args)}; }
       | K_CAST      '(' arg=unaliasedSelector K_AS t=native_type ')'  { tmp = cast{.style = cast::cast_style::sql, .arg = std::move(arg), .type = std::move(t)}; }
       )
       ( '.' fi=cident { tmp = field_selection{std::move(tmp), std::move(fi)}; } )*
    { $s = tmp; }
    ;

selectionFunctionArgs returns [std::vector<expression> a]
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

whereClause returns [expression clause]
    @init { std::vector<expression> terms; }
    : e1=relation { terms.push_back(std::move(e1)); } (K_AND en=relation { terms.push_back(std::move(en)); })*
        { clause = conjunction{std::move(terms)}; }
    ;

orderByClause[raw::select_statement::parameters::orderings_type& orderings]
    @init{
        raw::select_statement::ordering ordering = raw::select_statement::ordering::ascending;
    }
    : c=cident (K_ASC | K_DESC { ordering = raw::select_statement::ordering::descending; })? { orderings.emplace_back(c, ordering); }
    ;

jsonValue returns [expression value]
    :
    | s=STRING_LITERAL { $value = untyped_constant{untyped_constant::string, $s.text}; }
    | m=marker         { $value = std::move(m); }
    ;

/**
 * INSERT INTO <CF> (<column>, <column>, <column>, ...)
 * VALUES (<value>, <value>, <value>, ...)
 * USING TIMESTAMP <long>;
 *
 */
insertStatement returns [std::unique_ptr<raw::modification_statement> expr]
    @init {
        auto attrs = std::make_unique<cql3::attributes::raw>();
        std::vector<::shared_ptr<cql3::column_identifier::raw>> column_names;
        std::vector<expression> values;
        bool if_not_exists = false;
        bool default_unset = false;
        std::optional<expression> json_value;
    }
    : K_INSERT K_INTO cf=columnFamilyName
        ('(' c1=cident { column_names.push_back(c1); }  ( ',' cn=cident { column_names.push_back(cn); } )* ')'
            K_VALUES
            '(' v1=term { values.push_back(v1); } ( ',' vn=term { values.push_back(vn); } )* ')'
            ( K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
            ( usingClause[attrs] )?
              {
              $expr = std::make_unique<raw::insert_statement>(std::move(cf),
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
              $expr = std::make_unique<raw::insert_json_statement>(std::move(cf),
                                                       std::move(attrs),
                                                       std::move(*json_value),
                                                       if_not_exists,
                                                       default_unset);
              }
        )
    ;

usingClause[std::unique_ptr<cql3::attributes::raw>& attrs]
    : K_USING usingClauseObjective[attrs] ( K_AND usingClauseObjective[attrs] )*
    ;

usingClauseObjective[std::unique_ptr<cql3::attributes::raw>& attrs]
    : K_TIMESTAMP ts=intValue { attrs->timestamp = ts; }
    | K_TTL t=intValue { attrs->time_to_live = t; }
    | K_TIMEOUT to=term { attrs->timeout = to; }
    ;

usingTimestampTimeoutClause[std::unique_ptr<cql3::attributes::raw>& attrs]
    : K_USING usingTimestampTimeoutClauseObjective[attrs] ( K_AND usingTimestampTimeoutClauseObjective[attrs] )*
    ;

usingTimestampTimeoutClauseObjective[std::unique_ptr<cql3::attributes::raw>& attrs]
    : K_TIMESTAMP ts=intValue { attrs->timestamp = ts; }
    | K_TIMEOUT to=term { attrs->timeout = to; }
    ;

usingTimeoutClause[std::unique_ptr<cql3::attributes::raw>& attrs]
    : K_USING K_TIMEOUT to=term { attrs->timeout = to; }
    ;

/**
 * UPDATE <CF>
 * USING TIMESTAMP <long>
 * SET name1 = value1, name2 = value2
 * WHERE key = value;
 */
updateStatement returns [std::unique_ptr<raw::update_statement> expr]
    @init {
        bool if_exists = false;
        auto attrs = std::make_unique<cql3::attributes::raw>();
        std::vector<std::pair<::shared_ptr<cql3::column_identifier::raw>, std::unique_ptr<cql3::operation::raw_update>>> operations;
        std::optional<expression> cond_opt;
    }
    : K_UPDATE cf=columnFamilyName
      ( usingClause[attrs] )?
      K_SET columnOperation[operations] (',' columnOperation[operations])*
      K_WHERE wclause=whereClause
      ( K_IF (K_EXISTS{ if_exists = true; } | conditions=updateConditions { cond_opt = std::move(conditions); } ))?
      {
          return std::make_unique<raw::update_statement>(std::move(cf),
                                                  std::move(attrs),
                                                  std::move(operations),
                                                  std::move(wclause),
                                                  std::move(cond_opt),
                                                  if_exists);
     }
    ;

updateConditions returns [expression cond]
    @init {
        std::vector<expression> conditions;
    }
    : c1=columnCondition { conditions.emplace_back(std::move(c1)); }
         ( K_AND cn=columnCondition { conditions.emplace_back(std::move(cn)); } )*
    {
        return conjunction{std::move(conditions)};
    }
    ;

/**
 * DELETE name1, name2
 * FROM <CF>
 * [USING (TIMESTAMP <long> | TIMEOUT <duration>) [AND ...]]
 * WHERE KEY = keyname
   [IF (EXISTS | name = value, ...)];
 */
deleteStatement returns [std::unique_ptr<raw::delete_statement> expr]
    @init {
        auto attrs = std::make_unique<cql3::attributes::raw>();
        std::vector<std::unique_ptr<cql3::operation::raw_deletion>> column_deletions;
        bool if_exists = false;
        std::optional<expression> cond_opt;
    }
    : K_DELETE ( dels=deleteSelection { column_deletions = std::move(dels); } )?
      K_FROM cf=columnFamilyName
      ( usingTimestampTimeoutClause[attrs] )?
      K_WHERE wclause=whereClause
      ( K_IF ( K_EXISTS { if_exists = true; } | conditions=updateConditions { cond_opt = std::move(conditions); } ))?
      {
          return std::make_unique<raw::delete_statement>(cf,
                                            std::move(attrs),
                                            std::move(column_deletions),
                                            std::move(wclause),
                                            std::move(cond_opt),
                                            if_exists);
      }
    ;

deleteSelection returns [std::vector<std::unique_ptr<cql3::operation::raw_deletion>> operations]
    : t1=deleteOp { $operations.emplace_back(std::move(t1)); }
      (',' tN=deleteOp { $operations.emplace_back(std::move(tN)); })*
    ;

deleteOp returns [std::unique_ptr<cql3::operation::raw_deletion> op]
    : c=cident                { $op = std::make_unique<cql3::operation::column_deletion>(std::move(c)); }
    | c=cident '[' t=term ']' { $op = std::make_unique<cql3::operation::element_deletion>(std::move(c), std::move(t)); }
    | c=cident '.' field=ident { $op = std::make_unique<cql3::operation::field_deletion>(std::move(c), std::move(field)); }
    ;

pruneMaterializedViewStatement returns [std::unique_ptr<raw::select_statement> expr]
    @init {
        bool is_distinct = false;
        std::optional<expression> limit;
        std::optional<expression> per_partition_limit;
        raw::select_statement::parameters::orderings_type orderings;
        bool allow_filtering = false;
        raw::select_statement::parameters::statement_subtype statement_subtype = raw::select_statement::parameters::statement_subtype::PRUNE_MATERIALIZED_VIEW;
        bool bypass_cache = false;
        auto attrs = std::make_unique<cql3::attributes::raw>();
        expression wclause = conjunction{};
    }
	: K_PRUNE K_MATERIALIZED K_VIEW cf=columnFamilyName (K_WHERE w=whereClause { wclause = std::move(w); } )? ( usingClause[attrs] )?
	  {
	        auto params = make_lw_shared<raw::select_statement::parameters>(std::move(orderings), is_distinct, allow_filtering, statement_subtype, bypass_cache);
	        return std::make_unique<raw::select_statement>(std::move(cf), std::move(params),
            std::vector<shared_ptr<raw_selector>>(), std::move(wclause), std::move(limit), std::move(per_partition_limit),
            std::vector<::shared_ptr<cql3::column_identifier::raw>>(), std::move(attrs));
	  }
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
batchStatement returns [std::unique_ptr<cql3::statements::raw::batch_statement> expr]
    @init {
        using btype = cql3::statements::raw::batch_statement::type; 
        btype type = btype::LOGGED;
        std::vector<std::unique_ptr<cql3::statements::raw::modification_statement>> statements;
        auto attrs = std::make_unique<cql3::attributes::raw>();
    }
    : K_BEGIN
      ( K_UNLOGGED { type = btype::UNLOGGED; } | K_COUNTER { type = btype::COUNTER; } )?
      K_BATCH ( usingClause[attrs] )?
          ( s=batchStatementObjective ';'? { statements.push_back(std::move(s)); } )*
      K_APPLY K_BATCH
      {
          $expr = std::make_unique<cql3::statements::raw::batch_statement>(type, std::move(attrs), std::move(statements));
      }
    ;

batchStatementObjective returns [std::unique_ptr<cql3::statements::raw::modification_statement> statement]
    : i=insertStatement  { $statement = std::move(i); }
    | u=updateStatement  { $statement = std::move(u); }
    | d=deleteStatement  { $statement = std::move(d); }
    ;

dropAggregateStatement returns [std::unique_ptr<cql3::statements::drop_aggregate_statement> expr]
    @init {
        bool if_exists = false;
        std::vector<shared_ptr<cql3_type::raw>> arg_types;
        bool args_present = false;
    }
    : K_DROP K_AGGREGATE
      (K_IF K_EXISTS { if_exists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { arg_types.push_back(v); }
            ( ',' v=comparatorType { arg_types.push_back(v); } )*
          )?
        ')'
        { args_present = true; }
      )?
      { $expr = std::make_unique<cql3::statements::drop_aggregate_statement>(std::move(fn), std::move(arg_types), args_present, if_exists); }
    ;

createAggregateStatement returns [std::unique_ptr<cql3::statements::create_aggregate_statement> expr]
    @init {
        bool or_replace = false;
        bool if_not_exists = false;

        std::vector<shared_ptr<cql3_type::raw>> arg_types;
        std::optional<sstring> ffunc;
        std::optional<expr::expression> ival;
        std::optional<sstring> rfunc;
    }
    : K_CREATE (K_OR K_REPLACE { or_replace = true; })?
      K_AGGREGATE
      (K_IF K_NOT K_EXISTS { if_not_exists = true; })?
      fn=functionName
      '('
        (
          v=comparatorType { arg_types.push_back(v); }
          ( ',' v=comparatorType { arg_types.push_back(v); } )*
        )?
      ')'
      K_SFUNC sfunc = allowedFunctionName
      K_STYPE stype = comparatorType
      (
        K_REDUCEFUNC reduce_name = allowedFunctionName { rfunc = reduce_name; }
      )?
      (
        K_FINALFUNC final_func = allowedFunctionName { ffunc = final_func; }
      )?
      (
        K_INITCOND init_val = term { ival = init_val; }
      )?
      { $expr = std::make_unique<cql3::statements::create_aggregate_statement>(std::move(fn), std::move(arg_types), std::move(sfunc), std::move(stype), std::move(rfunc), std::move(ffunc), std::move(ival), or_replace, if_not_exists); }
    ;

createFunctionStatement returns [std::unique_ptr<cql3::statements::create_function_statement> expr]
    @init {
        bool or_replace = false;
        bool if_not_exists = false;

        std::vector<shared_ptr<cql3::column_identifier>> arg_names;
        std::vector<shared_ptr<cql3_type::raw>> arg_types;
        bool called_on_null_input = false;
    }
    : K_CREATE
        // "OR REPLACE" and "IF NOT EXISTS" cannot be used together
        ((K_OR K_REPLACE { or_replace = true; } K_FUNCTION)
         | (K_FUNCTION K_IF K_NOT K_EXISTS { if_not_exists = true; })
         | K_FUNCTION)
      fn=functionName
      '('
        (
          k=ident v=comparatorType { arg_names.push_back(k); arg_types.push_back(v); }
          ( ',' k=ident v=comparatorType { arg_names.push_back(k); arg_types.push_back(v); } )*
        )?
      ')'
      ( (K_RETURNS K_NULL) | (K_CALLED { called_on_null_input = true; })) K_ON K_NULL K_INPUT
      K_RETURNS rt = comparatorType
      K_LANGUAGE language = IDENT
      K_AS body = STRING_LITERAL
      { $expr = std::make_unique<cql3::statements::create_function_statement>(std::move(fn), to_lower($language.text), $body.text, std::move(arg_names), std::move(arg_types), std::move(rt), called_on_null_input, or_replace, if_not_exists); }
    ;

dropFunctionStatement returns [std::unique_ptr<cql3::statements::drop_function_statement> expr]
    @init {
        bool if_exists = false;
        std::vector<shared_ptr<cql3_type::raw>> arg_types;
        bool args_present = false;
    }
    : K_DROP K_FUNCTION
      (K_IF K_EXISTS { if_exists = true; } )?
      fn=functionName
      (
        '('
          (
            v=comparatorType { arg_types.push_back(v); }
            ( ',' v=comparatorType { arg_types.push_back(v); } )*
          )?
        ')'
        { args_present = true; }
      )?
      { $expr = std::make_unique<cql3::statements::drop_function_statement>(std::move(fn), std::move(arg_types), args_present, if_exists); }
    ;

/**
 * CREATE KEYSPACE [IF NOT EXISTS] <KEYSPACE> WITH attr1 = value1 AND attr2 = value2;
 */
createKeyspaceStatement returns [std::unique_ptr<cql3::statements::create_keyspace_statement> expr]
    @init {
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
        bool if_not_exists = false;
    }
    : K_CREATE K_KEYSPACE (K_IF K_NOT K_EXISTS { if_not_exists = true; } )? ks=keyspaceName
      K_WITH properties[*attrs] { $expr = std::make_unique<cql3::statements::create_keyspace_statement>(ks, attrs, if_not_exists); }
    ;

/**
 * CREATE COLUMNFAMILY [IF NOT EXISTS] <CF> (
 *     <name1> <type>,
 *     <name2> <type>,
 *     <name3> <type>
 * ) WITH <property> = <value> AND ...;
 */
createTableStatement returns [std::unique_ptr<cql3::statements::create_table_statement::raw_statement> expr]
    @init { bool if_not_exists = false; }
    : K_CREATE K_COLUMNFAMILY (K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
      cf=columnFamilyName { $expr = std::make_unique<cql3::statements::create_table_statement::raw_statement>(cf, if_not_exists); }
      cfamDefinition[*expr]
    ;

cfamDefinition[cql3::statements::create_table_statement::raw_statement& expr]
    : '(' cfamColumns[expr] ( ',' cfamColumns[expr]? )* ')'
      ( K_WITH cfamProperty[$expr.properties()] ( K_AND cfamProperty[$expr.properties()] )*)?
    ;

cfamColumns[cql3::statements::create_table_statement::raw_statement& expr]
    @init { bool is_static=false; }
    : k=ident v=comparatorType (K_STATIC {is_static = true;})? { $expr.add_definition(k, v, is_static); }
        (K_PRIMARY K_KEY { $expr.add_key_aliases(std::vector<shared_ptr<cql3::column_identifier>>{k}); })?
    | K_PRIMARY K_KEY '(' pkDef[expr] (',' c=ident { $expr.add_column_alias(c); } )* ')'
    ;

pkDef[cql3::statements::create_table_statement::raw_statement& expr]
    @init { std::vector<shared_ptr<cql3::column_identifier>> l; }
    : k=ident { $expr.add_key_aliases(std::vector<shared_ptr<cql3::column_identifier>>{k}); }
    | '(' k1=ident { l.push_back(k1); } ( ',' kn=ident { l.push_back(kn); } )* ')' { $expr.add_key_aliases(l); }
    ;

cfamProperty[cql3::statements::cf_properties& expr]
    : property[*$expr.properties()]
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
createTypeStatement returns [std::unique_ptr<create_type_statement> expr]
    @init { bool if_not_exists = false; }
    : K_CREATE K_TYPE (K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
         tn=userTypeName { $expr = std::make_unique<create_type_statement>(tn, if_not_exists); }
         '(' typeColumns[*expr] ( ',' typeColumns[*expr]? )* ')'
    ;

typeColumns[create_type_statement& expr]
    : k=ident v=comparatorType { $expr.add_definition(k, v); }
    ;


/**
 * CREATE INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>);
 * CREATE CUSTOM INDEX [IF NOT EXISTS] [indexName] ON <columnFamily> (<columnName>) USING <indexClass>;
 */
createIndexStatement returns [std::unique_ptr<create_index_statement> expr]
    @init {
        auto props = make_shared<index_prop_defs>();
        bool if_not_exists = false;
        auto name = ::make_shared<cql3::index_name>();
        std::vector<::shared_ptr<index_target::raw>> targets;
    }
    : K_CREATE (K_CUSTOM { props->is_custom = true; })? K_INDEX (K_IF K_NOT K_EXISTS { if_not_exists = true; } )?
        (idxName[*name])? K_ON cf=columnFamilyName '(' (target1=indexIdent { targets.emplace_back(target1); } (',' target2=indexIdent { targets.emplace_back(target2); } )*)? ')'
        (K_USING cls=STRING_LITERAL { props->custom_class = sstring{$cls.text}; })?
        (K_WITH properties[*props])?
      { $expr = std::make_unique<create_index_statement>(cf, name, targets, props, if_not_exists); }
    ;

indexIdent returns [::shared_ptr<index_target::raw> id]
    @init {
        std::vector<::shared_ptr<cql3::column_identifier::raw>> columns;
    }
    : c=cident                   { $id = index_target::raw::regular_values_of(c); }
    | K_VALUES '(' c=cident ')'  { $id = index_target::raw::collection_values_of(c); }
    | K_KEYS '(' c=cident ')'    { $id = index_target::raw::keys_of(c); }
    | K_ENTRIES '(' c=cident ')' { $id = index_target::raw::keys_and_values_of(c); }
    | K_FULL '(' c=cident ')'    { $id = index_target::raw::full_collection(c); }
    | '(' c1=cident { columns.push_back(c1); } ( ',' cn=cident { columns.push_back(cn); } )* ')' { $id = index_target::raw::columns(std::move(columns)); }

    ;

/**
 * CREATE MATERIALIZED VIEW <viewName> AS
 *  SELECT <columns>
 *  FROM <CF>
 *  WHERE <pkColumns> IS NOT NULL
 *  PRIMARY KEY (<pkColumns>)
 *  WITH <property> = <value> AND ...;
 */
createViewStatement returns [std::unique_ptr<create_view_statement> expr]
    @init {
        bool if_not_exists = false;
        std::vector<::shared_ptr<cql3::column_identifier::raw>> partition_keys;
        std::vector<::shared_ptr<cql3::column_identifier::raw>> composite_keys;
        expression wclause = conjunction{};
    }
    : K_CREATE K_MATERIALIZED K_VIEW (K_IF K_NOT K_EXISTS { if_not_exists = true; })? cf=columnFamilyName K_AS
        K_SELECT sclause=selectClause K_FROM basecf=columnFamilyName
        (K_WHERE w=whereClause { wclause = std::move(w); } )?
        K_PRIMARY K_KEY (
        '(' '(' k1=cident { partition_keys.push_back(k1); } ( ',' kn=cident { partition_keys.push_back(kn); } )* ')' ( ',' c1=cident { composite_keys.push_back(c1); } )* ')'
    |   '(' k1=cident { partition_keys.push_back(k1); } ( ',' cn=cident { composite_keys.push_back(cn); } )* ')'
        )
        {
             $expr = std::make_unique<create_view_statement>(
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
alterKeyspaceStatement returns [std::unique_ptr<cql3::statements::alter_keyspace_statement> expr]
    @init {
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
    }
    : K_ALTER K_KEYSPACE ks=keyspaceName
        K_WITH properties[*attrs] { $expr = std::make_unique<cql3::statements::alter_keyspace_statement>(ks, attrs); }
    ;

/**
 * ALTER COLUMN FAMILY <CF> ALTER <column> TYPE <newtype>;
 * ALTER COLUMN FAMILY <CF> ADD <column> <newtype>; | ALTER COLUMN FAMILY <CF> ADD (<column> <newtype>,<column1> <newtype1>..... <column n> <newtype n>)
 * ALTER COLUMN FAMILY <CF> DROP <column>; | ALTER COLUMN FAMILY <CF> DROP ( <column>,<column1>.....<column n>)
 * ALTER COLUMN FAMILY <CF> WITH <property> = <value>;
 * ALTER COLUMN FAMILY <CF> RENAME <column> TO <column>;
 */
alterTableStatement returns [std::unique_ptr<alter_table_statement> expr]
    @init {
        alter_table_statement::type type;
        auto props = cql3::statements::cf_prop_defs();
        std::vector<alter_table_statement::column_change> column_changes;
        std::vector<std::pair<shared_ptr<cql3::column_identifier::raw>, shared_ptr<cql3::column_identifier::raw>>> renames;
    }
    : K_ALTER K_COLUMNFAMILY cf=columnFamilyName
          ( K_ALTER id=cident K_TYPE v=comparatorType { type = alter_table_statement::type::alter; column_changes.emplace_back(alter_table_statement::column_change{id, v}); }
          | K_ADD                                     { type = alter_table_statement::type::add; }
            (          id=cident   v=comparatorType   s=cfisStatic { column_changes.emplace_back(alter_table_statement::column_change{id,  v,  s});  }
            | '('     id1=cident  v1=comparatorType  s1=cfisStatic { column_changes.emplace_back(alter_table_statement::column_change{id1, v1, s1}); }
                 (',' idn=cident  vn=comparatorType  sn=cfisStatic { column_changes.emplace_back(alter_table_statement::column_change{idn, vn, sn}); } )* ')'
            )
          | K_DROP                                    { type = alter_table_statement::type::drop; }
            (          id=cident { column_changes.emplace_back(alter_table_statement::column_change{id});  }
            | '('     id1=cident { column_changes.emplace_back(alter_table_statement::column_change{id1}); }
                 (',' idn=cident { column_changes.emplace_back(alter_table_statement::column_change{idn}); } )* ')'
            )
          | K_WITH  properties[props]                 { type = alter_table_statement::type::opts; }
          | K_RENAME                                  { type = alter_table_statement::type::rename; }
               id1=cident K_TO toId1=cident { renames.emplace_back(id1, toId1); }
               ( K_AND idn=cident K_TO toIdn=cident { renames.emplace_back(idn, toIdn); } )*
          )
    {
        $expr = std::make_unique<alter_table_statement>(std::move(cf), type, std::move(column_changes), std::move(props), std::move(renames));
    }
    ;

cfisStatic returns [bool isStaticColumn]
    @init{
        bool isStatic = false;
    }
    : (K_STATIC { isStatic=true; })?
    {
        $isStaticColumn = isStatic;
    }
    ;

/**
 * ALTER TYPE <name> ALTER <field> TYPE <newtype>;
 * ALTER TYPE <name> ADD <field> <newtype>;
 * ALTER TYPE <name> RENAME <field> TO <newtype> AND ...;
 */
alterTypeStatement returns [std::unique_ptr<alter_type_statement> expr]
    : K_ALTER K_TYPE name=userTypeName
          ( K_ALTER f=ident K_TYPE v=comparatorType { $expr = std::make_unique<alter_type_statement::add_or_alter>(name, false, f, v); }
          | K_ADD   f=ident v=comparatorType        { $expr = std::make_unique<alter_type_statement::add_or_alter>(name, true, f, v); }
          | K_RENAME
               { $expr = std::make_unique<alter_type_statement::renames>(name); }
               renames[{ static_cast<alter_type_statement::renames&>(*$expr) }]
          )
    ;

/**
 * ALTER MATERIALIZED VIEW <CF> WITH <property> = <value>;
 */
alterViewStatement returns [std::unique_ptr<alter_view_statement> expr]
    @init {
        auto props = cql3::statements::cf_prop_defs();
    }
    : K_ALTER K_MATERIALIZED K_VIEW cf=columnFamilyName K_WITH properties[props]
    {
        $expr = std::make_unique<alter_view_statement>(std::move(cf), std::move(props));
    }
    ;

renames[alter_type_statement::renames& expr]
    : fromId=ident K_TO toId=ident { $expr.add_rename(fromId, toId); }
      ( K_AND renames[$expr] )?
    ;

/**
 * DROP KEYSPACE [IF EXISTS] <KSP>;
 */
dropKeyspaceStatement returns [std::unique_ptr<drop_keyspace_statement> ksp]
    @init { bool if_exists = false; }
    : K_DROP K_KEYSPACE (K_IF K_EXISTS { if_exists = true; } )? ks=keyspaceName { $ksp = std::make_unique<drop_keyspace_statement>(ks, if_exists); }
    ;

/**
 * DROP COLUMNFAMILY [IF EXISTS] <CF>;
 */
dropTableStatement returns [std::unique_ptr<drop_table_statement> stmt]
    @init { bool if_exists = false; }
    : K_DROP K_COLUMNFAMILY (K_IF K_EXISTS { if_exists = true; } )? cf=columnFamilyName { $stmt = std::make_unique<drop_table_statement>(cf, if_exists); }
    ;

/**
 * DROP TYPE <name>;
 */
dropTypeStatement returns [std::unique_ptr<drop_type_statement> stmt]
    @init { bool if_exists = false; }
    : K_DROP K_TYPE (K_IF K_EXISTS { if_exists = true; } )? name=userTypeName { $stmt = std::make_unique<drop_type_statement>(name, if_exists); }
    ;

/**
 * DROP MATERIALIZED VIEW [IF EXISTS] <view_name>
 */
dropViewStatement returns [std::unique_ptr<drop_view_statement> stmt]
    @init { bool if_exists = false; }
    : K_DROP K_MATERIALIZED K_VIEW (K_IF K_EXISTS { if_exists = true; } )? cf=columnFamilyName
      { $stmt = std::make_unique<drop_view_statement>(cf, if_exists); }
    ;

/**
 * DROP INDEX [IF EXISTS] <INDEX_NAME>
 */
dropIndexStatement returns [std::unique_ptr<drop_index_statement> expr]
    @init { bool if_exists = false; }
    : K_DROP K_INDEX (K_IF K_EXISTS { if_exists = true; } )? index=indexName
      { $expr = std::make_unique<drop_index_statement>(index, if_exists); }
    ;

/**
  * TRUNCATE [TABLE] <CF>
  * [USING TIMEOUT <duration>];
  */
truncateStatement returns [std::unique_ptr<raw::truncate_statement> stmt]
    @init {
        auto attrs = std::make_unique<cql3::attributes::raw>();
    }
    : K_TRUNCATE (K_COLUMNFAMILY)? cf=columnFamilyName
      ( usingTimeoutClause[attrs] )?
      {
        $stmt = std::make_unique<raw::truncate_statement>(std::move(cf), std::move(attrs));
      }
    ;

/**
 * GRANT <permission> ON <resource> TO <grantee>
 */
grantStatement returns [std::unique_ptr<grant_statement> stmt]
    : K_GRANT
          permissionOrAll
      K_ON
          resource
      K_TO
          grantee=userOrRoleName
      { $stmt = std::make_unique<grant_statement>($permissionOrAll.perms, $resource.res, std::move(grantee)); }
    ;

/**
 * REVOKE <permission> ON <resource> FROM <revokee>
 */
revokeStatement returns [std::unique_ptr<revoke_statement> stmt]
    : K_REVOKE
          permissionOrAll
      K_ON
          resource
      K_FROM
          revokee=userOrRoleName
      { $stmt = std::make_unique<revoke_statement>($permissionOrAll.perms, $resource.res, std::move(revokee)); }
    ;

/**
 * GRANT <rolename> to <grantee>
 */
grantRoleStatement returns [std::unique_ptr<grant_role_statement> stmt]
    : K_GRANT role=userOrRoleName K_TO grantee=userOrRoleName
      { $stmt = std::make_unique<grant_role_statement>(std::move(role), std::move(grantee));  }
    ;

/**
 * REVOKE <rolename> FROM <revokee>
 */
revokeRoleStatement returns [std::unique_ptr<revoke_role_statement> stmt]
    : K_REVOKE role=userOrRoleName K_FROM revokee=userOrRoleName
      { $stmt = std::make_unique<revoke_role_statement>(std::move(role), std::move(revokee)); }
    ;

listPermissionsStatement returns [std::unique_ptr<list_permissions_statement> stmt]
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
      { $stmt = std::make_unique<list_permissions_statement>($permissionOrAll.perms, std::move(r), std::move(role), recursive); }
    ;

permission returns [auth::permission perm]
    : p=(K_CREATE | K_ALTER | K_DROP | K_SELECT | K_MODIFY | K_AUTHORIZE | K_DESCRIBE | K_EXECUTE)
    { $perm = auth::permissions::from_string($p.text); }
    ;

permissionOrAll returns [auth::permission_set perms]
    : K_ALL ( K_PERMISSIONS )?       { $perms = auth::permissions::ALL; }
    | p=permission ( K_PERMISSION )? { $perms = auth::permission_set::from_mask(auth::permission_set::mask_for($p.perm)); }
    ;

resource returns [uninitialized<auth::resource> res]
    : d=dataResource     { $res = std::move(d); }
    | r=roleResource     { $res = std::move(r); }
    | f=functionResource { $res = std::move(f); }
    ;

dataResource returns [uninitialized<auth::resource> res]
    : K_ALL K_KEYSPACES { $res = auth::resource(auth::resource_kind::data); }
    | K_KEYSPACE ks = keyspaceName { $res = auth::make_data_resource($ks.id); }
    | ( K_COLUMNFAMILY )? cf = columnFamilyName
      { $res = auth::make_data_resource($cf.name.has_keyspace() ? $cf.name.get_keyspace() : "", $cf.name.get_column_family()); }
    ;

roleResource returns [uninitialized<auth::resource> res]
    : K_ALL K_ROLES { $res = auth::resource(auth::resource_kind::role); }
    | K_ROLE role = userOrRoleName { $res = auth::make_role_resource(static_cast<const cql3::role_name&>(role).to_string()); }
    ;

functionResource returns [uninitialized<auth::resource> res]
    @init {
        std::vector<shared_ptr<cql3_type::raw>> args_types;
    }
    : K_ALL K_FUNCTIONS { $res = auth::make_functions_resource(); }
    | K_ALL K_FUNCTIONS K_IN K_KEYSPACE ks = keyspaceName { $res = auth::make_functions_resource($ks.id); }
    | K_FUNCTION fn=functionName
      (
        '('
          (
            v=comparatorType { args_types.push_back(v); }
            ( ',' v=comparatorType { args_types.push_back(v); } )*
          )?
        ')'
      )
      { $res = auth::make_functions_resource($fn.s.keyspace, $fn.s.name, args_types); }
    ;

/**
 * CREATE USER [IF NOT EXISTS] <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
createUserStatement returns [std::unique_ptr<create_role_statement> stmt]
    @init {
        cql3::role_options opts;
        opts.is_superuser = false;
        opts.can_login = true;

        bool ifNotExists = false;
    }
    : K_CREATE K_USER (K_IF K_NOT K_EXISTS { ifNotExists = true; })? u=username
      ( K_WITH K_PASSWORD v=STRING_LITERAL { opts.password = $v.text; })?
      ( K_SUPERUSER { opts.is_superuser = true; } | K_NOSUPERUSER { opts.is_superuser = false; } )?
      { $stmt = std::make_unique<create_role_statement>(cql3::role_name(u, cql3::preserve_role_case::yes), std::move(opts), ifNotExists); }
    ;

/**
 * ALTER USER <username> [WITH PASSWORD <password>] [SUPERUSER|NOSUPERUSER]
 */
alterUserStatement returns [std::unique_ptr<alter_role_statement> stmt]
    @init {
        cql3::role_options opts;
    }
    : K_ALTER K_USER u=username
      ( K_WITH K_PASSWORD v=STRING_LITERAL { opts.password = $v.text; })?
      ( K_SUPERUSER { opts.is_superuser = true; } | K_NOSUPERUSER { opts.is_superuser = false; } )?
      { $stmt = std::make_unique<alter_role_statement>(cql3::role_name(u, cql3::preserve_role_case::yes), std::move(opts)); }
    ;

/**
 * DROP USER [IF EXISTS] <username>
 */
dropUserStatement returns [std::unique_ptr<drop_role_statement> stmt]
    @init { bool ifExists = false; }
    : K_DROP K_USER (K_IF K_EXISTS { ifExists = true; })? u=username
      { $stmt = std::make_unique<drop_role_statement>(cql3::role_name(u, cql3::preserve_role_case::yes), ifExists); }
    ;

/**
 * LIST USERS
 */
listUsersStatement returns [std::unique_ptr<list_users_statement> stmt]
    : K_LIST K_USERS { $stmt = std::make_unique<list_users_statement>(); }
    ;

/**
 * CREATE ROLE [IF NOT EXISTS] <role_name> [WITH <roleOption> [AND <roleOption>]*]
 */
createRoleStatement returns [std::unique_ptr<create_role_statement> stmt]
    @init {
        cql3::role_options opts;
        opts.is_superuser = false;
        opts.can_login = false;
        bool if_not_exists = false;
    }
    : K_CREATE K_ROLE (K_IF K_NOT K_EXISTS { if_not_exists = true; })? name=userOrRoleName
      (K_WITH roleOptions[opts])?
      { $stmt = std::make_unique<create_role_statement>(name, std::move(opts), if_not_exists); }
    ;

/**
 * ALTER ROLE <rolename> [WITH <roleOption> [AND <roleOption>]*]
 */
alterRoleStatement returns [std::unique_ptr<alter_role_statement> stmt]
    @init {
        cql3::role_options opts;
    }
    : K_ALTER K_ROLE name=userOrRoleName
      (K_WITH roleOptions[opts])?
      { $stmt = std::make_unique<alter_role_statement>(name, std::move(opts)); }
    ;

/**
 * DROP ROLE [IF EXISTS] <rolename>
 */
dropRoleStatement returns [std::unique_ptr<drop_role_statement> stmt]
    @init {
        bool if_exists = false;
    }
    : K_DROP K_ROLE (K_IF K_EXISTS { if_exists = true; })? name=userOrRoleName
      { $stmt = std::make_unique<drop_role_statement>(name, if_exists); }
    ;

/**
 * LIST ROLES [OF <rolename>] [NORECURSIVE]
 */
listRolesStatement returns [std::unique_ptr<list_roles_statement> stmt]
    @init {
        bool recursive = true;
        std::optional<cql3::role_name> grantee;
    }
    : K_LIST K_ROLES
        (K_OF g=userOrRoleName { grantee = std::move(g); })?
        (K_NORECURSIVE { recursive = false; })?
        { $stmt = std::make_unique<list_roles_statement>(grantee, recursive); }
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

// Introduce a more natural syntax (SERVICE LEVEL), but still allow
// the original one (SERVICE_LEVEL)
serviceLevel
    : K_SERVICE_LEVEL | ( K_SERVICE K_LEVEL )
    ;
serviceLevels
    : K_SERVICE_LEVELS | ( K_SERVICE K_LEVELS )
    ;
/**
 * CREATE SERVICE_LEVEL [IF NOT EXISTS] <service_level_name> [WITH <param> = <value>]
 */
createServiceLevelStatement returns [std::unique_ptr<create_service_level_statement> stmt]
    @init {
        auto attrs = make_shared<cql3::statements::sl_prop_defs>();
        bool if_not_exists = false;
    }
    : K_CREATE serviceLevel (K_IF K_NOT K_EXISTS { if_not_exists = true; })? name=serviceLevelOrRoleName (K_WITH properties[*attrs])?
      { $stmt = std::make_unique<create_service_level_statement>(name, attrs, if_not_exists); }
    ;

/**
 * ALTER SERVICE_LEVEL <service_level_name> WITH <param> = <value>
 */
alterServiceLevelStatement returns [std::unique_ptr<alter_service_level_statement> stmt]
    @init {
        auto attrs = make_shared<cql3::statements::sl_prop_defs>();
    }
    : K_ALTER serviceLevel name=serviceLevelOrRoleName K_WITH properties[*attrs]
      { $stmt = std::make_unique<alter_service_level_statement>(name, attrs); }
    ;

/**
 * DROP SERVICE_LEVEL [IF EXISTS] <service_level_name>
 */
dropServiceLevelStatement returns [std::unique_ptr<drop_service_level_statement> stmt]
    @init {
        bool if_exists = false;
    }
    : K_DROP serviceLevel (K_IF K_EXISTS { if_exists = true; })? name=serviceLevelOrRoleName
      { $stmt = std::make_unique<drop_service_level_statement>(name, if_exists); }
    ;

/**
 * ATTACH SERVICE_LEVEL <service_level_name> TO <role_name>
 */
attachServiceLevelStatement returns [std::unique_ptr<attach_service_level_statement> stmt]
    @init {
    }
    : K_ATTACH serviceLevel service_level_name=serviceLevelOrRoleName K_TO role_name=serviceLevelOrRoleName
      { $stmt = std::make_unique<attach_service_level_statement>(service_level_name, role_name); }
    ;

/**
 * DETACH SERVICE_LEVEL FROM <role_name>
 */
detachServiceLevelStatement returns [std::unique_ptr<detach_service_level_statement> stmt]
    @init {
    }
    : K_DETACH serviceLevel K_FROM role_name=serviceLevelOrRoleName
      { $stmt = std::make_unique<detach_service_level_statement>(role_name); }
    ;


/**
 * LIST SERVICE_LEVEL <service_level_name>
 * LIST ALL SERVICE_LEVELS
 */
listServiceLevelStatement returns [std::unique_ptr<list_service_level_statement> stmt]
    @init {
    }
    : K_LIST serviceLevel service_level_name=serviceLevelOrRoleName
      { $stmt = std::make_unique<list_service_level_statement>(service_level_name, false); } |
      K_LIST K_ALL serviceLevels
      { $stmt = std::make_unique<list_service_level_statement>("", true); }
    ;

/**
 * LIST ATTACHED SERVICE_LEVEL OF <role_name>
 * LIST ALL ATTACHED SERVICE_LEVELS
 */
listServiceLevelAttachStatement returns [std::unique_ptr<list_service_level_attachments_statement> stmt]
    @init {
        bool allow_nonexisting_roles = false;
    }
    : K_LIST K_ATTACHED serviceLevel K_OF role_name=serviceLevelOrRoleName
      { $stmt = std::make_unique<list_service_level_attachments_statement>(role_name); } |
      K_LIST K_ALL K_ATTACHED serviceLevels
      { $stmt = std::make_unique<list_service_level_attachments_statement>(); }
    ;

/**
 * (DESCRIBE | DESC) (
 *    CLUSTER
 *    [FULL] SCHEMA
 *    KEYSPACES
 *    [ONLY] KEYSPACE <name>?
 *    TABLES
 *    TABLE <name>
 *    TYPES
 *    TYPE <name>
 *    FUNCTIONS
 *    FUNCTION <name>
 *    AGGREGATES
 *    AGGREGATE <name>
 * ) (WITH INTERNALS)?
 */
describeStatement returns [std::unique_ptr<cql3::statements::raw::describe_statement> stmt]
    @init {
        bool fullSchema = false;
        bool pending = false;
        bool config = false;
        bool only = false;
        std::optional<sstring> keyspace;
        sstring generic_name = "";
    }
    : ( K_DESCRIBE | K_DESC )
    ( (K_CLUSTER) => K_CLUSTER                      { $stmt = cql3::statements::raw::describe_statement::cluster();                }
    | (K_FULL { fullSchema=true; })? K_SCHEMA       { $stmt = cql3::statements::raw::describe_statement::schema(fullSchema);       }
    | (K_KEYSPACES) => K_KEYSPACES                  { $stmt = cql3::statements::raw::describe_statement::keyspaces();              }
    | (K_ONLY { only=true; })? K_KEYSPACE ( ks=keyspaceName { keyspace = ks; })?
                                                    { $stmt = cql3::statements::raw::describe_statement::keyspace(keyspace, only); }
    | (K_TABLES) => K_TABLES                        { $stmt = cql3::statements::raw::describe_statement::tables();                 }
    | K_COLUMNFAMILY cf=columnFamilyName            { $stmt = cql3::statements::raw::describe_statement::table(cf);                }
    | K_INDEX idx=columnFamilyName                  { $stmt = cql3::statements::raw::describe_statement::index(idx);               }
    | K_MATERIALIZED K_VIEW view=columnFamilyName   { $stmt = cql3::statements::raw::describe_statement::view(view);               }
    | (K_TYPES) => K_TYPES                          { $stmt = cql3::statements::raw::describe_statement::types();                  }
    | K_TYPE tn=userTypeName                        { $stmt = cql3::statements::raw::describe_statement::type(tn);                 }
    | (K_FUNCTIONS) => K_FUNCTIONS                  { $stmt = cql3::statements::raw::describe_statement::functions();              }
    | K_FUNCTION fn=functionName                    { $stmt = cql3::statements::raw::describe_statement::function(fn);             }
    | (K_AGGREGATES) => K_AGGREGATES                { $stmt = cql3::statements::raw::describe_statement::aggregates();             }
    | K_AGGREGATE ag=functionName                   { $stmt = cql3::statements::raw::describe_statement::aggregate(ag);            }
    | ( ( ksT=IDENT                                 { keyspace = sstring{$ksT.text}; }
        | ksT=QUOTED_NAME                           { keyspace = sstring{$ksT.text}; }
        | ksK=unreserved_keyword                    { keyspace = ksK; } ) 
        '.' )?
        ( tT=IDENT                                  { generic_name = sstring{$tT.text}; }
        | tT=QUOTED_NAME                            { generic_name = sstring{$tT.text}; }
        | tK=unreserved_keyword                     { generic_name = tK; } )
                                                    { $stmt = cql3::statements::raw::describe_statement::generic(keyspace, generic_name); }
    )
    ( K_WITH K_INTERNALS { $stmt->with_internals_details(); } )?
    ;

/** DEFINITIONS **/

// Column Identifiers.  These need to be treated differently from other
// identifiers because the underlying comparator is not necessarily text. See
// CASSANDRA-8178 for details.
cident returns [shared_ptr<cql3::column_identifier::raw> id]
    : t=IDENT              { $id = ::make_shared<cql3::column_identifier::raw>(sstring{$t.text}, false); }
    | t=QUOTED_NAME        { $id = ::make_shared<cql3::column_identifier::raw>(sstring{$t.text}, true); }
    | k=unreserved_keyword { $id = ::make_shared<cql3::column_identifier::raw>(k, false); }
    ;

// Identifiers that do not refer to columns or where the comparator is known to be text
ident returns [shared_ptr<cql3::column_identifier> id]
    : t=IDENT              { $id = ::make_shared<cql3::column_identifier>(sstring{$t.text}, false); }
    | t=QUOTED_NAME        { $id = ::make_shared<cql3::column_identifier>(sstring{$t.text}, true); }
    | k=unreserved_keyword { $id = ::make_shared<cql3::column_identifier>(k, false); }
    ;

// Keyspace & Column family names
keyspaceName returns [sstring id]
    @init { auto name = cql3::cf_name(); }
    : ksName[name] { $id = name.get_keyspace(); }
    ;

indexName returns [::shared_ptr<cql3::index_name> name]
    @init { $name = ::make_shared<cql3::index_name>(); }
    : (ksName[*name] '.')? idxName[*name]
    ;

columnFamilyName returns [cql3::cf_name name]
    @init { $name = cql3::cf_name(); }
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
    
serviceLevelOrRoleName returns [sstring name]
: t=IDENT              { $name = sstring($t.text);
						 std::transform($name.begin(), $name.end(), $name.begin(), ::tolower); }
| t=STRING_LITERAL     { $name = sstring($t.text); }
| t=QUOTED_NAME        { $name = sstring($t.text); }
| k=unreserved_keyword { $name = k;
						 std::transform($name.begin(), $name.end(), $name.begin(), ::tolower);}
| QMARK {add_recognition_error("Bind variables cannot be used for service levels or role names");}
;

ksName[cql3::keyspace_element_name& name]
    : t=IDENT              { $name.set_keyspace($t.text, false);}
    | t=QUOTED_NAME        { $name.set_keyspace($t.text, true);}
    | k=unreserved_keyword { $name.set_keyspace(k, false);}
    | QMARK {add_recognition_error("Bind variables cannot be used for keyspace names");}
    ;

cfName[cql3::cf_name& name]
    : t=IDENT              { $name.set_column_family($t.text, false); }
    | t=QUOTED_NAME        { $name.set_column_family($t.text, true); }
    | k=unreserved_keyword { $name.set_column_family(k, false); }
    | QMARK {add_recognition_error("Bind variables cannot be used for table names");}
    ;

idxName[cql3::index_name& name]
    : t=IDENT              { $name.set_index($t.text, false); }
    | t=QUOTED_NAME        { $name.set_index($t.text, true);}
    | k=unreserved_keyword { $name.set_index(k, false); }
    | QMARK {add_recognition_error("Bind variables cannot be used for index names");}
    ;

constant returns [untyped_constant constant]
    @init{std::string sign;}
    : t=STRING_LITERAL {
                    // This is a workaround for antlr3 not distinguishing between
                    // calling in lexer setText() with an empty string and not calling
                    // setText() at all.
                    auto text = $t.text;
                    if (text.size() == 1 && text[0] == '\xFF') {
                        text = {};
                    }
                    $constant = untyped_constant{untyped_constant::string, std::move(text)};
                }
    | t=INTEGER        { $constant = untyped_constant{untyped_constant::integer, $t.text}; }
    | t=FLOAT          { $constant = untyped_constant{untyped_constant::floating_point, $t.text}; }
    | t=BOOLEAN        { $constant = untyped_constant{untyped_constant::boolean, $t.text}; }
    | t=DURATION       { $constant = untyped_constant{untyped_constant::duration, $t.text}; }
    | t=UUID           { $constant = untyped_constant{untyped_constant::uuid, $t.text}; }
    | t=HEXNUMBER      { $constant = untyped_constant{untyped_constant::hex, $t.text}; }
    | { sign=""; } ('-' {sign = "-"; } )? t=(K_NAN | K_INFINITY) { $constant = untyped_constant{untyped_constant::floating_point, sign + $t.text}; }
    ;

mapLiteral returns [collection_constructor map]
    @init{std::vector<expression> m;}
    : '{' { }
          ( k1=term ':' v1=term { m.push_back(tuple_constructor{{std::move(k1), std::move(v1)}}); }
            ( ',' kn=term ':' vn=term { m.push_back(tuple_constructor{{std::move(kn), std::move(vn)}}); } )*  )?
      '}' { $map = collection_constructor{collection_constructor::style_type::map, std::move(m)}; }
    ;

setOrMapLiteral[expression t] returns [collection_constructor value]
	@init{ std::vector<expression> e; }
    : ':' v=term { e.push_back(tuple_constructor{{std::move(t), std::move(v)}}); }
          ( ',' kn=term ':' vn=term { e.push_back(tuple_constructor{{std::move(kn), std::move(vn)}}); } )*
      { $value = collection_constructor{collection_constructor::style_type::map, std::move(e)}; }
    | { e.push_back(std::move(t)); }
          ( ',' tn=term { e.push_back(std::move(tn)); } )*
      { $value = collection_constructor{collection_constructor::style_type::set, std::move(e)}; }
    ;

collectionLiteral returns [expression value]
	@init{ std::vector<expression> l; }
    : '['
          ( t1=term { l.push_back(std::move(t1)); } ( ',' tn=term { l.push_back(std::move(tn)); } )* )?
      ']' { $value = collection_constructor{collection_constructor::style_type::list, std::move(l)}; }
    | '{' t=term v=setOrMapLiteral[t] { $value = std::move(v); } '}'
    // Note that we have an ambiguity between maps and set for "{}". So we force it to a set literal,
    // and deal with it later based on the type of the column (SetLiteral.java).
    | '{' '}' { $value = collection_constructor{collection_constructor::style_type::set, {}}; }
    ;

usertypeLiteral returns [expression ut]
    @init{ usertype_constructor::elements_map_type m; }
    @after{ $ut = usertype_constructor{std::move(m)}; }
    // We don't allow empty literals because that conflicts with sets/maps and is currently useless since we don't allow empty user types
    : '{' k1=ident ':' v1=term { m.emplace(std::move(*k1), std::move(v1)); } ( ',' kn=ident ':' vn=term { m.emplace(std::move(*kn), std::move(vn)); } )* '}'
    ;

tupleLiteral returns [expression tt]
    @init{ std::vector<expression> l; }
    @after{ $tt = tuple_constructor{std::move(l)}; }
    : '(' t1=term { l.push_back(std::move(t1)); } ( ',' tn=term { l.push_back(std::move(tn)); } )* ')'
    ;

value returns [expression value]
    : c=constant           { $value = std::move(c); }
    | l=collectionLiteral  { $value = std::move(l); }
    | u=usertypeLiteral    { $value = std::move(u); }
    | t=tupleLiteral       { $value = std::move(t); }
    | K_NULL               { $value = make_untyped_null(); }
    | e=marker             { $value = std::move(e); }
    ;

marker returns [expression value]
    : ':' id=ident         { $value = new_bind_variables(id); }
    | QMARK                { $value = new_bind_variables(shared_ptr<cql3::column_identifier>{}); }
    ;

intValue returns [expression value]
    :
    | t=INTEGER     { $value = untyped_constant{untyped_constant::integer, $t.text}; }
    | e=marker      { $value = std::move(e); }
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

functionArgs returns [std::vector<expression> a]
    : '(' ')'
    | '(' t1=term { a.push_back(std::move(t1)); }
          ( ',' tn=term { a.push_back(std::move(tn)); } )*
       ')'
    ;

term returns [expression term1]
    : v=value                          { $term1 = std::move(v); }
    | f=functionName args=functionArgs { $term1 = function_call{std::move(f), std::move(args)}; }
    | '(' c=comparatorType ')' t=term  { $term1 = cast{.style = cast::cast_style::c, .arg = std::move(t), .type = c}; }
    ;

columnOperation[operations_type& operations]
    : key=cident columnOperationDifferentiator[operations, key]
    ;

columnOperationDifferentiator[operations_type& operations, ::shared_ptr<cql3::column_identifier::raw> key]
    : '=' normalColumnOperation[operations, key]
    | '[' k=term ']' collectionColumnOperation[operations, key, k, false]
    | '.' field=ident udtColumnOperation[operations, key, field]
    | '[' K_SCYLLA_TIMEUUID_LIST_INDEX '(' k=term ')' ']' collectionColumnOperation[operations, key, k, true]
    ;

normalColumnOperation[operations_type& operations, ::shared_ptr<cql3::column_identifier::raw> key]
    : t=term ('+' c=cident )?
      {
          if (!c) {
              operations.emplace_back(std::move(key), std::make_unique<cql3::operation::set_value>(t));
          } else {
              if (*key != *c) {
                add_recognition_error("Only expressions of the form X = <value> + X are supported.");
              }
              operations.emplace_back(std::move(key), std::make_unique<cql3::operation::prepend>(t));
          }
      }
    | c=cident sig=('+' | '-') t=term
      {
          if (*key != *c) {
              add_recognition_error("Only expressions of the form X = X " + $sig.text + "<value> are supported.");
          }
          std::unique_ptr<cql3::operation::raw_update> op;
          if ($sig.text == "+") {
              op = std::make_unique<cql3::operation::addition>(t);
          } else {
              op = std::make_unique<cql3::operation::subtraction>(t);
          }
          operations.emplace_back(std::move(key), std::move(op));
      }
    | c=cident i=INTEGER
      {
          // Note that this production *is* necessary because X = X - 3 will in fact be lexed as [ X, '=', X, INTEGER].
          if (*key != *c) {
              // We don't yet allow a '+' in front of an integer, but we could in the future really, so let's be future-proof in our error message
              add_recognition_error("Only expressions of the form X = X " + sstring($i.text[0] == '-' ? "-" : "+") + " <value> are supported.");
          }
          operations.emplace_back(std::move(key), std::make_unique<cql3::operation::addition>(untyped_constant{untyped_constant::integer, $i.text}));
      }
    | K_SCYLLA_COUNTER_SHARD_LIST '(' t=term ')'
      {
          operations.emplace_back(std::move(key), std::make_unique<cql3::operation::set_counter_value_from_tuple_list>(t));
      }
    ;

collectionColumnOperation[operations_type& operations,
                          shared_ptr<cql3::column_identifier::raw> key,
                          expression k,
                          bool by_uuid]
    : '=' t=term
      {
          operations.emplace_back(std::move(key), std::make_unique<cql3::operation::set_element>(std::move(k), std::move(t), by_uuid));
      }
    ;

udtColumnOperation[operations_type& operations,
                   shared_ptr<cql3::column_identifier::raw> key,
                   shared_ptr<cql3::column_identifier> field]
    : '=' t=term
      {
          operations.emplace_back(std::move(key), std::make_unique<cql3::operation::set_field>(std::move(field), std::move(t)));
      }
    ;

columnRefExpr returns [expression e]
    : column=cident { e = unresolved_identifier{column}; }
    ;

subscriptExpr returns [expression e]
    : col=columnRefExpr { e = std::move(col); }
        ( '[' sub=term ']'  { e = subscript{std::move(e), std::move(sub)}; } )?
    ;

singleColumnInValuesOrMarkerExpr returns [expression e]
    : values=singleColumnInValues { e = collection_constructor{collection_constructor::style_type::list, std::move(values)}; }
    | m=marker { e = std::move(m); }
    ;

columnCondition returns [expression e]
    // Note: we'll reject duplicates later
    : key=subscriptExpr
        ( op=relationType t=term {
                    e = binary_operator(
                            key,
                            op,
                            t);
                }
        | K_IN
            values=singleColumnInValuesOrMarkerExpr {
                    e = binary_operator(
                            key,
                            oper_t::IN,
                            std::move(values));
                }
        )
    ;

properties[cql3::statements::property_definitions& props]
    : property[props] (K_AND property[props])*
    ;

property[cql3::statements::property_definitions& props]
    : k=ident '=' simple=propertyValue { try { $props.add_property(k->to_string(), simple); } catch (exceptions::syntax_exception e) { add_recognition_error(e.what()); } }
    | k=ident '=' map=mapLiteral { try { $props.add_property(k->to_string(), convert_property_map(map)); } catch (exceptions::syntax_exception e) { add_recognition_error(e.what()); } }
    ;

propertyValue returns [sstring str]
    : c=constant           { $str = c.raw_text; }
    // FIXME: unreserved keywords below are indistinguishable from their string representation,
    // which might be problematic in the future. A possible solution is to use a more complicated
    // type for storing property values instead of just plain strings. For the specific case
    // of "null" it would be enough to use an optional, but for the general case it should be
    // a variant-like class which distinguishes plain string values from special keywords
    | u=unreserved_keyword { $str = u; }
    | K_NULL { $str = "null"; }
    ;

relationType returns [oper_t op]
    : '='  { $op = oper_t::EQ; }
    | '<'  { $op = oper_t::LT; }
    | '<=' { $op = oper_t::LTE; }
    | '>'  { $op = oper_t::GT; }
    | '>=' { $op = oper_t::GTE; }
    | '!=' { $op = oper_t::NEQ; }
    | K_LIKE { $op = oper_t::LIKE; }
    ;

relation returns [expression e]
    @init{ oper_t rt; }
    : name=cident type=relationType t=term { $e = binary_operator(unresolved_identifier{std::move(name)}, type, std::move(t)); }

    | K_TOKEN l=tupleOfIdentifiers type=relationType t=term
        {
          $e = binary_operator(
            function_call{functions::function_name::native_function("token"), std::move(l.elements)},
            type,
            std::move(t));
        }
    | name=cident K_IS K_NOT K_NULL {
          $e = binary_operator(unresolved_identifier{std::move(name)}, oper_t::IS_NOT, make_untyped_null()); }
    | name=cident K_IN marker1=marker
        { $e = binary_operator(unresolved_identifier{std::move(name)}, oper_t::IN, std::move(marker1)); }
    | name=cident K_IN in_values=singleColumnInValues
        { $e = binary_operator(unresolved_identifier{std::move(name)}, oper_t::IN,
        collection_constructor {
            .style = collection_constructor::style_type::list,
            .elements = std::move(in_values)
        }); }
    | name=cident K_CONTAINS { rt = oper_t::CONTAINS; } (K_KEY { rt = oper_t::CONTAINS_KEY; })?
        t=term { $e = binary_operator(unresolved_identifier{std::move(name)}, rt, std::move(t)); }
    | name=cident '[' key=term ']' type=relationType t=term { $e = binary_operator(subscript{.val = unresolved_identifier{std::move(name)}, .sub = std::move(key)}, type, std::move(t)); }
    | ids=tupleOfIdentifiers
      ( K_IN
          ( '(' ')'
              {
                $e = binary_operator(
                    ids,
                    oper_t::IN,
                    collection_constructor {
                      .style = collection_constructor::style_type::list,
                      .elements = std::vector<expression>()
                    }
                  );
              }
          | tupleInMarker=marker /* (a, b, c) IN ? */
              {
                $e = binary_operator(
                    ids,
                    oper_t::IN,
                    std::move(tupleInMarker)
                  );
              }
          | literals=tupleOfTupleLiterals /* (a, b, c) IN ((1, 2, 3), (4, 5, 6), ...) */
              {
                $e = binary_operator(
                    ids,
                    oper_t::IN,
                    collection_constructor {
                      .style = collection_constructor::style_type::list,
                      .elements = std::move(literals)
                    }
                  );
              }
          | markers=tupleOfMarkersForTuples /* (a, b, c) IN (?, ?, ...) */
              {
                $e = binary_operator(
                    ids,
                    oper_t::IN,
                    collection_constructor {
                      .style = collection_constructor::style_type::list,
                      .elements = std::move(markers)
                    }
                  );
              }
          )
      | type=relationType literal=tupleLiteral /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
          {
              $e = binary_operator(ids, type, std::move(literal));
          }
      | type=relationType K_SCYLLA_CLUSTERING_BOUND literal=tupleLiteral /* (a, b, c) > (1, 2, 3) or (a, b, c) > (?, ?, ?) */
          {
              $e = binary_operator(ids, type, std::move(literal), cql3::expr::comparison_order::clustering);
          }
      | type=relationType tupleMarker=marker /* (a, b, c) >= ? */
          {
              $e = binary_operator(ids, type, std::move(tupleMarker));
          }
      )
    | '(' e1=relation ')' { $e = std::move(e1); }
    ;

tupleOfIdentifiers returns [tuple_constructor tup]
    : '(' n1=cident { $tup.elements.push_back(unresolved_identifier{std::move(n1)}); } (',' ni=cident { $tup.elements.push_back(unresolved_identifier{std::move(ni)}); })* ')'
    ;

listOfIdentifiers returns [std::vector<::shared_ptr<cql3::column_identifier::raw>> ids]
    : n1=cident { $ids.push_back(n1); } (',' ni=cident { $ids.push_back(ni); })*
    ;

singleColumnInValues returns [std::vector<expression> list]
    : '(' ( t1 = term { $list.push_back(std::move(t1)); } (',' ti=term { $list.push_back(std::move(ti)); })* )? ')'
    ;

tupleOfTupleLiterals returns [std::vector<expression> literals]
    : '(' t1=tupleLiteral { $literals.emplace_back(std::move(t1)); } (',' ti=tupleLiteral { $literals.emplace_back(std::move(ti)); })* ')'
    ;

tupleOfMarkersForTuples returns [std::vector<expression> markers]
    : '(' m1=marker { $markers.emplace_back(std::move(m1)); } (',' mi=marker { $markers.emplace_back(std::move(mi)); })* ')'
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

native_or_internal_type [bool internal] returns [data_type t]
    : n=native_type     { $t = n; }
    // The "internal" types, only supported when internal==true:
    | K_EMPTY   {
        if (internal) {
            $t = empty_type;
        } else {
            add_recognition_error("Invalid (reserved) user type name empty");
        }
      }
    ;

comparatorType returns [shared_ptr<cql3_type::raw> t]
    : tt=comparator_type[false]    { $t = tt; }
    ;

native_type returns [data_type t]
    : K_ASCII     { $t = ascii_type; }
    | K_BIGINT    { $t = long_type; }
    | K_BLOB      { $t = bytes_type; }
    | K_BOOLEAN   { $t = boolean_type; }
    | K_COUNTER   { $t = counter_type; }
    | K_DECIMAL   { $t = decimal_type; }
    | K_DOUBLE    { $t = double_type; }
    | K_DURATION  { $t = duration_type; }
    | K_FLOAT     { $t = float_type; }
    | K_INET      { $t = inet_addr_type; }
    | K_INT       { $t = int32_type; }
    | K_SMALLINT  { $t = short_type; }
    | K_TEXT      { $t = utf8_type; }
    | K_TIMESTAMP { $t = timestamp_type; }
    | K_TINYINT   { $t = byte_type; }
    | K_UUID      { $t = uuid_type; }
    | K_VARCHAR   { $t = utf8_type; }
    | K_VARINT    { $t = varint_type; }
    | K_TIMEUUID  { $t = timeuuid_type; }
    | K_DATE      { $t = simple_date_type; }
    | K_TIME      { $t = time_type; }
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

username returns [sstring str]
    : t=IDENT { $str = $t.text; }
    | t=STRING_LITERAL { $str = $t.text; }
    | s=unreserved_keyword { $str = s; }
    | QUOTED_NAME { add_recognition_error("Quoted strings are not supported for user names"); }
    ;

// Basically the same as cident, but we need to exlude existing CQL3 types
// (which for some reason are not reserved otherwise)
non_type_ident returns [shared_ptr<cql3::column_identifier> id]
    : t=IDENT                    { if (_reserved_type_names().contains($t.text)) { add_recognition_error("Invalid (reserved) user type name " + $t.text); } $id = ::make_shared<cql3::column_identifier>($t.text, false); }
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
    | u=type_unreserved_keyword  { $str = u; }
    ;

basic_unreserved_keyword returns [sstring str]
    : k=( K_KEYS
        | K_AS
        | K_CLUSTER
        | K_CLUSTERING
        | K_COMPACT
        | K_STORAGE
        | K_TABLES
        | K_TYPE
        | K_TYPES
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
        | K_INTERNALS
        | K_STATIC
        | K_FROZEN
        | K_TUPLE
        | K_FUNCTION
        | K_FUNCTIONS
        | K_AGGREGATE
        | K_AGGREGATES
        | K_SFUNC
        | K_STYPE
        | K_REDUCEFUNC
        | K_FINALFUNC
        | K_INITCOND
        | K_RETURNS
        | K_LANGUAGE
        | K_CALLED
        | K_INPUT
        | K_JSON
        | K_CACHE
        | K_BYPASS
        | K_LIKE
        | K_PER
        | K_PARTITION
        | K_SERVICE_LEVEL
        | K_ATTACH
        | K_DETACH
        | K_SERVICE_LEVELS
        | K_ATTACHED
        | K_FOR
        | K_GROUP
        | K_TIMEOUT
        | K_SERVICE
        | K_LEVEL
        | K_LEVELS
        | K_PRUNE
        | K_ONLY
        | K_DESCRIBE
        | K_DESC
        | K_EXECUTE
        | K_MUTATION_FRAGMENTS
        ) { $str = $k.text; }
    ;

type_unreserved_keyword returns [sstring str]
    : k=( K_ASCII
        | K_BIGINT
        | K_BLOB
        | K_BOOLEAN
        | K_COUNTER
        | K_DECIMAL
        | K_DOUBLE
        | K_DURATION
        | K_FLOAT
        | K_INET
        | K_INT
        | K_SMALLINT
        | K_TEXT
        | K_TIMESTAMP
        | K_TINYINT
        | K_UUID
        | K_VARCHAR
        | K_VARINT
        | K_TIMEUUID
        | K_DATE
        | K_TIME
        | K_EMPTY
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
K_SCHEMA:      S C H E M A;
K_KEYSPACE:    ( K E Y S P A C E
                 | K_SCHEMA );
K_KEYSPACES:   K E Y S P A C E S;
K_COLUMNFAMILY:( C O L U M N F A M I L Y
                 | T A B L E );
K_TABLES:      ( C O L U M N F A M I L I E S
                 | T A B L E S );
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
K_TYPES:       T Y P E S;
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
K_INTERNALS:   I N T E R N A L S;
K_ONLY:        O N L Y;

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

K_CLUSTER:     C L U S T E R;
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
K_FUNCTIONS:   F U N C T I O N S;
K_AGGREGATE:   A G G R E G A T E;
K_AGGREGATES:  A G G R E G A T E S;
K_SFUNC:       S F U N C;
K_STYPE:       S T Y P E;
K_REDUCEFUNC:  R E D U C E F U N C;
K_FINALFUNC:   F I N A L F U N C;
K_INITCOND:    I N I T C O N D;
K_RETURNS:     R E T U R N S;
K_CALLED:      C A L L E D;
K_INPUT:       I N P U T;
K_LANGUAGE:    L A N G U A G E;
K_OR:          O R;
K_REPLACE:     R E P L A C E;
K_JSON:        J S O N;
K_DEFAULT:     D E F A U L T;
K_UNSET:       U N S E T;

K_EMPTY:       E M P T Y;

K_BYPASS:      B Y P A S S;
K_CACHE:       C A C H E;

K_PER:         P E R;
K_PARTITION:   P A R T I T I O N;

K_SERVICE_LEVEL: S E R V I C E '_' L E V E L;
K_ATTACH: A T T A C H;
K_DETACH: D E T A C H;
K_SERVICE_LEVELS: S E R V I C E '_' L E V E L S;
K_ATTACHED: A T T A C H E D;
K_FOR: F O R;
K_SERVICE: S E R V I C E;
K_LEVEL: L E V E L;
K_LEVELS: L E V E L S;

K_SCYLLA_TIMEUUID_LIST_INDEX: S C Y L L A '_' T I M E U U I D '_' L I S T '_' I N D E X;
K_SCYLLA_COUNTER_SHARD_LIST: S C Y L L A '_' C O U N T E R '_' S H A R D '_' L I S T; 
K_SCYLLA_CLUSTERING_BOUND: S C Y L L A '_' C L U S T E R I N G '_' B O U N D;


K_GROUP:       G R O U P;

K_LIKE:        L I K E;

K_TIMEOUT:     T I M E O U T;
K_PRUNE:       P R U N E;

K_EXECUTE:     E X E C U T E;

K_MUTATION_FRAGMENTS:    M U T A T I O N '_' F R A G M E N T S;

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
