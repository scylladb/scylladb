include(generate_cql_grammar)

find_package(ANTLR3 REQUIRED)

generate_cql_grammar(
  GRAMMAR Cql.g
  SOURCES cql_grammar_srcs)
set_source_files_properties(${cql_grammar_srcs}
  PROPERTIES
    COMPILE_OPTIONS "-Wno-uninitialized;-Wno-parentheses-equality")

set(cql_parser_srcs ${cql_grammar_srcs})
list(FILTER cql_parser_srcs INCLUDE REGEX "Parser.cpp$")

set(unoptimized_modes "Coverage,Debug,Sanitize")
set(sanitized_modes "Debug,Sanitize")
set_property(
    SOURCE ${cql_parser_srcs}
    APPEND
    PROPERTY COMPILE_OPTIONS
      # Unoptimized parsers end up using huge amounts of stack space and
      # overflowing their stack
      $<$<CONFIG:${unoptimized_modes}>:-O1>
      # use-after-scope sanitizer also uses large amount of stack space
      # and overflows the stack of CqlParser
      $<$<CONFIG:${sanitized_modes}>:-fsanitize-address-use-after-scope>)

add_library(cql3 STATIC)
target_sources(cql3
  PRIVATE
    type_json.cc
    attributes.cc
    cf_name.cc
    cql3_type.cc
    operation.cc
    index_name.cc
    keyspace_element_name.cc
    lists.cc
    sets.cc
    maps.cc
    values.cc
    expr/expression.cc
    expr/restrictions.cc
    expr/prepare_expr.cc
    functions/user_function.cc
    functions/functions.cc
    functions/aggregate_fcts.cc
    functions/castas_fcts.cc
    functions/error_injection_fcts.cc
    statements/cf_prop_defs.cc
    statements/cf_statement.cc
    statements/authentication_statement.cc
    statements/create_keyspace_statement.cc
    statements/create_table_statement.cc
    statements/create_view_statement.cc
    statements/create_type_statement.cc
    statements/create_function_statement.cc
    statements/create_aggregate_statement.cc
    statements/drop_index_statement.cc
    statements/drop_keyspace_statement.cc
    statements/drop_table_statement.cc
    statements/drop_view_statement.cc
    statements/drop_type_statement.cc
    statements/drop_function_statement.cc
    statements/drop_aggregate_statement.cc
    statements/schema_altering_statement.cc
    statements/ks_prop_defs.cc
    statements/function_statement.cc
    statements/modification_statement.cc
    statements/cas_request.cc
    statements/raw/parsed_statement.cc
    statements/property_definitions.cc
    statements/update_statement.cc
    statements/strongly_consistent_modification_statement.cc
    statements/strongly_consistent_select_statement.cc
    statements/delete_statement.cc
    statements/prune_materialized_view_statement.cc
    statements/batch_statement.cc
    statements/select_statement.cc
    statements/use_statement.cc
    statements/index_prop_defs.cc
    statements/index_target.cc
    statements/create_index_statement.cc
    statements/truncate_statement.cc
    statements/alter_table_statement.cc
    statements/alter_view_statement.cc
    statements/list_users_statement.cc
    statements/authorization_statement.cc
    statements/permission_altering_statement.cc
    statements/list_permissions_statement.cc
    statements/grant_statement.cc
    statements/revoke_statement.cc
    statements/alter_type_statement.cc
    statements/alter_keyspace_statement.cc
    statements/role-management-statements.cc
    statements/service_level_statement.cc
    statements/create_service_level_statement.cc
    statements/alter_service_level_statement.cc
    statements/sl_prop_defs.cc
    statements/drop_service_level_statement.cc
    statements/attach_service_level_statement.cc
    statements/detach_service_level_statement.cc
    statements/list_service_level_statement.cc
    statements/list_service_level_attachments_statement.cc
    statements/list_effective_service_level_statement.cc
    statements/describe_statement.cc
    update_parameters.cc
    util.cc
    ut_name.cc
    role_name.cc
    column_identifier.cc
    column_specification.cc
    constants.cc
    query_processor.cc
    query_options.cc
    user_types.cc
    untyped_result_set.cc
    selection/selectable.cc
    selection/selection.cc
    selection/selector.cc
    restrictions/statement_restrictions.cc
    result_set.cc
    prepare_context.cc
    ${cql_grammar_srcs})
target_include_directories(cql3
  PUBLIC
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_BINARY_DIR})
target_link_libraries(cql3
  PUBLIC
    idl
    wasmtime_bindings
    Seastar::seastar
    absl::headers
    xxHash::xxhash
    ANTLR3::antlr3
  PRIVATE
    lang
    transport)

check_headers(check-headers cql3
  GLOB_RECURSE ${CMAKE_CURRENT_SOURCE_DIR}/*.hh)
