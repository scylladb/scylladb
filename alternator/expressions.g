/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

/*
 * The DynamoDB protocol is based on JSON, and most DynamoDB requests
 * describe the operation and its parameters via JSON objects such as maps
 * and lists. Nevertheless, in some types of requests an "expression" is
 * passed as a single string, and we need to parse this string. These
 * cases include:
 *  1. Attribute paths, such as "a[3].b.c", are used in projection
 *     expressions as well as inside other expressions described below.
 *  2. Condition expressions, such as "(NOT (a=b OR c=d)) AND e=f",
 *     used in conditional updates, filters, and other places.
 *  3. Update expressions, such as "SET #a.b = :x, c = :y DELETE d"
 *
 * All these expression syntaxes are very simple: Most of them could be
 * parsed as regular expressions, and the parenthesized condition expression
 * could be done with a simple hand-written lexical analyzer and recursive-
 * descent parser. Nevertheless, we decided to specify these parsers in the
 * ANTLR3 language already used in the Scylla project, hopefully making these
 * parsers easier to reason about, and easier to change if needed - and
 * reducing the amount of boiler-plate code.
 */

grammar expressions;

options {
    language = Cpp;
}

@parser::namespace{alternator}
@lexer::namespace{alternator}

/* TODO: explain what these traits things are. I haven't seen them explained
 * in any document... Compilation fails without these fail because a definition
 * of "expressionsLexerTraits" and "expressionParserTraits" is needed.
 */
@lexer::traits {
    class expressionsLexer;
    class expressionsParser;
    typedef antlr3::Traits<expressionsLexer, expressionsParser> expressionsLexerTraits;
}
@parser::traits {
    typedef expressionsLexerTraits expressionsParserTraits;
}

@lexer::header {
	#include "alternator/expressions.hh"
	// ANTLR generates a bunch of unused variables and functions. Yuck...
    #pragma GCC diagnostic ignored "-Wunused-variable"
    #pragma GCC diagnostic ignored "-Wunused-function"
}
@parser::header {
	#include "expressionsLexer.hpp"
}

/* By default, ANTLR3 composes elaborate syntax-error messages, saying which
 * token was unexpected, where, and so on on, but then dutifully writes these
 * error messages to the standard error, and returns from the parser as if
 * everything was fine, with a half-constructed output object! If we define
 * the "displayRecognitionError" method, it will be called upon to build this
 * error message, and we can instead throw an exception to stop the parsing
 * immediately. This is good enough for now, for our simple needs, but if
 * we ever want to show more information about the syntax error, Cql3.g
 * contains an elaborate implementation (it would be nice if we could reuse
 * it, not duplicate it).
 * Unfortunately, we have to repeat the same definition twice - once for the
 * parser, and once for the lexer.
 */
@parser::context {
    void displayRecognitionError(ANTLR_UINT8** token_names, ExceptionBaseType* ex) {
        const char* err;
        switch (ex->getType()) {
        case antlr3::ExceptionType::FAILED_PREDICATE_EXCEPTION:
            err = "expression nested too deeply";
            break;
        default:
            err = "syntax error";
            break;
        }
        // Alternator expressions are always single line so ex->get_line()
        // is always 1, no sense to print it.
        // TODO: return the position as part of the exception, so the
        // caller in expressions.cc that knows the expression string can
        // mark the error position in the final error message.
        throw expressions_syntax_error(format("{} at char {}", err,
            ex->get_charPositionInLine()));
    }
}
@lexer::context {
    void displayRecognitionError(ANTLR_UINT8** token_names, ExceptionBaseType* ex) {
        throw expressions_syntax_error("syntax error");
    }
}

/* Unfortunately, ANTLR uses recursion - not the heap - to parse recursive
 * expressions. To make things even worse, ANTLR has no way to limit the
 * depth of this recursion (unlike Yacc which has YYMAXDEPTH). So deeply-
 * nested expression like "(((((((((((((..." can easily crash Scylla on a
 * stack overflow (see issue #14477).
 *
 * We are lucky that in the grammar for DynamoDB expressions (below),
 * only a few specific rules can recurse, so it was fairly easy to add a
 * "depth" counter to a few specific rules, and then use a predicate
 * "{depth<MAX_DEPTH}?" to avoid parsing if the depth exceeds this limit,
 * and throw a FAILED_PREDICATE_EXCEPTION in that case, which we will
 * report to the user as a "expression nested too deeply" error.
 */
@parser::members {
    static constexpr int MAX_DEPTH = 400;
}

/*
 * Lexical analysis phase, i.e., splitting the input up to tokens.
 * Lexical analyzer rules have names starting in capital letters.
 * "fragment" rules do not generate tokens, and are just aliases used to
 * make other rules more readable.
 * Characters *not* listed here, e.g., '=', '(', etc., will be handled
 * as individual tokens on their own right.
 * Whitespace spans are skipped, so do not generate tokens.
 */
WHITESPACE: (' ' | '\t' | '\n' | '\r')+ { skip(); };

/* shortcuts for case-insensitive keywords */
fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');
/* These keywords must be appear before the generic NAME token below,
 * because NAME matches too, and the first to match wins.
 */
SET: S E T;
REMOVE: R E M O V E;
ADD: A D D;
DELETE: D E L E T E;

AND: A N D;
OR: O R;
NOT: N O T;
BETWEEN: B E T W E E N;
IN: I N;

fragment ALPHA: 'A'..'Z' | 'a'..'z';
fragment DIGIT: '0'..'9';
fragment ALNUM: ALPHA | DIGIT | '_';
INTEGER: DIGIT+;
NAME: ALPHA ALNUM*;
NAMEREF: '#' ALNUM+;
VALREF: ':' ALNUM+;

/*
 * Parsing phase - parsing the string of tokens generated by the lexical
 * analyzer defined above.
 */

path_component: NAME | NAMEREF;
path returns [parsed::path p]:
    root=path_component           { $p.set_root($root.text); }
    (   '.' name=path_component   { $p.add_dot($name.text); }
      | '[' INTEGER ']'           { $p.add_index(std::stoi($INTEGER.text)); }
    )*;

/* See comment above why the "depth" counter was needed here */
value[int depth] returns [parsed::value v]:
      VALREF       { $v.set_valref($VALREF.text); }
    | path         { $v.set_path($path.p); }
    | {depth<MAX_DEPTH}? NAME { $v.set_func_name($NAME.text); }
     '(' x=value[depth+1]    { $v.add_func_parameter($x.v); }
     (',' x=value[depth+1]   { $v.add_func_parameter($x.v); })*
     ')'
    ;

update_expression_set_rhs returns [parsed::set_rhs rhs]:
    v=value[0]  { $rhs.set_value(std::move($v.v)); }
    (   '+' v=value[0]  { $rhs.set_plus(std::move($v.v)); }
      | '-' v=value[0]  { $rhs.set_minus(std::move($v.v)); }
    )?
    ;

update_expression_set_action returns [parsed::update_expression::action a]:
    path '=' rhs=update_expression_set_rhs { $a.assign_set($path.p, $rhs.rhs); };

update_expression_remove_action returns [parsed::update_expression::action a]:
    path { $a.assign_remove($path.p); };

update_expression_add_action returns [parsed::update_expression::action a]:
    path VALREF { $a.assign_add($path.p, $VALREF.text); };

update_expression_delete_action returns [parsed::update_expression::action a]:
    path VALREF { $a.assign_del($path.p, $VALREF.text); };

update_expression_clause returns [parsed::update_expression e]:
      SET s=update_expression_set_action { $e.add(s); }
      (',' s=update_expression_set_action { $e.add(s); })*
    | REMOVE r=update_expression_remove_action { $e.add(r); }
      (',' r=update_expression_remove_action { $e.add(r); })*
    | ADD a=update_expression_add_action { $e.add(a); }
      (',' a=update_expression_add_action { $e.add(a); })*
    | DELETE d=update_expression_delete_action { $e.add(d); }
      (',' d=update_expression_delete_action { $e.add(d); })*
    ;

// Note the "EOF" token at the end of the update expression. We want to the
//  parser to match the entire string given to it - not just its beginning!
update_expression returns [parsed::update_expression e]:
    (update_expression_clause { e.append($update_expression_clause.e); })* EOF;

projection_expression returns [std::vector<parsed::path> v]:
    p=path      { $v.push_back(std::move($p.p)); }
    (',' p=path { $v.push_back(std::move($p.p)); } )* EOF;


primitive_condition returns [parsed::primitive_condition c]:
      v=value[0]      { $c.add_value(std::move($v.v));
                        $c.set_operator(parsed::primitive_condition::type::VALUE); }
      (  (  '='       { $c.set_operator(parsed::primitive_condition::type::EQ); }
          | '<' '>'   { $c.set_operator(parsed::primitive_condition::type::NE); }
          | '<'       { $c.set_operator(parsed::primitive_condition::type::LT); }
          | '<' '='   { $c.set_operator(parsed::primitive_condition::type::LE); }
          | '>'       { $c.set_operator(parsed::primitive_condition::type::GT); }
          | '>' '='   { $c.set_operator(parsed::primitive_condition::type::GE); }
         )
         v=value[0]   { $c.add_value(std::move($v.v)); }
       | BETWEEN      { $c.set_operator(parsed::primitive_condition::type::BETWEEN); }
         v=value[0]   { $c.add_value(std::move($v.v)); }
         AND
         v=value[0]   { $c.add_value(std::move($v.v)); }
       | IN '('       { $c.set_operator(parsed::primitive_condition::type::IN); }
         v=value[0]   { $c.add_value(std::move($v.v)); }
         (',' v=value[0] { $c.add_value(std::move($v.v)); })*
         ')'
      )?
    ;

// The following rules for parsing boolean expressions are verbose and
// somewhat strange because of Antlr 3's limitations on recursive rules,
// common rule prefixes, and (lack of) support for operator precedence.
// These rules could have been written more clearly using a more powerful
// parser generator - such as Yacc.
// See comment above why the "depth" counter was needed here.
boolean_expression[int depth] returns [parsed::condition_expression e]:
	  b=boolean_expression_1[depth]       { $e.append(std::move($b.e), '|'); }
	  (OR b=boolean_expression_1[depth]   { $e.append(std::move($b.e), '|'); } )*
	;
boolean_expression_1[int depth] returns [parsed::condition_expression e]:
	  b=boolean_expression_2[depth]       { $e.append(std::move($b.e), '&'); }
	  (AND b=boolean_expression_2[depth]  { $e.append(std::move($b.e), '&'); } )*
	;
boolean_expression_2[int depth] returns [parsed::condition_expression e]:
	  p=primitive_condition        { $e.set_primitive(std::move($p.c)); }
	| {depth<MAX_DEPTH}? NOT b=boolean_expression_2[depth+1]   { $e = std::move($b.e); $e.apply_not(); }
	| {depth<MAX_DEPTH}? '(' b=boolean_expression[depth+1] ')' { $e = std::move($b.e); }
    ;

condition_expression returns [parsed::condition_expression e]:
    boolean_expression[0] { e=std::move($boolean_expression.e); } EOF;
