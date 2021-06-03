/*
 * Copyright (C) 2019-present ScyllaDB
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

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/like_matcher.hh"

auto matcher(const char* s) { return like_matcher(bytes(s)); }

bool matches(const like_matcher& m, const char* txt) { return m(bytes(txt)); }

#if __cplusplus > 201703L

auto matcher(const char8_t* s) { return matcher(reinterpret_cast<const char*>(s)); }

bool matches(const like_matcher& m, const char8_t* txt) { return m(bytes(reinterpret_cast<const char*>(txt))); }

#endif

BOOST_AUTO_TEST_CASE(test_literal) {
    auto m = matcher(u8"abc");
    BOOST_TEST(matches(m, u8"abc"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8"b"));
    BOOST_TEST(!matches(m, u8"ab"));
    BOOST_TEST(!matches(m, u8"abcd"));
    BOOST_TEST(!matches(m, u8" abc"));
}

BOOST_AUTO_TEST_CASE(test_empty) {
    auto m = matcher(u8"");
    BOOST_TEST(matches(m, u8""));
    BOOST_TEST(!matches(m, u8" "));
    BOOST_TEST(!matches(m, u8"abcd"));
}

BOOST_AUTO_TEST_CASE(test_underscore_start) {
    auto m = matcher(u8"_a");
    BOOST_TEST(matches(m, u8"aa"));
    BOOST_TEST(matches(m, u8"Шa"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8".aa"));
}

BOOST_AUTO_TEST_CASE(test_underscore_end) {
    auto m = matcher(u8"a_");
    BOOST_TEST(matches(m, u8"aa"));
    BOOST_TEST(matches(m, u8"aШ"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8"aa."));
}

BOOST_AUTO_TEST_CASE(test_underscore_middle) {
    auto m = matcher(u8"a_c");
    BOOST_TEST(matches(m, u8"abc"));
    BOOST_TEST(matches(m, u8"aШc"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"ac"));
    BOOST_TEST(!matches(m, u8"abcd"));
    BOOST_TEST(!matches(m, u8"abb"));
}

BOOST_AUTO_TEST_CASE(test_underscore_consecutive) {
    auto m = matcher(u8"a__d");
    BOOST_TEST(matches(m, u8"abcd"));
    BOOST_TEST(matches(m, u8"a__d"));
    BOOST_TEST(matches(m, u8"aШШd"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"abcde"));
    BOOST_TEST(!matches(m, u8"a__e"));
    BOOST_TEST(!matches(m, u8"e__d"));
}

BOOST_AUTO_TEST_CASE(test_underscore_multiple) {
    auto m2 = matcher(u8"a_c_");
    BOOST_TEST(matches(m2, u8"abcd"));
    BOOST_TEST(matches(m2, u8"arc."));
    BOOST_TEST(matches(m2, u8"aШcШ"));
    BOOST_TEST(!matches(m2, u8""));
    BOOST_TEST(!matches(m2, u8"abcde"));
    BOOST_TEST(!matches(m2, u8"abdc"));
    BOOST_TEST(!matches(m2, u8"4bcd"));

    auto m3 = matcher(u8"_cyll_D_");
    BOOST_TEST(matches(m3, u8"ScyllaDB"));
    BOOST_TEST(matches(m3, u8"ШcyllaD2"));
    BOOST_TEST(!matches(m3, u8""));
    BOOST_TEST(!matches(m3, u8"ScyllaDB2"));
}

BOOST_AUTO_TEST_CASE(test_percent_start) {
    auto m = matcher(u8"%bcd");
    BOOST_TEST(matches(m, u8"bcd"));
    BOOST_TEST(matches(m, u8"abcd"));
    BOOST_TEST(matches(m, u8"ШШabcd"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"bcde"));
    BOOST_TEST(!matches(m, u8"abcde"));
    BOOST_TEST(!matches(m, u8"aaaaaaaaaaaaabce"));
}

BOOST_AUTO_TEST_CASE(test_percent_end) {
    auto m = matcher(u8"abc%");
    BOOST_TEST(matches(m, u8"abc"));
    BOOST_TEST(matches(m, u8"abcd"));
    BOOST_TEST(matches(m, u8"abccccccccccccccccccccc"));
    BOOST_TEST(matches(m, u8"abcdШШ"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8"ab"));
    BOOST_TEST(!matches(m, u8"abd"));
}

BOOST_AUTO_TEST_CASE(test_percent_middle) {
    auto m = matcher(u8"a%z");
    BOOST_TEST(matches(m, u8"az"));
    BOOST_TEST(matches(m, u8"aaz"));
    BOOST_TEST(matches(m, u8"aШШz"));
    BOOST_TEST(matches(m, u8"a...................................z"));
    BOOST_TEST(!matches(m, u8""));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8"ab"));
    BOOST_TEST(!matches(m, u8"aza"));
    BOOST_TEST(!matches(m, u8"aШШШШШШШШШШza"));

    auto und = matcher(u8"a%_");
    BOOST_TEST(matches(und, u8"a_"));
    BOOST_TEST(matches(und, u8"ab"));
    BOOST_TEST(matches(und, u8"aШШШШШШШШШШ"));
    BOOST_TEST(!matches(und, u8""));
    BOOST_TEST(!matches(und, u8"a"));
    BOOST_TEST(!matches(und, u8"b_"));
}

BOOST_AUTO_TEST_CASE(test_percent_multiple) {
    auto cons = matcher(u8"a%%z");
    BOOST_TEST(matches(cons, u8"az"));
    BOOST_TEST(matches(cons, u8"aaz"));
    BOOST_TEST(matches(cons, u8"aШШz"));
    BOOST_TEST(matches(cons, u8"a...................................z"));
    BOOST_TEST(!matches(cons, u8""));
    BOOST_TEST(!matches(cons, u8"a"));
    BOOST_TEST(!matches(cons, u8"ab"));
    BOOST_TEST(!matches(cons, u8"aza"));
    BOOST_TEST(!matches(cons, u8"aШШШШШШШШШШza"));

    auto spread = matcher(u8"|%|%|");
    BOOST_TEST(matches(spread, u8"|||"));
    BOOST_TEST(matches(spread, u8"|a||"));
    BOOST_TEST(matches(spread, u8"||b|"));
    BOOST_TEST(matches(spread, u8"|a|b|"));
    BOOST_TEST(matches(spread, u8"|||||||"));
    BOOST_TEST(matches(spread, u8"|ШШШШШШШШШШza||"));
    BOOST_TEST(matches(spread, u8"||ШШШШШШШШШШza|"));
    BOOST_TEST(matches(spread, u8"|ШШШШШШШШШШza|....................|"));
    BOOST_TEST(!matches(spread, u8""));
    BOOST_TEST(!matches(spread, u8"|"));
    BOOST_TEST(!matches(spread, u8"|+"));
    BOOST_TEST(!matches(spread, u8"|+++++"));
    BOOST_TEST(!matches(spread, u8"||"));
    BOOST_TEST(!matches(spread, u8"|.......................|"));
    BOOST_TEST(!matches(spread, u8"|.......................|++++++++++"));

    auto bookends = matcher(u8"%ac%");
    BOOST_TEST(matches(bookends, u8"ac"));
    BOOST_TEST(matches(bookends, u8"ack"));
    BOOST_TEST(matches(bookends, u8"lac"));
    BOOST_TEST(matches(bookends, u8"sack"));
    BOOST_TEST(matches(bookends, u8"stack"));
    BOOST_TEST(matches(bookends, u8"backend"));
    BOOST_TEST(!matches(bookends, u8""));
    BOOST_TEST(!matches(bookends, u8"a"));
    BOOST_TEST(!matches(bookends, u8"c"));
    BOOST_TEST(!matches(bookends, u8"abc"));
    BOOST_TEST(!matches(bookends, u8"stuck"));
    BOOST_TEST(!matches(bookends, u8"dark"));
}

BOOST_AUTO_TEST_CASE(test_escape_underscore) {
    auto last = matcher(u8R"(a\_)");
    BOOST_TEST(matches(last, u8"a_"));
    BOOST_TEST(!matches(last, u8"ab"));

    auto mid = matcher(u8R"(a\__)");
    BOOST_TEST(matches(mid, u8"a_Ш"));
    BOOST_TEST(!matches(mid, u8"abc"));

    auto first = matcher(u8R"(\__)");
    BOOST_TEST(matches(first, u8"_Ш"));
    BOOST_TEST(!matches(first, u8"a_"));
}

BOOST_AUTO_TEST_CASE(test_escape_percent) {
    auto last = matcher(u8R"(a\%)");
    BOOST_TEST(matches(last, u8"a%"));
    BOOST_TEST(!matches(last, u8"ab"));
    BOOST_TEST(!matches(last, u8"abc"));

    auto perc2 = matcher(u8R"(a%\%)");
    BOOST_TEST(matches(perc2, u8"a%"));
    BOOST_TEST(matches(perc2, u8"ab%"));
    BOOST_TEST(matches(perc2, u8"aШШШШШШШШШШ%"));
    BOOST_TEST(!matches(perc2, u8"a"));
    BOOST_TEST(!matches(perc2, u8"abcd"));

    auto mid1 = matcher(u8R"(a\%z)");
    BOOST_TEST(matches(mid1, u8"a%z"));
    BOOST_TEST(!matches(mid1, u8"az"));
    BOOST_TEST(!matches(mid1, u8"a.z"));
    BOOST_TEST(!matches(mid1, u8"a%.z"));

    auto mid2 = matcher(u8R"(a%\%z)");
    BOOST_TEST(matches(mid2, u8"a%z"));
    BOOST_TEST(matches(mid2, u8"aa%z"));
    BOOST_TEST(matches(mid2, u8"aШШШШШШШШШШza%z"));
    BOOST_TEST(!matches(mid2, u8"az"));
    BOOST_TEST(!matches(mid2, u8"a.z"));
    BOOST_TEST(!matches(mid2, u8"a%.z"));

    auto mid3 = matcher(u8R"(%\%\%.)");
    BOOST_TEST(matches(mid3, u8"%%."));
    BOOST_TEST(matches(mid3, u8".%%."));
    BOOST_TEST(matches(mid3, u8"abcdefgh%%."));
    BOOST_TEST(!matches(mid3, u8"%%"));
    BOOST_TEST(!matches(mid3, u8"%."));
    BOOST_TEST(!matches(mid3, u8".%%.extra"));

    auto first1 = matcher(u8R"(\%%)");
    BOOST_TEST(matches(first1, u8"%"));
    BOOST_TEST(matches(first1, u8"%."));
    BOOST_TEST(matches(first1, u8"%abcdefgh"));
    BOOST_TEST(matches(first1, u8"%ШШШШШШШШШШ%"));
    BOOST_TEST(!matches(first1, u8""));
    BOOST_TEST(!matches(first1, u8"a%"));
    BOOST_TEST(!matches(first1, u8"abcde"));

    auto first2 = matcher(u8R"(\%a%z)");
    BOOST_TEST(matches(first2, u8"%az"));
    BOOST_TEST(matches(first2, u8"%azzzzzzz"));
    BOOST_TEST(matches(first2, u8"%a.z"));
    BOOST_TEST(!matches(first2, u8""));
    BOOST_TEST(!matches(first2, u8"%"));
    BOOST_TEST(!matches(first2, u8"%a"));

    auto cons = matcher(u8R"(a\%\%z)");
    BOOST_TEST(matches(cons, u8"a%%z"));
    BOOST_TEST(!matches(cons, u8"a%+%z"));
}

BOOST_AUTO_TEST_CASE(test_escape_any_char) {
    auto period = matcher(u8R"(a\.)");
    BOOST_TEST(matches(period, u8"a."));
    BOOST_TEST(!matches(period, u8"az"));

    auto b = matcher(u8R"(\bc)");
    BOOST_TEST(matches(b, u8"bc"));
    BOOST_TEST(!matches(b, u8R"(\bc)"));

    auto sh = matcher(u8R"(\Ш)");
    BOOST_TEST(matches(sh, u8"Ш"));
    BOOST_TEST(!matches(sh, u8"ШШ"));
    BOOST_TEST(!matches(sh, u8R"(\Ш)"));

    auto backslash1 = matcher(u8R"(a\\c)");
    BOOST_TEST(matches(backslash1, u8R"(a\c)"));
    BOOST_TEST(!matches(backslash1, u8R"(a\\c)"));

    auto backslash2 = matcher(u8R"(a\\)");
    BOOST_TEST(matches(backslash2, u8R"(a\)"));
    BOOST_TEST(!matches(backslash2, u8R"(a\\)"));
}

BOOST_AUTO_TEST_CASE(test_single_backslash_at_end) {
    auto m = matcher(u8R"(a%\)");
    BOOST_TEST(matches(m, u8R"(a\)"));
    BOOST_TEST(matches(m, u8R"(az\)"));
    BOOST_TEST(matches(m, u8R"(aaaaaaaaaaaaaaaaaaaaaa\)"));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8"az"));
    BOOST_TEST(!matches(m, u8R"(a\\a)"));
}

BOOST_AUTO_TEST_CASE(test_double_backslash_at_end) {
    auto m = matcher(u8R"(a%\\)");
    BOOST_TEST(matches(m, u8R"(a\)"));
    BOOST_TEST(matches(m, u8R"(az\)"));
    BOOST_TEST(matches(m, u8R"(a\\)"));
    BOOST_TEST(matches(m, u8R"(aaaaaaaaaaaaaaaaaaaaaa\)"));
    BOOST_TEST(!matches(m, u8"a"));
    BOOST_TEST(!matches(m, u8"az"));
    BOOST_TEST(!matches(m, u8R"(a\\a)"));
}

BOOST_AUTO_TEST_CASE(test_brackets) {
    auto single = matcher(u8"[ab");
    BOOST_TEST(matches(single, u8"[ab"));
    BOOST_TEST(!matches(single, u8"[ba"));

    auto matched = matcher(u8"[ab]");
    BOOST_TEST(matches(matched, u8"[ab]"));
    BOOST_TEST(!matches(matched, u8"a"));

    auto escaped = matcher(u8R"(\[ab\])");
    BOOST_TEST(matches(escaped, u8"[ab]"));
    BOOST_TEST(!matches(escaped, u8"a"));

    auto with_circumflex = matcher(u8"[^a]");
    BOOST_TEST(matches(with_circumflex, u8"[^a]"));
    BOOST_TEST(!matches(with_circumflex, u8"b"));

    BOOST_TEST(matches(matcher(u8"[[]"), u8"[[]"));
    BOOST_TEST(matches(matcher(u8"[\[]"), u8"[[]"));
}

BOOST_AUTO_TEST_CASE(test_asterisk) {
    auto alone = matcher(u8"*");
    BOOST_TEST(matches(alone, u8"*"));
    BOOST_TEST(!matches(alone, u8""));

    auto regular = matcher(u8"a*");
    BOOST_TEST(matches(regular, u8"a*"));
    BOOST_TEST(!matches(regular, u8""));
    BOOST_TEST(!matches(regular, u8"a"));
    BOOST_TEST(!matches(regular, u8"aa"));

    auto escaped = matcher(u8R"(a\*)");
    BOOST_TEST(matches(escaped, u8"a*"));
    BOOST_TEST(!matches(escaped, u8""));
    BOOST_TEST(!matches(escaped, u8"a"));
    BOOST_TEST(!matches(escaped, u8"aa"));
}

BOOST_AUTO_TEST_CASE(test_period) {
    auto alone = matcher(u8".");
    BOOST_TEST(matches(alone, u8"."));
    BOOST_TEST(!matches(alone, u8"a"));

    auto regular = matcher(u8"a.c");
    BOOST_TEST(matches(regular, u8"a.c"));
    BOOST_TEST(!matches(regular, u8"abc"));

    auto escaped = matcher(u8R"(a\.c)");
    BOOST_TEST(matches(escaped, u8"a.c"));
    BOOST_TEST(!matches(escaped, u8"abc"));
}

BOOST_AUTO_TEST_CASE(test_parentheses) {
    auto regular = matcher(u8"(ab)");
    BOOST_TEST(matches(regular, u8"(ab)"));
    BOOST_TEST(!matches(regular, u8"ab"));

    auto escaped = matcher(u8"\\(ab\\)");
    BOOST_TEST(matches(escaped, u8"(ab)"));
    BOOST_TEST(!matches(escaped, u8"ab"));
}

BOOST_AUTO_TEST_CASE(test_plus) {
    auto m = matcher(u8"a+");
    BOOST_TEST(matches(m, u8"a+"));
    BOOST_TEST(!matches(m, u8"aa"));
}

BOOST_AUTO_TEST_CASE(test_question_mark) {
    auto regular = matcher(u8"a?");
    BOOST_TEST(matches(regular, u8"a?"));
    BOOST_TEST(!matches(regular, u8""));

    auto escaped = matcher(u8"a\?");
    BOOST_TEST(matches(escaped, u8"a?"));
    BOOST_TEST(!matches(escaped, u8""));
}

BOOST_AUTO_TEST_CASE(test_escaped_digit) {
    auto m = matcher(u8R"(\3)");
    BOOST_TEST(matches(m, u8"3"));
    BOOST_TEST(!matches(m, u8R"(\3)"));
}

BOOST_AUTO_TEST_CASE(test_escaped_braces) {
    auto m = matcher(u8R"(a\{3\})");
    BOOST_TEST(matches(m, u8"a{3}"));
    BOOST_TEST(!matches(m, u8"aaa"));
}

BOOST_AUTO_TEST_CASE(test_circumflex) {
    auto alone = matcher(u8"^");
    BOOST_TEST(matches(alone, u8"^"));
    BOOST_TEST(!matches(alone, u8""));

    auto start = matcher(u8"^abc");
    BOOST_TEST(matches(start, u8"^abc"));
    BOOST_TEST(!matches(start, u8"abc"));

    auto escaped = matcher(u8R"(\^abc)");
    BOOST_TEST(matches(escaped, u8"^abc"));
    BOOST_TEST(!matches(escaped, u8"abc"));

    BOOST_TEST(matches(matcher(u8"abc^"), u8"abc^"));
    BOOST_TEST(matches(matcher(u8R"(abc\^)"), u8"abc^"));
    BOOST_TEST(matches(matcher(u8"a^bc"), u8"a^bc"));
    BOOST_TEST(matches(matcher(u8R"(a\^bc)"), u8"a^bc"));
}

BOOST_AUTO_TEST_CASE(test_dollar) {
    auto alone = matcher(u8"$");
    BOOST_TEST(matches(alone, u8"$"));
    BOOST_TEST(!matches(alone, u8""));

    auto end = matcher(u8"abc$");
    BOOST_TEST(matches(end, u8"abc$"));
    BOOST_TEST(!matches(end, u8"abc"));

    auto escaped = matcher(u8R"(abc\$)");
    BOOST_TEST(matches(escaped, u8"abc$"));
    BOOST_TEST(!matches(escaped, u8"abc"));

    BOOST_TEST(matches(matcher(u8"$abc"), u8"$abc"));
    BOOST_TEST(matches(matcher(u8R"(\$abc)"), u8"$abc"));
    BOOST_TEST(matches(matcher(u8"a$bc"), u8"a$bc"));
    BOOST_TEST(matches(matcher(u8R"(a\$bc)"), u8"a$bc"));
}

BOOST_AUTO_TEST_CASE(test_reset) {
    auto m = matcher(u8"alpha");
    BOOST_TEST(matches(m, u8"alpha"));
    m.reset(bytes(reinterpret_cast<const char*>(u8"omega")));
    BOOST_TEST(!matches(m, u8"alpha"));
    BOOST_TEST(matches(m, u8"omega"));
    m.reset(bytes(reinterpret_cast<const char*>(u8"omega")));
    BOOST_TEST(!matches(m, u8"alpha"));
    BOOST_TEST(matches(m, u8"omega"));
    m.reset(bytes(reinterpret_cast<const char*>(u8"alpha")));
    BOOST_TEST(matches(m, u8"alpha"));
    BOOST_TEST(!matches(m, u8"omega"));
}
