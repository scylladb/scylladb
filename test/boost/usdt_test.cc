/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#define BOOST_TEST_MODULE core
#include <boost/test/unit_test.hpp>

#include "utils/usdt.hh"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

#include <elf.h>

// The probes under test. Each probe must appear in this binary's
// .note.stapsdt ELF section with provider "scylla" and its name.
static void emit_test_probes() {
    uint64_t v = 42;
    const char* s = "hello";
    SCYLLA_USDT(usdt_test_probe_0);
    SCYLLA_USDT(usdt_test_probe_1, v);
    SCYLLA_USDT(usdt_test_probe_2, v, s);
    SCYLLA_USDT(usdt_test_probe_3, v, s, v + 1);
    SCYLLA_USDT(usdt_test_probe_4, v, s, v + 1, v + 2);
    SCYLLA_USDT(usdt_test_probe_5, v, s, v + 1, v + 2, v + 3);
    SCYLLA_USDT(usdt_test_probe_6, v, s, v + 1, v + 2, v + 3, v + 4);
}

// A probe site is inert when no tracer is attached, but its argument
// expressions are still evaluated. Each argument must be evaluated
// exactly once.
BOOST_AUTO_TEST_CASE(test_arguments_evaluated_exactly_once) {
    int calls = 0;
    auto arg = [&] {
        ++calls;
        return calls;
    };
    SCYLLA_USDT(usdt_test_eval_once, arg(), arg());
#ifdef SCYLLA_USDT_ENABLED
    BOOST_REQUIRE_EQUAL(calls, 2);
#else
    BOOST_REQUIRE_EQUAL(calls, 0);
#endif
}

// Emitting probes with 0 to 6 arguments must compile and be safe to
// execute with no tracer attached.
BOOST_AUTO_TEST_CASE(test_probes_execute) {
    emit_test_probes();
}

#ifdef SCYLLA_USDT_ENABLED

// Reads the .note.stapsdt section of this test binary.
static std::vector<char> read_stapsdt_notes() {
    std::ifstream f("/proc/self/exe", std::ios::binary);
    BOOST_REQUIRE(f);
    std::vector<char> elf{std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
    BOOST_REQUIRE(elf.size() > sizeof(Elf64_Ehdr));

    Elf64_Ehdr ehdr;
    std::memcpy(&ehdr, elf.data(), sizeof(ehdr));
    BOOST_REQUIRE(std::memcmp(ehdr.e_ident, ELFMAG, SELFMAG) == 0);

    std::vector<Elf64_Shdr> shdrs(ehdr.e_shnum);
    std::memcpy(shdrs.data(), elf.data() + ehdr.e_shoff, ehdr.e_shnum * sizeof(Elf64_Shdr));
    const char* shstrtab = elf.data() + shdrs[ehdr.e_shstrndx].sh_offset;

    for (const auto& shdr : shdrs) {
        if (std::string(shstrtab + shdr.sh_name) == ".note.stapsdt") {
            return {elf.data() + shdr.sh_offset, elf.data() + shdr.sh_offset + shdr.sh_size};
        }
    }
    BOOST_FAIL("no .note.stapsdt section found in /proc/self/exe");
    return {};
}

// Each stapsdt note's descriptor holds three 64-bit addresses followed
// by NUL-terminated provider, name, and argument strings. Searching for
// the adjacent "provider\0name\0" byte sequences is sufficient to prove
// each probe was registered. Returns the argument string of the probe
// with `nargs` arguments.
static std::string find_test_probe_args(const std::vector<char>& notes, int nargs) {
    std::string needle = "scylla";
    needle.push_back('\0');
    needle += "usdt_test_probe_" + std::to_string(nargs);
    needle.push_back('\0');
    auto it = std::search(notes.begin(), notes.end(), needle.begin(), needle.end());
    BOOST_REQUIRE_MESSAGE(it != notes.end(),
            "probe scylla:usdt_test_probe_" + std::to_string(nargs) + " not found in .note.stapsdt");
    auto args_begin = it + needle.size();
    return std::string(args_begin, std::find(args_begin, notes.end(), '\0'));
}

BOOST_AUTO_TEST_CASE(test_probes_present_in_elf_notes) {
    emit_test_probes();
    auto notes = read_stapsdt_notes();
    BOOST_REQUIRE(!notes.empty());

    for (int nargs = 0; nargs <= 6; ++nargs) {
        find_test_probe_args(notes, nargs);
    }
}

// The argument recipe recorded in the note must match the probe's arity:
// an n-argument probe records n whitespace-separated argument specs.
BOOST_AUTO_TEST_CASE(test_probe_argument_counts) {
    emit_test_probes();
    auto notes = read_stapsdt_notes();

    for (int nargs = 0; nargs <= 6; ++nargs) {
        std::string arg_str = find_test_probe_args(notes, nargs);
        int found = 0;
        for (size_t pos = 0; pos < arg_str.size();) {
            pos = arg_str.find('@', pos);
            if (pos == std::string::npos) {
                break;
            }
            ++found;
            ++pos;
        }
        BOOST_REQUIRE_MESSAGE(found == nargs,
                "probe usdt_test_probe_" + std::to_string(nargs) + " records " +
                std::to_string(found) + " arguments, expected " + std::to_string(nargs));
    }
}

#endif // SCYLLA_USDT_ENABLED
