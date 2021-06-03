/*
 * Copyright (C) 2019-present ScyllaDB
 */

#include "build_id.hh"
#include <fmt/printf.h>
#include <link.h>
#include <seastar/core/align.hh>
#include <sstream>
#include <cassert>

using namespace seastar;

static const Elf64_Nhdr* get_nt_build_id(dl_phdr_info* info) {
    auto base = info->dlpi_addr;
    const auto* h = info->dlpi_phdr;
    auto num_headers = info->dlpi_phnum;
    for (int i = 0; i != num_headers; ++i, ++h) {
        if (h->p_type != PT_NOTE) {
            continue;
        }

        auto* p = reinterpret_cast<const char*>(base + h->p_vaddr);
        auto* e = p + h->p_memsz;
        while (p != e) {
            const auto* n = reinterpret_cast<const Elf64_Nhdr*>(p);
            if (n->n_type == NT_GNU_BUILD_ID) {
                return n;
            }

            p += sizeof(Elf64_Nhdr);

            p += n->n_namesz;
            p = align_up(p, 4);

            p += n->n_descsz;
            p = align_up(p, 4);
        }
    }

    assert(0 && "no NT_GNU_BUILD_ID note");
}

static int callback(dl_phdr_info* info, size_t size, void* data) {
    std::string& ret = *(std::string*)data;
    std::ostringstream os;

    // The first DSO is always the main program, which has an empty name.
    assert(strlen(info->dlpi_name) == 0);

    auto* n = get_nt_build_id(info);
    auto* p = reinterpret_cast<const unsigned char*>(n);

    p += sizeof(Elf64_Nhdr);

    p += n->n_namesz;
    p = align_up(p, 4);

    auto* desc = p;
    auto* desc_end = p + n->n_descsz;
    while (desc < desc_end) {
        fmt::fprintf(os, "%02x", *desc++);
    }
    ret = os.str();
    return 1;
}

std::string get_build_id() {
    std::string ret;
    int r = dl_iterate_phdr(callback, &ret);
    assert(r == 1);
    return ret;
}
