/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/minidump/elf_core.hh"

#include <cstring>
#include <elf.h>
#include <sys/procfs.h>
#include <sys/user.h>
#include <unistd.h>

// x86_64 only for now; the dumper is not built for other architectures.
#ifdef __x86_64__

namespace minidump::elf {

static size_t align4(size_t n) {
    return (n + 3) & ~size_t(3);
}

void core_layout::add_note(std::string_view name, uint32_t type, std::span<const char> desc) {
    Elf64_Nhdr hdr{};
    hdr.n_namesz = name.size() + 1;
    hdr.n_descsz = desc.size();
    hdr.n_type = type;
    auto pos = _notes.size();
    _notes.resize(pos + sizeof(hdr) + align4(name.size() + 1) + align4(desc.size()), 0);
    std::memcpy(_notes.data() + pos, &hdr, sizeof(hdr));
    pos += sizeof(hdr);
    std::memcpy(_notes.data() + pos, name.data(), name.size());
    pos += align4(name.size() + 1);
    std::memcpy(_notes.data() + pos, desc.data(), desc.size());
}

template <typename T>
static std::span<const char> as_bytes(const T& v) {
    return {reinterpret_cast<const char*>(&v), sizeof(v)};
}

void core_layout::add_thread(const thread_state& t, pid_t pid) {
    elf_prstatus st{};
    st.pr_pid = t.tid;
    st.pr_ppid = getppid();
    st.pr_pgrp = getpgrp();
    st.pr_sid = getsid(0);
    (void)pid;
    auto& g = t.context.uc_mcontext.gregs;
    user_regs_struct r{};
    r.r15 = g[REG_R15];
    r.r14 = g[REG_R14];
    r.r13 = g[REG_R13];
    r.r12 = g[REG_R12];
    r.rbp = g[REG_RBP];
    r.rbx = g[REG_RBX];
    r.r11 = g[REG_R11];
    r.r10 = g[REG_R10];
    r.r9 = g[REG_R9];
    r.r8 = g[REG_R8];
    r.rax = g[REG_RAX];
    r.rcx = g[REG_RCX];
    r.rdx = g[REG_RDX];
    r.rsi = g[REG_RSI];
    r.rdi = g[REG_RDI];
    r.orig_rax = -1;
    r.rip = g[REG_RIP];
    r.cs = 0x33;
    r.eflags = g[REG_EFL];
    r.rsp = g[REG_RSP];
    r.ss = 0x2b;
    r.fs_base = t.fs_base;
    static_assert(sizeof(st.pr_reg) == sizeof(r));
    std::memcpy(&st.pr_reg, &r, sizeof(r));
    st.pr_fpvalid = t.context.uc_mcontext.fpregs != nullptr;
    add_note("CORE", NT_PRSTATUS, as_bytes(st));
    if (t.context.uc_mcontext.fpregs) {
        // getcontext() saves the FPU state into __fpregs_mem, but the context
        // was copied since, so don't follow the pointer.
        add_note("CORE", NT_FPREGSET, as_bytes(t.context.__fpregs_mem));
    }
}

void core_layout::add_process_info(pid_t pid, std::string_view fname, std::string_view args) {
    elf_prpsinfo ps{};
    ps.pr_state = 0;
    ps.pr_sname = 'R';
    ps.pr_pid = pid;
    ps.pr_ppid = getppid();
    ps.pr_pgrp = getpgrp();
    ps.pr_sid = getsid(0);
    ps.pr_uid = getuid();
    ps.pr_gid = getgid();
    std::memcpy(ps.pr_fname, fname.data(), std::min(fname.size(), sizeof(ps.pr_fname) - 1));
    std::memcpy(ps.pr_psargs, args.data(), std::min(args.size(), sizeof(ps.pr_psargs) - 1));
    add_note("CORE", NT_PRPSINFO, as_bytes(ps));
}

void core_layout::add_auxv(std::span<const char> auxv) {
    add_note("CORE", NT_AUXV, auxv);
}

void core_layout::add_files(std::span<const file_mapping> files) {
    std::vector<char> desc;
    auto put = [&] (uint64_t v) {
        desc.insert(desc.end(), reinterpret_cast<const char*>(&v), reinterpret_cast<const char*>(&v) + sizeof(v));
    };
    const uint64_t page_size = ::getpagesize();
    put(files.size());
    put(page_size);
    for (auto& f : files) {
        put(f.start);
        put(f.end);
        put(f.offset / page_size);
    }
    for (auto& f : files) {
        desc.insert(desc.end(), f.path.begin(), f.path.end());
        desc.push_back(0);
    }
    add_note("CORE", NT_FILE, desc);
}

void core_layout::add_segment(uint64_t vaddr, uint64_t size) {
    _segments.push_back(load_segment{vaddr, size});
}

std::vector<char> core_layout::finalize() {
    const size_t phnum = _segments.size() + 1;
    _extended_numbering = phnum >= PN_XNUM;
    const size_t notes_offset = sizeof(Elf64_Ehdr) + phnum * sizeof(Elf64_Phdr);
    _header_size = (notes_offset + _notes.size() + 4095) & ~size_t(4095);
    uint64_t off = _header_size;
    for (auto& s : _segments) {
        s.file_offset = off;
        off += s.size;
    }
    _data_size = off - _header_size;

    std::vector<char> out(_header_size, 0);
    Elf64_Ehdr eh{};
    std::memcpy(eh.e_ident, ELFMAG, SELFMAG);
    eh.e_ident[EI_CLASS] = ELFCLASS64;
    eh.e_ident[EI_DATA] = ELFDATA2LSB;
    eh.e_ident[EI_VERSION] = EV_CURRENT;
    eh.e_ident[EI_OSABI] = ELFOSABI_NONE;
    eh.e_type = ET_CORE;
    eh.e_machine = EM_X86_64;
    eh.e_version = EV_CURRENT;
    eh.e_phoff = sizeof(Elf64_Ehdr);
    eh.e_ehsize = sizeof(Elf64_Ehdr);
    eh.e_phentsize = sizeof(Elf64_Phdr);
    eh.e_phnum = _extended_numbering ? PN_XNUM : phnum;
    if (_extended_numbering) {
        // The real count goes to sh_info of section header 0, like the kernel does.
        eh.e_shoff = trailer_offset();
        eh.e_shentsize = sizeof(Elf64_Shdr);
        eh.e_shnum = 1;
    }
    std::memcpy(out.data(), &eh, sizeof(eh));

    auto* ph = reinterpret_cast<Elf64_Phdr*>(out.data() + sizeof(Elf64_Ehdr));
    ph[0] = Elf64_Phdr{};
    ph[0].p_type = PT_NOTE;
    ph[0].p_offset = notes_offset;
    ph[0].p_filesz = _notes.size();
    ph[0].p_align = 4;
    for (size_t i = 0; i < _segments.size(); ++i) {
        auto& s = _segments[i];
        auto& p = ph[i + 1];
        p = Elf64_Phdr{};
        p.p_type = PT_LOAD;
        p.p_flags = PF_R | PF_W;
        p.p_offset = s.file_offset;
        p.p_vaddr = s.vaddr;
        p.p_filesz = s.size;
        p.p_memsz = s.size;
        p.p_align = 1;
    }
    std::memcpy(out.data() + notes_offset, _notes.data(), _notes.size());
    return out;
}

std::vector<char> core_layout::trailer() const {
    if (!_extended_numbering) {
        return {};
    }
    Elf64_Shdr sh{};
    sh.sh_type = SHT_NULL;
    sh.sh_size = 1;
    sh.sh_info = _segments.size() + 1;
    std::vector<char> out(sizeof(sh));
    std::memcpy(out.data(), &sh, sizeof(sh));
    return out;
}

uint64_t core_layout::file_size() const noexcept {
    return trailer_offset() + (_extended_numbering ? sizeof(Elf64_Shdr) : 0);
}

}

#endif
