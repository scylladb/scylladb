/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "utils/minidump/elf_core.hh"

#include <algorithm>
#include <cstring>
#include <elf.h>
#include <sys/procfs.h>
#include <sys/user.h>
#include <unistd.h>

// Only for the architectures the dumper is built for.
#if defined(__x86_64__) || defined(__aarch64__)

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

void core_layout::add_thread(const thread_state& t) {
    elf_prstatus st{};
    st.pr_pid = t.tid;
    st.pr_ppid = getppid();
    st.pr_pgrp = getpgrp();
    st.pr_sid = getsid(0);
#ifdef __x86_64__
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
    r.fs_base = t.thread_pointer;
    static_assert(sizeof(st.pr_reg) == sizeof(r));
    std::memcpy(&st.pr_reg, &r, sizeof(r));
    st.pr_fpvalid = t.context.uc_mcontext.fpregs != nullptr;
    add_note("CORE", NT_PRSTATUS, as_bytes(st));
    if (t.context.uc_mcontext.fpregs) {
        // getcontext() saves the FPU state into __fpregs_mem, but the context
        // was copied since, so don't follow the pointer.
        add_note("CORE", NT_FPREGSET, as_bytes(t.context.__fpregs_mem));
    }
#else
    auto& m = t.context.uc_mcontext;
    user_regs_struct r{};
    std::memcpy(r.regs, m.regs, sizeof(r.regs));
    r.sp = m.sp;
    r.pc = m.pc;
    r.pstate = m.pstate;
    static_assert(sizeof(st.pr_reg) == sizeof(r));
    std::memcpy(&st.pr_reg, &r, sizeof(r));
    // getcontext() puts the FP/SIMD registers first in __reserved, as a
    // struct fpsimd_context: a magic and size, fpsr, fpcr, then v0-v31.
    constexpr uint32_t fpsimd_magic = 0x46508001;
    uint32_t magic;
    std::memcpy(&magic, m.__reserved, sizeof(magic));
    st.pr_fpvalid = magic == fpsimd_magic;
    add_note("CORE", NT_PRSTATUS, as_bytes(st));
    if (st.pr_fpvalid) {
        user_fpsimd_struct fp{};
        std::memcpy(&fp.fpsr, m.__reserved + 8, sizeof(fp.fpsr));
        std::memcpy(&fp.fpcr, m.__reserved + 12, sizeof(fp.fpcr));
        std::memcpy(fp.vregs, m.__reserved + 16, sizeof(fp.vregs));
        add_note("CORE", NT_FPREGSET, as_bytes(fp));
    }
    // gdb finds the thread pointer, and with it TLS, in this note.
    uint64_t tpidr = t.thread_pointer;
    add_note("LINUX", NT_ARM_TLS, as_bytes(tpidr));
#endif
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

void core_layout::add_segment(uint64_t vaddr, uint64_t size, uint64_t file_offset) {
    _segments.push_back(load_segment{vaddr, size, file_offset});
}

std::vector<char> core_layout::tail(uint64_t tail_offset) {
    std::sort(_segments.begin(), _segments.end(), [] (auto& a, auto& b) { return a.vaddr < b.vaddr; });
    _tail_offset = tail_offset;
    _phnum = _segments.size() + 1;
    const uint64_t notes_offset = tail_offset + _phnum * sizeof(Elf64_Phdr);
    const uint64_t notes_end = notes_offset + _notes.size();
    const bool extended = _phnum >= PN_XNUM;
    _shoff = extended ? (notes_end + 7) & ~uint64_t(7) : 0;
    _file_size = extended ? _shoff + sizeof(Elf64_Shdr) : notes_end;

    std::vector<char> out(_file_size - tail_offset, 0);
    auto* ph = reinterpret_cast<Elf64_Phdr*>(out.data());
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
    std::memcpy(out.data() + (notes_offset - tail_offset), _notes.data(), _notes.size());
    if (extended) {
        // The real count goes to sh_info of section header 0, like the kernel does.
        Elf64_Shdr sh{};
        sh.sh_type = SHT_NULL;
        sh.sh_size = 1;
        sh.sh_info = _phnum;
        std::memcpy(out.data() + (_shoff - tail_offset), &sh, sizeof(sh));
    }
    return out;
}

std::vector<char> core_layout::header() const {
    Elf64_Ehdr eh{};
    std::memcpy(eh.e_ident, ELFMAG, SELFMAG);
    eh.e_ident[EI_CLASS] = ELFCLASS64;
    eh.e_ident[EI_DATA] = ELFDATA2LSB;
    eh.e_ident[EI_VERSION] = EV_CURRENT;
    eh.e_ident[EI_OSABI] = ELFOSABI_NONE;
    eh.e_type = ET_CORE;
#ifdef __x86_64__
    eh.e_machine = EM_X86_64;
#else
    eh.e_machine = EM_AARCH64;
#endif
    eh.e_version = EV_CURRENT;
    eh.e_phoff = _tail_offset;
    eh.e_ehsize = sizeof(Elf64_Ehdr);
    eh.e_phentsize = sizeof(Elf64_Phdr);
    eh.e_phnum = _phnum >= PN_XNUM ? PN_XNUM : _phnum;
    if (_shoff) {
        eh.e_shoff = _shoff;
        eh.e_shentsize = sizeof(Elf64_Shdr);
        eh.e_shnum = 1;
    }
    std::vector<char> out(sizeof(eh));
    std::memcpy(out.data(), &eh, sizeof(eh));
    return out;
}

}

#endif
