/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <sys/types.h>
#include <sys/ucontext.h>

// Building blocks of an ELF core file which gdb opens like a kernel core dump.
namespace minidump::elf {

// Registers of one thread, as captured with getcontext().
struct thread_state {
    pid_t tid = 0;
    ucontext_t context{};
    // The thread pointer, which locates the thread's TLS.
    uint64_t thread_pointer = 0;
};

// A file-backed mapping, for NT_FILE.
struct file_mapping {
    uint64_t start;
    uint64_t end;
    uint64_t offset;
    std::string path;
};

// One PT_LOAD segment and where its data is in the file.
struct load_segment {
    uint64_t vaddr;
    uint64_t size;
    uint64_t file_offset;
};

// Where segment data starts in the file: the ELF header, written last, sits
// before it. The program headers and notes go after the data, so a writer
// can stream segments before it knows how many there will be.
constexpr uint64_t data_offset = 4096;

class core_layout {
    std::vector<char> _notes;
    std::vector<load_segment> _segments;
    uint64_t _tail_offset = 0;
    uint64_t _shoff = 0;
    uint64_t _file_size = 0;
    size_t _phnum = 0;
public:
    void add_note(std::string_view name, uint32_t type, std::span<const char> desc);
    void add_thread(const thread_state& t);
    void add_process_info(pid_t pid, std::string_view fname, std::string_view args);
    void add_auxv(std::span<const char> auxv);
    void add_files(std::span<const file_mapping> files);

    // Segments must not overlap; their data is at file_offset.
    void add_segment(uint64_t vaddr, uint64_t size, uint64_t file_offset);

    // The program headers and notes (and, for very many segments, the
    // section header holding their count), to be written at tail_offset,
    // after all segment data.
    std::vector<char> tail(uint64_t tail_offset);
    // The ELF header, for file offset 0. Call after tail().
    std::vector<char> header() const;
    // The file size. Call after tail().
    uint64_t file_size() const noexcept { return _file_size; }
};

}
