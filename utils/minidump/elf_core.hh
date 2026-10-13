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
    uint64_t fs_base = 0;
};

// A file-backed mapping, for NT_FILE.
struct file_mapping {
    uint64_t start;
    uint64_t end;
    uint64_t offset;
    std::string path;
};

// One PT_LOAD segment. file_offset is assigned by core_layout::finalize().
struct load_segment {
    uint64_t vaddr;
    uint64_t size;
    uint64_t file_offset = 0;
};

class core_layout {
    std::vector<char> _notes;
    std::vector<load_segment> _segments;
    size_t _header_size = 0;
    size_t _data_size = 0;
    bool _extended_numbering = false;
public:
    void add_note(std::string_view name, uint32_t type, std::span<const char> desc);
    void add_thread(const thread_state& t, pid_t pid);
    void add_process_info(pid_t pid, std::string_view fname, std::string_view args);
    void add_auxv(std::span<const char> auxv);
    void add_files(std::span<const file_mapping> files);

    // Segments must be added in increasing, non-overlapping address order.
    void add_segment(uint64_t vaddr, uint64_t size);

    // Assigns file offsets to segments. Returns the bytes to write at file
    // offset 0 (ELF header, program headers and notes); the segments' data
    // follows at the offsets assigned, and trailer() goes to trailer_offset().
    std::vector<char> finalize();
    std::span<const load_segment> segments() const noexcept { return _segments; }
    uint64_t trailer_offset() const noexcept { return _header_size + _data_size; }
    std::vector<char> trailer() const;
    uint64_t file_size() const noexcept;
};

}
