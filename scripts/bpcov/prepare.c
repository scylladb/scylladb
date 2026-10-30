/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * bpcov-prepare: choose the breakpoint addresses for bpcov.
 *
 * Usage: bpcov-prepare EXECUTABLE SOURCE_DIR OUTPUT_DIR
 *
 * For every line of Scylla's own source code that has compiled code in
 * EXECUTABLE, find the addresses where that code begins, so that the tracer
 * (tracer.c) can put a breakpoint there and learn whether the line ran. The
 * results are written to three files in OUTPUT_DIR:
 *
 *   files.txt        The Scylla source files that have code, one path (relative
 *                    to SOURCE_DIR) per line. A file's number is its position
 *                    in this list, starting at 0.
 *   rows.bin         Records of {uint64_t address; uint32_t file; uint32_t line;},
 *                    sorted by address: running the code at this address means
 *                    this line of this file ran. One address may have several
 *                    records, and one line usually has several addresses.
 *   breakpoints.bin  The distinct addresses of rows.bin, as sorted uint64_t.
 *
 * All the information comes from the DWARF debug information which the
 * compiler writes into the executable, even with -gline-tables-only. DWARF is
 * specified in "DWARF Debugging Information Format, Version 5" (dwarfstd.org,
 * 2017); the section numbers in the comments below refer to that document.
 * The executable has the information in two forms, and we need both:
 *
 * 1. The line-number table (section .debug_line, DWARF 5 section 6.2) maps
 *    every instruction to a source file and line. Its main use is debuggers
 *    showing where a program is. It is encoded as a small program for a
 *    simple state machine, whose registers (address, file, line, ...) are
 *    changed by its opcodes; whenever an opcode "emits a row", the current
 *    registers say that the code from this address on belongs to this line.
 *    The table also has, in its header, the list of the source files whose
 *    numbers the "file" register refers to.
 *    We take one address from every run of consecutive rows for the same
 *    line: the first row in the run that is marked as a good place for a
 *    breakpoint on the line ("is_stmt", which the compiler sets for the
 *    beginning of a line's code).
 *
 * 2. The line table attributes every instruction of an inlined function to the
 *    callee's lines, never to the line which called it. So a line such as
 *    "opts.enabled(false);", where enabled() is a small inline setter, has no
 *    code of its own in the line table. But the "debugging information
 *    entries" in .debug_info (DWARF 5 chapters 2 and 3) describe every inlined
 *    call with a DW_TAG_inlined_subroutine entry (section 3.3.8.2), which has
 *    the calling file and line (DW_AT_call_file, DW_AT_call_line) and the
 *    address ranges of the inlined code (section 2.17). If any of this code
 *    ran, the calling line ran, so we take the start of each of these ranges
 *    as an address of the calling line.
 *
 * An address found in either way may be at NOP instructions that the compiler
 * added as padding, to align the instruction after them (e.g., the beginning
 * of a loop). Execution often doesn't pass through this padding, but jumps
 * over it to the aligned instruction, so a breakpoint on the padding may
 * never be hit even when the line ran. So we move such an address to the
 * first instruction after the NOPs. This is safe: a NOP always continues to
 * the next instruction, so that instruction is reached whenever the NOP is.
 *
 * We leave out lines that consist only of closing braces (such as "}" or
 * "});"). The only code such a line has is calls to the destructors of the
 * scope's variables, and often only in code that runs when an exception
 * passes through the scope - which the compiler generates and attributes to
 * the end of the scope. Reporting such a line as never executed would usually
 * only say that no exception passed through that scope during the test, which
 * is true of nearly every scope and is not something to test. Leaving these
 * lines out loses nothing: if a scope never ran, its other lines show it.
 *
 * "Scylla's own source files" are files whose path (as the debug information
 * gives it, relative to the build's directory) is relative, is not in one of
 * the directories of other projects listed in excluded_prefixes below, and
 * exists in SOURCE_DIR. Scylla is compiled with -ffile-prefix-map, which makes
 * the paths of its files relative; system headers have absolute paths; and
 * some other code - such as Rust's standard library - has relative paths of
 * files that are not in our source tree.
 *
 * Limitations: only DWARF 5 units are read - this is what clang produces for
 * Scylla - while units of other versions (e.g., from prebuilt libraries) are
 * skipped. Only 64-bit little-endian executables and the 32-bit DWARF format
 * are supported. Debug sections may be compressed (-gz) with zstd or zlib.
 */

#define _GNU_SOURCE
#include <elf.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>
#include <zstd.h>

// Source files in these directories of the source tree are not Scylla's own.
static const char* excluded_prefixes[] = {"seastar/", "abseil/", "fmt/", "build/", "../", NULL};

static void fail(const char* msg) {
    fprintf(stderr, "bpcov-prepare: %s\n", msg);
    exit(1);
}

static void* xrealloc(void* p, size_t n) {
    p = realloc(p, n);
    if (!p) {
        fail("out of memory");
    }
    return p;
}

// The contents of a section of the executable.
struct section {
    const uint8_t* data;
    uint64_t size;
};

static struct section debug_line, debug_line_str, debug_str, debug_info, debug_abbrev, debug_addr, debug_rnglists;

// The range of addresses of the executable's code. Code that the linker
// discarded (e.g., a duplicate copy of an inline function) keeps its debug
// information, but with a placeholder address outside this range.
static uint64_t text_lo = UINT64_MAX, text_hi = 0;

static int in_text(uint64_t addr) {
    return addr >= text_lo && addr < text_hi;
}

// The executable's loadable segments of code (from its program headers), to
// read the instructions at an address.
struct code_segment {
    uint64_t vaddr, filesz;
    const uint8_t* data;
};
static struct code_segment code_segments[16];
static int n_code_segments;

// The bytes of code at addr, and how many of them there are (0 if addr is not
// in a code segment).
static const uint8_t* code_at(uint64_t addr, uint64_t* avail) {
    for (int i = 0; i < n_code_segments; i++) {
        const struct code_segment* c = &code_segments[i];
        if (addr >= c->vaddr && addr - c->vaddr < c->filesz) {
            *avail = c->filesz - (addr - c->vaddr);
            return c->data + (addr - c->vaddr);
        }
    }
    *avail = 0;
    return NULL;
}

// The length of the x86-64 NOP instruction at p, or 0 if it isn't one. The
// compiler pads with "nop" (0x90) and with the multi-byte "nop r/m" (0x0f 0x1f
// /0, with a ModRM byte and possibly SIB and displacement bytes), both possibly
// after any number of 0x66 (operand size) and 0x2e (cs segment) prefixes.
static uint64_t nop_length(const uint8_t* p, uint64_t avail) {
    uint64_t i = 0;
    while (i < avail && (p[i] == 0x66 || p[i] == 0x2e)) {
        i++;
    }
    if (i < avail && p[i] == 0x90) {
        return i + 1;
    }
    if (i + 2 >= avail || p[i] != 0x0f || p[i + 1] != 0x1f || ((p[i + 2] >> 3) & 7) != 0) {
        return 0;
    }
    uint8_t mod = p[i + 2] >> 6, rm = p[i + 2] & 7;
    uint64_t n = i + 3;
    if (mod != 3 && rm == 4) {
        n++;  // SIB byte
    }
    if (mod == 1) {
        n += 1;  // 8-bit displacement
    } else if (mod == 2 || (mod == 0 && rm == 5)) {
        n += 4;  // 32-bit displacement
    }
    return n <= avail ? n : 0;
}

// The address of the first instruction at or after addr that is not a NOP.
static uint64_t skip_nops(uint64_t addr) {
    for (;;) {
        uint64_t avail;
        const uint8_t* p = code_at(addr, &avail);
        uint64_t n = p ? nop_length(p, avail) : 0;
        if (n == 0) {
            return addr;
        }
        addr += n;
    }
}

// ---------------------------------------------------------------------------
// Reading DWARF's basic encodings (DWARF 5 section 7).

// Unsigned and signed LEB128, a variable-length encoding of integers in 7-bit
// groups, least significant first, with the high bit set on all but the last
// byte (section 7.6).
static uint64_t read_uleb(const uint8_t** p) {
    uint64_t result = 0;
    int shift = 0;
    uint8_t b;
    do {
        b = *(*p)++;
        if (shift < 64) {
            result |= (uint64_t)(b & 0x7f) << shift;
        }
        shift += 7;
    } while (b & 0x80);
    return result;
}

static int64_t read_sleb(const uint8_t** p) {
    int64_t result = 0;
    int shift = 0;
    uint8_t b;
    do {
        b = *(*p)++;
        if (shift < 64) {
            result |= (int64_t)(b & 0x7f) << shift;
        }
        shift += 7;
    } while (b & 0x80);
    if (shift < 64 && (b & 0x40)) {
        result |= -((int64_t)1 << shift);  // sign extension
    }
    return result;
}

// A fixed-size little-endian integer of n bytes.
static uint64_t read_n(const uint8_t** p, int n) {
    uint64_t v = 0;
    memcpy(&v, *p, n);
    *p += n;
    return v;
}

// DWARF constants, from DWARF 5 section 7. Only the ones we use.
enum {
    // Tags of debugging information entries (section 7.5.3).
    TAG_compile_unit = 0x11, TAG_inlined_subroutine = 0x1d,
    // Attributes (section 7.5.4).
    AT_stmt_list = 0x10, AT_low_pc = 0x11, AT_high_pc = 0x12, AT_ranges = 0x55,
    AT_call_file = 0x58, AT_call_line = 0x59, AT_addr_base = 0x73, AT_rnglists_base = 0x74,
    // Attribute forms: how an attribute's value is encoded (section 7.5.6).
    FORM_addr = 0x01, FORM_block2 = 0x03, FORM_block4 = 0x04, FORM_data2 = 0x05, FORM_data4 = 0x06,
    FORM_data8 = 0x07, FORM_string = 0x08, FORM_block = 0x09, FORM_block1 = 0x0a, FORM_data1 = 0x0b,
    FORM_flag = 0x0c, FORM_sdata = 0x0d, FORM_strp = 0x0e, FORM_udata = 0x0f, FORM_ref_addr = 0x10,
    FORM_ref1 = 0x11, FORM_ref2 = 0x12, FORM_ref4 = 0x13, FORM_ref8 = 0x14, FORM_ref_udata = 0x15,
    FORM_indirect = 0x16, FORM_sec_offset = 0x17, FORM_exprloc = 0x18, FORM_flag_present = 0x19,
    FORM_strx = 0x1a, FORM_addrx = 0x1b, FORM_ref_sup4 = 0x1c, FORM_strp_sup = 0x1d, FORM_data16 = 0x1e,
    FORM_line_strp = 0x1f, FORM_ref_sig8 = 0x20, FORM_implicit_const = 0x21, FORM_loclistx = 0x22,
    FORM_rnglistx = 0x23, FORM_ref_sup8 = 0x24, FORM_strx1 = 0x25, FORM_strx2 = 0x26, FORM_strx3 = 0x27,
    FORM_strx4 = 0x28, FORM_addrx1 = 0x29, FORM_addrx2 = 0x2a, FORM_addrx3 = 0x2b, FORM_addrx4 = 0x2c,
    // GNU extensions, which may appear in units built by gcc.
    FORM_GNU_addr_index = 0x1f01, FORM_GNU_str_index = 0x1f02, FORM_GNU_ref_alt = 0x1f20, FORM_GNU_strp_alt = 0x1f21,
    // Range list entries (section 7.25).
    RLE_end_of_list = 0, RLE_base_addressx = 1, RLE_startx_endx = 2, RLE_startx_length = 3, RLE_offset_pair = 4,
    RLE_base_address = 5, RLE_start_end = 6, RLE_start_length = 7,
    // Content types of the directory and file lists of a line table header (section 7.22).
    LNCT_path = 1, LNCT_directory_index = 2,
    // Line number program opcodes: standard (section 7.22, table 7.25) and extended (table 7.26).
    LNS_copy = 1, LNS_advance_pc = 2, LNS_advance_line = 3, LNS_set_file = 4, LNS_set_column = 5,
    LNS_negate_stmt = 6, LNS_set_basic_block = 7, LNS_const_add_pc = 8, LNS_fixed_advance_pc = 9,
    LNE_end_sequence = 1, LNE_set_address = 2,
};

// An attribute's value: a number, which is an index into .debug_addr rather
// than an address or a constant when is_addrx is set, or a string.
struct value {
    uint64_t v;
    int is_addrx;
    const char* str;
};

// Reads an attribute value of the given form (DWARF 5 section 7.5.5), or just
// skips it, for the many forms whose values we don't need. Assumes 64-bit
// addresses and the 32-bit DWARF format, where section offsets are 4 bytes.
static struct value read_form(const uint8_t** p, uint64_t form, int64_t implicit_const) {
    struct value r = {0, 0, NULL};
    switch (form) {
    case FORM_addr: case FORM_data8: case FORM_ref8: case FORM_ref_sig8: case FORM_ref_sup8:
        r.v = read_n(p, 8); break;
    case FORM_data1: case FORM_ref1: case FORM_flag: case FORM_strx1:
        r.v = read_n(p, 1); break;
    case FORM_data2: case FORM_ref2: case FORM_strx2:
        r.v = read_n(p, 2); break;
    case FORM_strx3:
        r.v = read_n(p, 3); break;
    case FORM_data4: case FORM_ref4: case FORM_ref_addr: case FORM_sec_offset: case FORM_ref_sup4:
    case FORM_strp_sup: case FORM_strx4: case FORM_GNU_ref_alt: case FORM_GNU_strp_alt:
        r.v = read_n(p, 4); break;
    case FORM_strp:  // an offset into .debug_str
        r.v = read_n(p, 4);
        r.str = r.v < debug_str.size ? (const char*)debug_str.data + r.v : NULL;
        break;
    case FORM_line_strp:  // an offset into .debug_line_str
        r.v = read_n(p, 4);
        r.str = r.v < debug_line_str.size ? (const char*)debug_line_str.data + r.v : NULL;
        break;
    case FORM_string:  // the string itself, null-terminated
        r.str = (const char*)*p;
        *p += strlen(r.str) + 1;
        break;
    case FORM_data16:
        *p += 16; break;
    case FORM_udata: case FORM_ref_udata: case FORM_strx: case FORM_loclistx: case FORM_rnglistx:
    case FORM_GNU_str_index:
        r.v = read_uleb(p); break;
    case FORM_sdata:
        r.v = read_sleb(p); break;
    case FORM_addrx: case FORM_GNU_addr_index:
        r.v = read_uleb(p); r.is_addrx = 1; break;
    case FORM_addrx1: r.v = read_n(p, 1); r.is_addrx = 1; break;
    case FORM_addrx2: r.v = read_n(p, 2); r.is_addrx = 1; break;
    case FORM_addrx3: r.v = read_n(p, 3); r.is_addrx = 1; break;
    case FORM_addrx4: r.v = read_n(p, 4); r.is_addrx = 1; break;
    case FORM_block1: { uint64_t n = read_n(p, 1); *p += n; break; }
    case FORM_block2: { uint64_t n = read_n(p, 2); *p += n; break; }
    case FORM_block4: { uint64_t n = read_n(p, 4); *p += n; break; }
    case FORM_block: case FORM_exprloc: { uint64_t n = read_uleb(p); *p += n; break; }
    case FORM_flag_present:
        r.v = 1; break;
    case FORM_implicit_const:  // the value is in the abbreviation, not here
        r.v = implicit_const; break;
    case FORM_indirect:  // the form is given here, before the value
        return read_form(p, read_uleb(p), implicit_const);
    default: {
        char msg[64];
        snprintf(msg, sizeof(msg), "unknown DWARF form 0x%lx", (unsigned long)form);
        fail(msg);
    }
    }
    return r;
}

// ---------------------------------------------------------------------------
// Source files.
//
// Each line table has its own list of files, so the same file appears in the
// line tables of many compilation units. We give each distinct Scylla source
// file one number, keeping the paths in a hash table.

static char** scylla_files;
static size_t n_scylla_files;

struct path_entry {
    char* path;
    int id;  // number of a Scylla source file, or -1 if not Scylla's
};
static struct path_entry* path_table;
static size_t path_table_size;  // a power of 2
static size_t path_table_used;
static const char* source_dir;

static uint64_t hash_string(const char* s) {
    uint64_t h = 14695981039346656037ull;  // FNV-1a
    for (; *s; s++) {
        h = (h ^ (uint8_t)*s) * 1099511628211ull;
    }
    return h;
}

static int is_scylla_source(const char* path) {
    if (path[0] == '/') {
        return 0;
    }
    for (const char** e = excluded_prefixes; *e; e++) {
        if (!strncmp(path, *e, strlen(*e))) {
            return 0;
        }
    }
    char full[4096];
    snprintf(full, sizeof(full), "%s/%s", source_dir, path);
    struct stat st;
    return stat(full, &st) == 0 && S_ISREG(st.st_mode);
}

// Normalizes a path like Python's os.path.normpath(): removes "." components,
// empty components and "x/.." pairs. Modifies the string in place.
static void normalize_path(char* path) {
    int absolute = path[0] == '/';
    char* parts[1024];
    int n = 0;
    for (char* tok = strtok(path, "/"); tok; tok = strtok(NULL, "/")) {
        if (!strcmp(tok, ".")) {
            continue;
        }
        if (!strcmp(tok, "..")) {
            if (n > 0 && strcmp(parts[n - 1], "..")) {
                n--;
                continue;
            }
            if (absolute) {
                continue;  // "/.." is "/"
            }
        }
        if (n < 1024) {
            parts[n++] = tok;
        }
    }
    // The parts point into path itself, in order, so we can copy them down.
    char result[4096];
    size_t len = 0;
    if (absolute) {
        result[len++] = '/';
    }
    for (int i = 0; i < n; i++) {
        size_t l = strlen(parts[i]);
        if (len + l + 2 > sizeof(result)) {
            break;
        }
        if (i > 0) {
            result[len++] = '/';
        }
        memcpy(result + len, parts[i], l);
        len += l;
    }
    if (len == 0) {
        result[len++] = '.';
    }
    result[len] = '\0';
    strcpy(path, result);
}

// Returns the number of the source file at this path, or -1 if it is not one
// of Scylla's source files.
static int file_id(const char* dir, const char* name) {
    char path[4096];
    if (name[0] == '/' || !dir || !dir[0]) {
        snprintf(path, sizeof(path), "%s", name);
    } else {
        snprintf(path, sizeof(path), "%s/%s", dir, name);
    }
    normalize_path(path);
    if (path_table_used * 2 >= path_table_size) {
        // Grow the hash table.
        size_t old_size = path_table_size;
        struct path_entry* old = path_table;
        path_table_size = old_size ? old_size * 2 : 4096;
        path_table = calloc(path_table_size, sizeof(struct path_entry));
        if (!path_table) {
            fail("out of memory");
        }
        for (size_t i = 0; i < old_size; i++) {
            if (old[i].path) {
                size_t j = hash_string(old[i].path) & (path_table_size - 1);
                while (path_table[j].path) {
                    j = (j + 1) & (path_table_size - 1);
                }
                path_table[j] = old[i];
            }
        }
        free(old);
    }
    size_t j = hash_string(path) & (path_table_size - 1);
    while (path_table[j].path) {
        if (!strcmp(path_table[j].path, path)) {
            return path_table[j].id;
        }
        j = (j + 1) & (path_table_size - 1);
    }
    path_table[j].path = strdup(path);
    path_table[j].id = -1;
    if (is_scylla_source(path)) {
        scylla_files = xrealloc(scylla_files, (n_scylla_files + 1) * sizeof(char*));
        scylla_files[n_scylla_files] = path_table[j].path;
        path_table[j].id = n_scylla_files++;
    }
    path_table_used++;
    return path_table[j].id;
}

// ---------------------------------------------------------------------------
// The results: (address, file, line) records.

struct row {
    uint64_t addr;
    uint32_t file;
    uint32_t line;
};
static struct row* rows;
static size_t n_rows, rows_capacity;

// For each Scylla source file, which of its lines consist only of closing
// braces (see the top of this file). Read from the source file when needed.
struct brace_lines {
    int loaded;
    uint8_t* is_brace_only;  // indexed by line number, starting at 1
    size_t n_lines;
};
static struct brace_lines* brace_lines;
static size_t n_brace_lines;

// A line consists only of closing braces, possibly with the parentheses,
// semicolons and commas that may follow them, and a // comment.
static int brace_only(const char* line, size_t len) {
    int braces = 0;
    for (size_t i = 0; i < len; i++) {
        char c = line[i];
        if (c == '/' && i + 1 < len && line[i + 1] == '/') {
            break;
        } else if (c == '}') {
            braces++;
        } else if (c != ')' && c != ';' && c != ',' && c != ' ' && c != '\t' && c != '\r' && c != '\n') {
            return 0;
        }
    }
    return braces > 0;
}

static void load_brace_lines(int file) {
    if ((size_t)file >= n_brace_lines) {
        size_t n = n_scylla_files;
        brace_lines = xrealloc(brace_lines, n * sizeof(struct brace_lines));
        memset(brace_lines + n_brace_lines, 0, (n - n_brace_lines) * sizeof(struct brace_lines));
        n_brace_lines = n;
    }
    struct brace_lines* b = &brace_lines[file];
    b->loaded = 1;
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", source_dir, scylla_files[file]);
    FILE* f = fopen(path, "r");
    if (!f) {
        return;
    }
    char* line = NULL;
    size_t cap = 0;
    ssize_t len;
    size_t capacity = 0;
    while ((len = getline(&line, &cap, f)) >= 0) {
        if (b->n_lines + 2 > capacity) {
            capacity = capacity ? capacity * 2 : 1024;
            b->is_brace_only = xrealloc(b->is_brace_only, capacity);
        }
        b->is_brace_only[++b->n_lines] = brace_only(line, len);
    }
    free(line);
    fclose(f);
}

static int is_brace_only_line(int file, uint64_t line) {
    if ((size_t)file >= n_brace_lines || !brace_lines[file].loaded) {
        load_brace_lines(file);
    }
    const struct brace_lines* b = &brace_lines[file];
    return line <= b->n_lines && b->is_brace_only[line];
}

static void add_row(uint64_t addr, int file, uint64_t line) {
    if (file < 0 || line == 0 || !in_text(addr) || is_brace_only_line(file, line)) {
        return;
    }
    addr = skip_nops(addr);
    if (n_rows == rows_capacity) {
        rows_capacity = rows_capacity ? rows_capacity * 2 : 1 << 20;
        rows = xrealloc(rows, rows_capacity * sizeof(struct row));
    }
    rows[n_rows++] = (struct row){addr, (uint32_t)file, (uint32_t)line};
}

// ---------------------------------------------------------------------------
// The line tables (.debug_line, DWARF 5 section 6.2).
//
// .debug_line is a sequence of line tables, usually one per compilation unit,
// each found by its offset in the section (the unit's DW_AT_stmt_list). We
// remember each table's offset, and the translation of its file numbers to
// ours, for interpreting DW_AT_call_file in .debug_info, which refers to the
// file list of the unit's line table.

struct line_table {
    uint64_t offset;
    int* files;  // file number in this table -> our file number, or -1
    uint64_t n_files;
};
static struct line_table* tables;
static size_t n_tables;

static const struct line_table* find_table(uint64_t offset) {
    size_t lo = 0, hi = n_tables;  // tables are sorted by offset
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (tables[mid].offset < offset) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo < n_tables && tables[lo].offset == offset ? &tables[lo] : NULL;
}

// The header lists directories and files as entries of several fields, whose
// "content type" (path, directory index, MD5, ...) and form are described
// first (section 6.2.4, items 14-20). Reads such a list, returning for each
// entry its path and directory index.
static uint64_t read_entry_list(const uint8_t** p, const char*** paths, uint64_t** dirs) {
    uint8_t n_formats = *(*p)++;
    uint64_t types[64], forms[64];
    if (n_formats > 64) {
        fail("too many fields in a line table header");
    }
    for (int i = 0; i < n_formats; i++) {
        types[i] = read_uleb(p);
        forms[i] = read_uleb(p);
    }
    uint64_t n = read_uleb(p);
    *paths = xrealloc(NULL, (n + 1) * sizeof(const char*));
    *dirs = xrealloc(NULL, (n + 1) * sizeof(uint64_t));
    for (uint64_t i = 0; i < n; i++) {
        (*paths)[i] = "";
        (*dirs)[i] = 0;
        for (int k = 0; k < n_formats; k++) {
            struct value v = read_form(p, forms[k], 0);
            if (types[k] == LNCT_path && v.str) {
                (*paths)[i] = v.str;
            } else if (types[k] == LNCT_directory_index) {
                (*dirs)[i] = v.v;
            }
        }
    }
    return n;
}

// For the rows of the current sequence: the file and line of the previous row,
// to find where a run of rows for the same line begins, and whether we already
// took an address from the current run.
static uint64_t prev_file, prev_line;
static int have_prev, run_taken;

// Called for each row the line number program emits.
static void emit_line_row(const struct line_table* t, uint64_t addr, uint64_t file, uint64_t line, int is_stmt,
                          int end_sequence) {
    if (end_sequence) {
        // The row just marks the end address of the sequence's last row.
        have_prev = 0;
        return;
    }
    if (!(have_prev && file == prev_file && line == prev_line)) {
        // The beginning of a run.
        have_prev = 1;
        prev_file = file;
        prev_line = line;
        run_taken = 0;
    }
    // Take the first row of the run that is marked is_stmt. It is usually the
    // run's first row, but not always. For example, for "x.counter++;" in a
    // debug build, the run begins with computing the counter's address and
    // AddressSanitizer's check of that address, and only the increment itself
    // is marked. A run without any is_stmt row gets no breakpoint.
    if (is_stmt && !run_taken && file < t->n_files) {
        add_row(addr, t->files[file], line);
        run_taken = 1;
    }
}

// Reads one line table at p, adds its rows, and returns the end of the table.
static const uint8_t* read_line_table(const uint8_t* p) {
    uint64_t offset = p - debug_line.data;
    // The header (section 6.2.4).
    uint64_t unit_length = read_n(&p, 4);
    if (unit_length >= 0xfffffff0) {
        fail("64-bit DWARF is not supported");
    }
    const uint8_t* end = p + unit_length;
    uint16_t version = read_n(&p, 2);
    tables = xrealloc(tables, (n_tables + 1) * sizeof(struct line_table));
    struct line_table* t = &tables[n_tables++];
    *t = (struct line_table){offset, NULL, 0};
    if (version != 5) {
        return end;  // see "Limitations" above
    }
    uint8_t address_size = read_n(&p, 1);
    read_n(&p, 1);  // segment_selector_size
    uint64_t header_length = read_n(&p, 4);
    const uint8_t* program = p + header_length;
    uint8_t min_inst_length = read_n(&p, 1);
    read_n(&p, 1);  // maximum_operations_per_instruction, only for VLIW machines
    uint8_t default_is_stmt = read_n(&p, 1);
    int8_t line_base = (int8_t)read_n(&p, 1);
    uint8_t line_range = read_n(&p, 1);
    uint8_t opcode_base = read_n(&p, 1);
    // The number of operands of each standard opcode, so that we can skip
    // opcodes we don't know.
    const uint8_t* standard_opcode_lengths = p;
    p += opcode_base - 1;
    if (address_size != 8 || line_range == 0) {
        fail("unsupported line table");
    }
    const char** dir_paths;
    uint64_t* unused;
    uint64_t n_dirs = read_entry_list(&p, &dir_paths, &unused);
    free(unused);
    const char** file_paths;
    uint64_t* file_dirs;
    t->n_files = read_entry_list(&p, &file_paths, &file_dirs);
    t->files = xrealloc(NULL, (t->n_files + 1) * sizeof(int));
    for (uint64_t i = 0; i < t->n_files; i++) {
        t->files[i] = file_id(file_dirs[i] < n_dirs ? dir_paths[file_dirs[i]] : "", file_paths[i]);
    }
    free(dir_paths);
    free(file_paths);
    free(file_dirs);

    // The line number program (section 6.2.5): run the state machine.
    p = program;
    uint64_t addr = 0, file = 1, line = 1;
    int is_stmt = default_is_stmt;
    have_prev = 0;
    while (p < end) {
        uint8_t opcode = *p++;
        if (opcode >= opcode_base) {
            // A "special opcode" (section 6.2.5.1) advances the address and the
            // line together, by amounts encoded in the opcode, and emits a row.
            uint8_t adjusted = opcode - opcode_base;
            addr += (adjusted / line_range) * min_inst_length;
            line += line_base + adjusted % line_range;
            emit_line_row(t, addr, file, line, is_stmt, 0);
        } else if (opcode == 0) {
            // An extended opcode (section 6.2.5.3): length, sub-opcode, operands.
            uint64_t len = read_uleb(&p);
            const uint8_t* next = p + len;
            uint8_t sub = *p++;
            if (sub == LNE_end_sequence) {
                emit_line_row(t, addr, file, line, is_stmt, 1);
                addr = 0;
                file = 1;
                line = 1;
                is_stmt = default_is_stmt;
            } else if (sub == LNE_set_address) {
                addr = read_n(&p, 8);
            }
            p = next;
        } else {
            // A standard opcode (section 6.2.5.2).
            switch (opcode) {
            case LNS_copy:
                emit_line_row(t, addr, file, line, is_stmt, 0);
                break;
            case LNS_advance_pc:
                addr += read_uleb(&p) * min_inst_length;
                break;
            case LNS_advance_line:
                line += read_sleb(&p);
                break;
            case LNS_set_file:
                file = read_uleb(&p);
                break;
            case LNS_negate_stmt:
                is_stmt = !is_stmt;
                break;
            case LNS_const_add_pc:
                // Advance the address like special opcode 255 would, without a row.
                addr += ((255 - opcode_base) / line_range) * min_inst_length;
                break;
            case LNS_fixed_advance_pc:
                addr += read_n(&p, 2);
                break;
            default:
                // Other opcodes (set_column, set_basic_block, set_prologue_end,
                // ...) don't matter to us; skip their ULEB128 operands.
                for (int i = 0; i < standard_opcode_lengths[opcode - 1]; i++) {
                    read_uleb(&p);
                }
            }
        }
    }
    return end;
}

// ---------------------------------------------------------------------------
// Inlined calls in .debug_info (DWARF 5 chapters 2, 3 and 7.5).
//
// .debug_info is a sequence of units, each a tree of "debugging information
// entries" (DIEs). Each DIE starts with an abbreviation code, which refers to
// an entry in the unit's abbreviation table (in .debug_abbrev) giving the
// DIE's tag (what it describes), whether it has children, and the list of its
// attributes and their forms; the attribute values follow the code. A DIE's
// children follow it, and end with a zero code.

struct attr_spec {
    uint64_t name, form;
    int64_t implicit_const;
};
struct abbrev {
    uint64_t tag;
    int has_children;
    struct attr_spec* attrs;
    size_t n_attrs;
};

// Units usually share one abbreviation table, so keep the last one parsed.
static uint64_t abbrevs_offset = UINT64_MAX;
static struct abbrev* abbrevs;  // indexed by abbreviation code
static size_t n_abbrevs;

// Reads an abbreviation table (section 7.5.3).
static void load_abbrevs(uint64_t offset) {
    if (offset == abbrevs_offset) {
        return;
    }
    for (size_t i = 0; i < n_abbrevs; i++) {
        free(abbrevs[i].attrs);
    }
    free(abbrevs);
    abbrevs = NULL;
    n_abbrevs = 0;
    const uint8_t* p = debug_abbrev.data + offset;
    for (;;) {
        uint64_t code = read_uleb(&p);
        if (code == 0) {
            break;
        }
        if (code >= n_abbrevs) {
            size_t n = code * 2 + 16;
            abbrevs = xrealloc(abbrevs, n * sizeof(struct abbrev));
            memset(abbrevs + n_abbrevs, 0, (n - n_abbrevs) * sizeof(struct abbrev));
            n_abbrevs = n;
        }
        struct abbrev* a = &abbrevs[code];
        a->tag = read_uleb(&p);
        a->has_children = *p++;
        size_t cap = 8;
        a->attrs = xrealloc(NULL, cap * sizeof(struct attr_spec));
        a->n_attrs = 0;
        for (;;) {
            uint64_t name = read_uleb(&p);
            uint64_t form = read_uleb(&p);
            if (name == 0 && form == 0) {
                break;
            }
            int64_t implicit_const = form == FORM_implicit_const ? read_sleb(&p) : 0;
            if (a->n_attrs == cap) {
                cap *= 2;
                a->attrs = xrealloc(a->attrs, cap * sizeof(struct attr_spec));
            }
            a->attrs[a->n_attrs++] = (struct attr_spec){name, form, implicit_const};
        }
    }
    abbrevs_offset = offset;
}

// The current unit's attributes which the other DIEs' values depend on: its
// line table, where its entries in .debug_addr and .debug_rnglists start, and
// its base address.
static const struct line_table* unit_table;
static uint64_t unit_addr_base, unit_rnglists_base, unit_low_pc;

// The addrx forms refer to the unit's list of addresses in .debug_addr,
// which starts at its DW_AT_addr_base (section 7.27).
static uint64_t resolve_addr(struct value v) {
    if (!v.is_addrx) {
        return v.v;
    }
    uint64_t off = unit_addr_base + v.v * 8;
    if (off + 8 > debug_addr.size) {
        fail("address index out of range");
    }
    const uint8_t* p = debug_addr.data + off;
    return read_n(&p, 8);
}

static void add_inlined_call(uint64_t call_file, uint64_t call_line, uint64_t start) {
    if (unit_table && call_file < unit_table->n_files) {
        add_row(start, unit_table->files[call_file], call_line);
    }
}

// Adds the start of each range in a range list (section 2.17.3) - the code
// of an inlined call that was split into several pieces. A range list is a
// sequence of entries (section 7.25): some give a range, some set the base
// address that later "offset pair" ranges are relative to.
static void add_inlined_call_ranges(uint64_t call_file, uint64_t call_line, uint64_t ranges, uint64_t form) {
    uint64_t off;
    if (form == FORM_rnglistx) {
        // An index into the unit's table of range list offsets, which starts at
        // its DW_AT_rnglists_base and holds offsets relative to it (section 7.28).
        const uint8_t* q = debug_rnglists.data + unit_rnglists_base + ranges * 4;
        off = unit_rnglists_base + read_n(&q, 4);
    } else {
        off = ranges;  // an offset into .debug_rnglists
    }
    if (off >= debug_rnglists.size) {
        fail("range list offset out of range");
    }
    const uint8_t* p = debug_rnglists.data + off;
    uint64_t base = unit_low_pc;
    for (;;) {
        uint8_t kind = *p++;
        uint64_t a, b;
        switch (kind) {
        case RLE_end_of_list:
            return;
        case RLE_base_addressx:
            base = resolve_addr((struct value){read_uleb(&p), 1, NULL});
            break;
        case RLE_startx_endx:
            a = read_uleb(&p);
            read_uleb(&p);
            add_inlined_call(call_file, call_line, resolve_addr((struct value){a, 1, NULL}));
            break;
        case RLE_startx_length:
            a = read_uleb(&p);
            read_uleb(&p);
            add_inlined_call(call_file, call_line, resolve_addr((struct value){a, 1, NULL}));
            break;
        case RLE_offset_pair:
            a = read_uleb(&p);
            b = read_uleb(&p);
            if (a != b) {
                add_inlined_call(call_file, call_line, base + a);
            }
            break;
        case RLE_base_address:
            base = read_n(&p, 8);
            break;
        case RLE_start_end:
            a = read_n(&p, 8);
            read_n(&p, 8);
            add_inlined_call(call_file, call_line, a);
            break;
        case RLE_start_length:
            a = read_n(&p, 8);
            read_uleb(&p);
            add_inlined_call(call_file, call_line, a);
            break;
        default:
            fail("unknown range list entry");
        }
    }
}

// Reads the DIEs of one unit, from p (just after the unit header) to end.
static void read_unit(const uint8_t* p, const uint8_t* end) {
    unit_table = NULL;
    unit_addr_base = unit_rnglists_base = unit_low_pc = 0;
    int depth = 0;  // how many DIEs with children we're inside
    while (p < end) {
        uint64_t code = read_uleb(&p);
        if (code == 0) {
            // The end of a list of children.
            if (--depth <= 0) {
                return;
            }
            continue;
        }
        if (code >= n_abbrevs || !abbrevs[code].attrs) {
            fail("bad abbreviation code");
        }
        const struct abbrev* a = &abbrevs[code];
        struct value low_pc = {0}, high_pc = {0};
        uint64_t high_pc_form = 0, ranges = 0, ranges_form = 0, call_file = 0, call_line = 0;
        int has_low_pc = 0, has_high_pc = 0, has_ranges = 0, has_call_file = 0;
        // We must read all the attributes, if only to skip them.
        for (size_t i = 0; i < a->n_attrs; i++) {
            const struct attr_spec* s = &a->attrs[i];
            struct value v = read_form(&p, s->form, s->implicit_const);
            switch (s->name) {
            case AT_low_pc: low_pc = v; has_low_pc = 1; break;
            case AT_high_pc: high_pc = v; high_pc_form = s->form; has_high_pc = 1; break;
            case AT_ranges: ranges = v.v; ranges_form = s->form; has_ranges = 1; break;
            case AT_call_file: call_file = v.v; has_call_file = 1; break;
            case AT_call_line: call_line = v.v; break;
            case AT_stmt_list: unit_table = find_table(v.v); break;
            case AT_addr_base: unit_addr_base = v.v; break;
            case AT_rnglists_base: unit_rnglists_base = v.v; break;
            }
        }
        if (a->tag == TAG_compile_unit) {
            // Only now, as DW_AT_addr_base may come after DW_AT_low_pc.
            unit_low_pc = has_low_pc ? resolve_addr(low_pc) : 0;
        } else if (a->tag == TAG_inlined_subroutine && has_call_file) {
            // The inlined code is either one range [low_pc, high_pc), where
            // high_pc is an address or, as usual, the length (section 2.17.2),
            // or a list of ranges.
            if (has_low_pc && has_high_pc) {
                uint64_t start = resolve_addr(low_pc);
                uint64_t end_addr = (high_pc_form == FORM_addr || high_pc.is_addrx) ? resolve_addr(high_pc) : start + high_pc.v;
                if (end_addr > start) {
                    add_inlined_call(call_file, call_line, start);
                }
            } else if (has_ranges) {
                add_inlined_call_ranges(call_file, call_line, ranges, ranges_form);
            }
        }
        if (a->has_children) {
            depth++;
        } else if (depth == 0) {
            return;  // a unit DIE without children
        }
    }
}

static void read_debug_info(void) {
    const uint8_t* p = debug_info.data;
    const uint8_t* end = debug_info.data + debug_info.size;
    while (p + 11 < end) {
        // The unit header (section 7.5.1).
        uint64_t length = read_n(&p, 4);
        if (length >= 0xfffffff0) {
            fail("64-bit DWARF is not supported");
        }
        const uint8_t* next = p + length;
        uint16_t version = read_n(&p, 2);
        uint8_t unit_type = version >= 5 ? read_n(&p, 1) : 0;
        if (version != 5 || (unit_type != 1 /* DW_UT_compile */ && unit_type != 3 /* DW_UT_partial */)) {
            p = next;  // see "Limitations" above
            continue;
        }
        uint8_t addr_size = read_n(&p, 1);
        uint64_t abbrev_offset = read_n(&p, 4);
        if (addr_size != 8) {
            fail("only 64-bit addresses are supported");
        }
        load_abbrevs(abbrev_offset);
        read_unit(p, next);
        p = next;
    }
}

// ---------------------------------------------------------------------------
// Compressed debug sections (-gz).
//
// A compressed section (flag SHF_COMPRESSED) starts with a header saying how it
// is compressed and its uncompressed size (Elf64_Chdr, see the "Section
// Compression" part of the ELF specification), followed by the compressed data.

static void decompress_section(struct section* s) {
    if (s->size < sizeof(Elf64_Chdr)) {
        fail("bad compressed section");
    }
    Elf64_Chdr ch;
    memcpy(&ch, s->data, sizeof(ch));
    const uint8_t* in = s->data + sizeof(ch);
    size_t in_size = s->size - sizeof(ch);
    uint8_t* out = xrealloc(NULL, ch.ch_size ? ch.ch_size : 1);
    if (ch.ch_type == ELFCOMPRESS_ZSTD) {
        size_t n = ZSTD_decompress(out, ch.ch_size, in, in_size);
        if (ZSTD_isError(n) || n != ch.ch_size) {
            fail("failed to decompress a zstd-compressed section");
        }
    } else if (ch.ch_type == ELFCOMPRESS_ZLIB) {
        uLongf n = ch.ch_size;
        if (uncompress(out, &n, in, in_size) != Z_OK || n != ch.ch_size) {
            fail("failed to decompress a zlib-compressed section");
        }
    } else {
        fail("unknown section compression type");
    }
    s->data = out;
    s->size = ch.ch_size;
}

// ---------------------------------------------------------------------------

static int compare_rows(const void* a, const void* b) {
    const struct row* x = a;
    const struct row* y = b;
    if (x->addr != y->addr) {
        return x->addr < y->addr ? -1 : 1;
    }
    if (x->file != y->file) {
        return x->file < y->file ? -1 : 1;
    }
    return x->line < y->line ? -1 : x->line > y->line;
}

static FILE* open_output(const char* dir, const char* name) {
    char path[4096];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "w");
    if (!f) {
        fail("cannot create output file");
    }
    return f;
}

static void close_output(FILE* f) {
    if (fclose(f) != 0) {
        fail("cannot write output file");
    }
}

int main(int argc, char** argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s EXECUTABLE SOURCE_DIR OUTPUT_DIR\n", argv[0]);
        return 1;
    }
    source_dir = argv[2];
    int fd = open(argv[1], O_RDONLY);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) < 0) {
        fail("cannot open executable");
    }
    const uint8_t* file = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (file == MAP_FAILED) {
        fail("cannot map executable");
    }
    // Find the code's addresses and the debug sections, from the ELF headers.
    const Elf64_Ehdr* eh = (const Elf64_Ehdr*)file;
    if (memcmp(eh->e_ident, ELFMAG, SELFMAG) != 0 || eh->e_ident[EI_CLASS] != ELFCLASS64 ||
        eh->e_ident[EI_DATA] != ELFDATA2LSB) {
        fail("not a 64-bit little-endian ELF file");
    }
    const Elf64_Phdr* ph = (const Elf64_Phdr*)(file + eh->e_phoff);
    for (int i = 0; i < eh->e_phnum; i++) {
        if (ph[i].p_type == PT_LOAD && (ph[i].p_flags & PF_X)) {
            text_lo = ph[i].p_vaddr < text_lo ? ph[i].p_vaddr : text_lo;
            text_hi = ph[i].p_vaddr + ph[i].p_memsz > text_hi ? ph[i].p_vaddr + ph[i].p_memsz : text_hi;
            if (n_code_segments == sizeof(code_segments) / sizeof(code_segments[0])) {
                fail("too many code segments");
            }
            code_segments[n_code_segments++] =
                (struct code_segment){ph[i].p_vaddr, ph[i].p_filesz, file + ph[i].p_offset};
        }
    }
    const Elf64_Shdr* sh = (const Elf64_Shdr*)(file + eh->e_shoff);
    const char* names = (const char*)file + sh[eh->e_shstrndx].sh_offset;
    for (int i = 0; i < eh->e_shnum; i++) {
        const char* name = names + sh[i].sh_name;
        struct section* s = !strcmp(name, ".debug_line") ? &debug_line
                          : !strcmp(name, ".debug_line_str") ? &debug_line_str
                          : !strcmp(name, ".debug_str") ? &debug_str
                          : !strcmp(name, ".debug_info") ? &debug_info
                          : !strcmp(name, ".debug_abbrev") ? &debug_abbrev
                          : !strcmp(name, ".debug_addr") ? &debug_addr
                          : !strcmp(name, ".debug_rnglists") ? &debug_rnglists
                          : NULL;
        if (s) {
            s->data = file + sh[i].sh_offset;
            s->size = sh[i].sh_size;
            if (sh[i].sh_flags & SHF_COMPRESSED) {
                decompress_section(s);
            }
        }
    }
    if (!debug_line.data || !debug_info.data || !debug_abbrev.data) {
        fail("the executable has no line-number information. Was it built with \"./configure.py --line-tables\"?");
    }

    // The line tables first, as the inlined calls refer to their file lists.
    const uint8_t* p = debug_line.data;
    while (p + 4 < debug_line.data + debug_line.size) {
        p = read_line_table(p);
    }
    read_debug_info();
    if (n_rows == 0) {
        fail("the executable has no line-number information for Scylla's source files");
    }

    qsort(rows, n_rows, sizeof(struct row), compare_rows);
    FILE* f = open_output(argv[3], "files.txt");
    for (size_t i = 0; i < n_scylla_files; i++) {
        fprintf(f, "%s\n", scylla_files[i]);
    }
    close_output(f);
    f = open_output(argv[3], "rows.bin");
    for (size_t i = 0; i < n_rows; i++) {
        // The same row may have been found twice, e.g., in the line table and
        // as an inlined call.
        if (i == 0 || compare_rows(&rows[i], &rows[i - 1]) != 0) {
            fwrite(&rows[i], sizeof(struct row), 1, f);
        }
    }
    close_output(f);
    f = open_output(argv[3], "breakpoints.bin");
    for (size_t i = 0; i < n_rows; i++) {
        if (i == 0 || rows[i].addr != rows[i - 1].addr) {
            fwrite(&rows[i].addr, sizeof(uint64_t), 1, f);
        }
    }
    close_output(f);
    return 0;
}
