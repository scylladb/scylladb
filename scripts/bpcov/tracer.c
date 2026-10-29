/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * bpcov-tracer: measure which code a program runs, using one-shot breakpoints.
 *
 * Usage: bpcov-tracer BREAKPOINTS HITS PROGRAM [ARGS...]
 *
 * BREAKPOINTS is a file of sorted, unique, little-endian uint64 addresses
 * (link-time addresses in PROGRAM), as written by bpcov-prepare. The tracer
 * runs PROGRAM under ptrace, and before it starts running, writes an int3
 * instruction at each of these addresses. When a thread hits one, the tracer
 * records the hit in the HITS file, restores the original byte, and lets the
 * thread continue from that address. So each breakpoint costs one trap the
 * first time it is reached, and nothing after that - the program then runs at
 * full speed.
 *
 * The HITS file has one byte for each breakpoint, in the order of BREAKPOINTS:
 * 0 if it was not hit, 1 if it was hit, 3 if it was hit only in a process the
 * program forked, and 2 if it could not be used (the original instruction was
 * already an int3). The tracer maps the file into its memory, so recording a
 * hit is just a write to memory. As the mapping is shared with the file, the
 * hits get to the file even if the tracer and the program are killed with
 * SIGKILL.
 *
 * Every trap is a round trip from the program to the tracer and back, so we
 * keep the work of the tracer for each trap to a minimum: a waitpid() that
 * returns the stopped thread, PTRACE_GETREGS to learn its address, a pwrite()
 * of one byte to /proc/PID/mem to restore the original instruction,
 * PTRACE_SETREGS to move the thread back to it, and PTRACE_CONT.
 *
 * The tracer's own messages go to the file HITS.log, not to its standard
 * error: the program shares that, and tests may check the program's output
 * (e.g., that a log file starts with Scylla's first log line).
 *
 * The tracer ignores SIGINT, SIGTERM, SIGHUP and SIGQUIT: these normally
 * come from a kill of the whole process group, which also reaches the
 * program. The tracer exits when the program does, with the same status.
 */

#define _GNU_SOURCE
#include <elf.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/ptrace.h>
#include <sys/stat.h>
#include <sys/user.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static uint64_t* bp_addr;
static uint8_t* bp_orig;
static uint8_t* bp_hit;  // the HITS file, mapped into memory
static size_t n_bp;
static uint64_t bias;
static int mem_fd;  // the program's /proc/PID/mem
static FILE* log_file;
static size_t n_hits, n_traps, n_skipped;

enum { NOT_HIT = 0, HIT = 1, UNUSABLE = 2, HIT_IN_FORK = 3 };

// Threads we have already seen, so we can swallow the SIGSTOP that every
// new traced thread starts with, and threads of processes which the program
// forked, which have their own copy of its memory and breakpoints. Indexed by
// tid, which is below pid_max <= 2^22.
#define MAX_TID (1 << 22)
static uint8_t seen_tid[MAX_TID / 8];
static uint8_t forked_tid[MAX_TID / 8];

static void die(const char* what) {
    // Errors also go to stderr, as the program won't run properly anyway.
    fprintf(stderr, "bpcov-tracer: %s: %s\n", what, strerror(errno));
    if (log_file) {
        fprintf(log_file, "bpcov-tracer: %s: %s\n", what, strerror(errno));
    }
    exit(127);
}

static double now() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

static void set_bit(uint8_t* bitmap, pid_t tid) {
    if (tid > 0 && tid < MAX_TID) {
        bitmap[tid / 8] |= 1 << (tid % 8);
    }
}

static int get_bit(const uint8_t* bitmap, pid_t tid) {
    return tid > 0 && tid < MAX_TID && (bitmap[tid / 8] & (1 << (tid % 8)));
}

static void read_breakpoints(const char* path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        die(path);
    }
    struct stat st;
    if (fstat(fd, &st) < 0) {
        die(path);
    }
    n_bp = st.st_size / sizeof(uint64_t);
    bp_addr = malloc(n_bp * sizeof(uint64_t));
    bp_orig = malloc(n_bp);
    if (!bp_addr || !bp_orig) {
        die("malloc");
    }
    size_t done = 0;
    while (done < n_bp * sizeof(uint64_t)) {
        ssize_t r = read(fd, (char*)bp_addr + done, n_bp * sizeof(uint64_t) - done);
        if (r <= 0) {
            die(path);
        }
        done += r;
    }
    close(fd);
}

// The difference between run-time and link-time addresses: zero for a
// non-PIE executable, the load address for a PIE one.
static uint64_t load_bias(pid_t pid) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/exe", pid);
    int fd = open(path, O_RDONLY);
    Elf64_Ehdr eh;
    if (fd < 0 || pread(fd, &eh, sizeof(eh), 0) != sizeof(eh)) {
        die(path);
    }
    close(fd);
    snprintf(path, sizeof(path), "/proc/%d/auxv", pid);
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        die(path);
    }
    Elf64_auxv_t aux;
    while (read(fd, &aux, sizeof(aux)) == sizeof(aux) && aux.a_type != AT_NULL) {
        if (aux.a_type == AT_ENTRY) {
            close(fd);
            return aux.a_un.a_val - eh.e_entry;
        }
    }
    die("no AT_ENTRY in auxv");
    return 0;
}

// Write an int3 at every breakpoint address, remembering the original byte.
// Works chunk by chunk through /proc/pid/mem, which can write the program's
// read-only text because we are its tracer.
static void insert_breakpoints(pid_t pid) {
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/mem", pid);
    int fd = open(path, O_RDWR);
    if (fd < 0) {
        die(path);
    }
    enum { chunk_size = 1 << 20 };
    static uint8_t buf[chunk_size];
    size_t i = 0;
    while (i < n_bp) {
        uint64_t start = bp_addr[i] + bias;
        size_t j = i;
        while (j < n_bp && bp_addr[j] + bias < start + chunk_size) {
            j++;
        }
        size_t len = bp_addr[j - 1] + bias - start + 1;
        if (pread(fd, buf, len, start) != (ssize_t)len) {
            die("reading program text");
        }
        for (size_t k = i; k < j; k++) {
            uint8_t* p = &buf[bp_addr[k] + bias - start];
            bp_orig[k] = *p;
            if (*p == 0xcc) {
                // Already an int3, we could not tell our trap from the original.
                bp_hit[k] = UNUSABLE;
                n_skipped++;
            } else {
                *p = 0xcc;
            }
        }
        if (pwrite(fd, buf, len, start) != (ssize_t)len) {
            die("writing program text");
        }
        i = j;
    }
    // Kept open, to restore the original instructions as they are hit.
    mem_fd = fd;
}

static ssize_t find_breakpoint(uint64_t addr) {
    size_t lo = 0, hi = n_bp;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (bp_addr[mid] < addr) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return (lo < n_bp && bp_addr[lo] == addr) ? (ssize_t)lo : -1;
}

// Handle a SIGTRAP stop of thread tid. Returns 1 if it was one of our
// breakpoints (and the thread was resumed), 0 if it was some other SIGTRAP.
static int handle_trap(pid_t tid) {
    struct user_regs_struct regs;
    if (ptrace(PTRACE_GETREGS, tid, 0, &regs) < 0) {
        return 0;
    }
    // The int3 instruction is one byte, and the trap leaves rip after it.
    uint64_t addr = regs.rip - 1;
    ssize_t i = find_breakpoint(addr - bias);
    if (i < 0 || bp_hit[i] == UNUSABLE) {
        return 0;
    }
    n_traps++;
    if (get_bit(forked_tid, tid)) {
        // A process the program forked has its own copy of the program's
        // memory, including the breakpoints, and the HITS file doesn't say
        // which of them we restored there. So check whether this one still
        // is an int3 (another thread could have hit it first).
        errno = 0;
        long word = ptrace(PTRACE_PEEKTEXT, tid, addr, 0);
        if (errno) {
            die("PTRACE_PEEKTEXT");
        }
        if ((word & 0xff) == 0xcc) {
            word = (word & ~0xffL) | bp_orig[i];
            if (ptrace(PTRACE_POKETEXT, tid, addr, word) < 0) {
                die("PTRACE_POKETEXT");
            }
        }
        if (bp_hit[i] == NOT_HIT) {
            bp_hit[i] = HIT_IN_FORK;
            n_hits++;
        }
    } else if (bp_hit[i] != HIT) {
        // If this breakpoint was already hit, another thread got here first,
        // and we already restored the original instruction; then we only need
        // to move this thread back to it.
        if (pwrite(mem_fd, &bp_orig[i], 1, addr) != 1) {
            die("restoring an instruction");
        }
        if (bp_hit[i] == NOT_HIT) {
            n_hits++;
        }
        bp_hit[i] = HIT;
    }
    regs.rip = addr;
    if (ptrace(PTRACE_SETREGS, tid, 0, &regs) < 0) {
        die("PTRACE_SETREGS");
    }
    ptrace(PTRACE_CONT, tid, 0, 0);
    return 1;
}

int main(int argc, char** argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s BREAKPOINTS HITS PROGRAM [ARGS...]\n", argv[0]);
        return 127;
    }
    read_breakpoints(argv[1]);
    int hits_fd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (hits_fd < 0 || ftruncate(hits_fd, n_bp) < 0) {
        die(argv[2]);
    }
    bp_hit = mmap(NULL, n_bp, PROT_READ | PROT_WRITE, MAP_SHARED, hits_fd, 0);
    if (bp_hit == MAP_FAILED) {
        die(argv[2]);
    }
    close(hits_fd);
    char log_path[4096];
    snprintf(log_path, sizeof(log_path), "%s.log", argv[2]);
    log_file = fopen(log_path, "a");
    if (!log_file) {
        die(log_path);
    }
    setvbuf(log_file, NULL, _IOLBF, 0);

    pid_t child = fork();
    if (child < 0) {
        die("fork");
    }
    if (child == 0) {
        if (ptrace(PTRACE_TRACEME, 0, 0, 0) < 0) {
            die("PTRACE_TRACEME");
        }
        execv(argv[3], argv + 3);
        die(argv[3]);
    }
    // Only now, so the program does not inherit these dispositions.
    signal(SIGINT, SIG_IGN);
    signal(SIGTERM, SIG_IGN);
    signal(SIGHUP, SIG_IGN);
    signal(SIGQUIT, SIG_IGN);

    int status;
    // The child stops with SIGTRAP right after a successful execv().
    if (waitpid(child, &status, 0) != child || !WIFSTOPPED(status) || WSTOPSIG(status) != SIGTRAP) {
        fprintf(stderr, "bpcov-tracer: failed to start %s\n", argv[3]);
        return 127;
    }
    set_bit(seen_tid, child);
    if (ptrace(PTRACE_SETOPTIONS, child, 0, PTRACE_O_TRACECLONE | PTRACE_O_TRACEFORK | PTRACE_O_TRACEVFORK |
                                              PTRACE_O_TRACEEXEC | PTRACE_O_EXITKILL) < 0) {
        die("PTRACE_SETOPTIONS");
    }
    bias = load_bias(child);
    double t0 = now();
    insert_breakpoints(child);
    fprintf(log_file, "inserted %zu breakpoints in %.2f seconds\n", n_bp - n_skipped, now() - t0);
    ptrace(PTRACE_CONT, child, 0, 0);

    int child_status = 0;
    for (;;) {
        pid_t tid = waitpid(-1, &status, __WALL);
        if (tid < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == ECHILD) {
                break;
            }
            die("waitpid");
        }
        if (WIFEXITED(status) || WIFSIGNALED(status)) {
            if (tid == child) {
                child_status = status;
            }
            continue;
        }
        if (!WIFSTOPPED(status)) {
            continue;
        }
        int sig = WSTOPSIG(status);
        int event = (unsigned)status >> 16;
        if (event) {
            if (event == PTRACE_EVENT_EXEC) {
                // A forked process ran another program, which has no breakpoints.
                ptrace(PTRACE_DETACH, tid, 0, 0);
            } else {
                ptrace(PTRACE_CONT, tid, 0, 0);
            }
            continue;
        }
        if (sig == SIGTRAP && handle_trap(tid)) {
            continue;
        }
        if (sig == SIGSTOP && !get_bit(seen_tid, tid)) {
            // The initial stop of a new thread or process. If it is not a
            // thread of the program's process, the program forked it.
            set_bit(seen_tid, tid);
            char path[64];
            snprintf(path, sizeof(path), "/proc/%d/task/%d", child, tid);
            if (access(path, F_OK) != 0) {
                set_bit(forked_tid, tid);
            }
            ptrace(PTRACE_CONT, tid, 0, 0);
            continue;
        }
        siginfo_t si;
        if (ptrace(PTRACE_GETSIGINFO, tid, 0, &si) < 0) {
            // A group-stop rather than a signal to deliver.
            ptrace(PTRACE_CONT, tid, 0, 0);
            continue;
        }
        ptrace(PTRACE_CONT, tid, 0, sig);
    }
    fprintf(log_file, "%zu of %zu breakpoints hit, in %zu traps\n", n_hits, n_bp - n_skipped, n_traps);
    if (WIFSIGNALED(child_status)) {
        signal(WTERMSIG(child_status), SIG_DFL);
        raise(WTERMSIG(child_status));
    }
    return WEXITSTATUS(child_status);
}
