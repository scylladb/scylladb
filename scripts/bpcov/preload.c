/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * bpcov-preload.so: measure which code a program runs, using one-shot
 * breakpoints handled inside the program itself. bpcov.py uses it by
 * default; the alternative, bpcov-tracer, handles them from another process
 * with ptrace.
 *
 * Usage: LD_PRELOAD=bpcov-preload.so BPCOV_PROGRAM=PATH \
 *        BPCOV_BREAKPOINTS=FILE BPCOV_HITS_DIR=DIR PROGRAM [ARGS...]
 *
 * The dynamic linker loads this library into every program that inherits
 * LD_PRELOAD, and runs its constructor before the program's own code
 * (including the program's static constructors). In the program whose
 * executable is BPCOV_PROGRAM, the constructor writes an int3 instruction
 * at each address of BPCOV_BREAKPOINTS (as written by bpcov-prepare), and
 * installs a handler for the SIGTRAP signal that an int3 causes. In any other
 * program, it does nothing.
 *
 * When a thread reaches a breakpoint, the handler records the hit, restores
 * the original instruction, and moves the thread back to it. So each
 * breakpoint costs one signal the first time it is reached, and nothing after
 * that. Unlike bpcov-tracer, this involves no other process: no context
 * switches, and fewer system calls. It also leaves ptrace free, for a
 * debugger or for LeakSanitizer.
 *
 * The hits of each process are in DIR/PID.bin, which has the same format as
 * bpcov-tracer's HITS file: one byte for each breakpoint, 1 if it was hit,
 * 2 if it could not be used (the original instruction was already an int3),
 * and 0 otherwise. The file is mapped into memory, so recording a hit is just
 * a write to memory, and the hits get to the file even if the program is
 * killed. A process that the program forks shares this mapping, so its hits
 * are recorded too (as 1, like the program's own). Messages go to
 * DIR/PID.bin.log.
 *
 * A SIGTRAP caused by an int3 can't be blocked: if the thread blocks SIGTRAP,
 * the kernel kills the process instead of running the handler. Seastar blocks
 * all signals in some of its threads, so this library replaces
 * pthread_sigmask() and sigprocmask() with versions that never block SIGTRAP.
 * It also replaces sigaction() and signal(), so that the program can't
 * replace the handler; a SIGTRAP that is not one of our breakpoints is passed
 * on to the handler the program asked for.
 */

#define _GNU_SOURCE
#include <dlfcn.h>
#include <elf.h>
#include <fcntl.h>
#include <link.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <ucontext.h>
#include <unistd.h>

enum { NOT_HIT = 0, HIT = 1, UNUSABLE = 2 };

static const uint64_t* bp_addr;  // the BREAKPOINTS file, mapped into memory
static uint8_t* bp_orig;
static uint8_t* bp_hit;  // the hits file, mapped into memory
static size_t n_bp;
static uint64_t bias;  // where the program was loaded, relative to its link-time addresses
static int active;

// The original instructions are restored by writing to /proc/self/mem, which
// can write to the read-only code. The file refers to the process which
// opened it, so a forked process must open its own.
static int mem_fd = -1;
static pid_t mem_pid;

// The handler that the program installed for SIGTRAP, if any.
static struct sigaction program_action;

static int (*real_sigaction)(int, const struct sigaction*, struct sigaction*);
static int (*real_pthread_sigmask)(int, const sigset_t*, sigset_t*);
static int (*real_sigprocmask)(int, const sigset_t*, sigset_t*);

static void die(const char* msg) {
    fprintf(stderr, "bpcov-preload: %s\n", msg);
    _exit(127);
}

// The index of the breakpoint at addr (a link-time address), or -1.
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
    return lo < n_bp && bp_addr[lo] == addr ? (ssize_t)lo : -1;
}

static void on_sigtrap(int sig, siginfo_t* info, void* context) {
    ucontext_t* uc = context;
    // The int3 instruction is one byte, and the trap leaves rip after it.
    uint64_t addr = uc->uc_mcontext.gregs[REG_RIP] - 1;
    // An int3 causes a SIGTRAP with si_code SI_KERNEL; any other SIGTRAP (e.g.,
    // one sent by kill()) is not ours.
    ssize_t i = active && info->si_code == SI_KERNEL ? find_breakpoint(addr - bias) : -1;
    if (i < 0 || bp_hit[i] == UNUSABLE || *(volatile uint8_t*)addr != 0xcc) {
        // Not one of our breakpoints - unless another thread has just
        // restored it, in which case the thread should run it again.
        if (i >= 0 && bp_hit[i] != UNUSABLE) {
            uc->uc_mcontext.gregs[REG_RIP] = addr;
            return;
        }
        if (program_action.sa_flags & SA_SIGINFO) {
            program_action.sa_sigaction(sig, info, context);
        } else if (program_action.sa_handler != SIG_DFL && program_action.sa_handler != SIG_IGN) {
            program_action.sa_handler(sig);
        } else {
            // What the default action would do: kill the process (with a
            // core dump, like an unhandled SIGTRAP).
            struct sigaction dfl = {.sa_handler = SIG_DFL};
            real_sigaction(SIGTRAP, &dfl, NULL);
            raise(SIGTRAP);
        }
        return;
    }
    pid_t pid = getpid();
    if (pid != mem_pid) {
        close(mem_fd);
        mem_fd = open("/proc/self/mem", O_RDWR | O_CLOEXEC);
        mem_pid = pid;
    }
    // Another thread may be restoring the same breakpoint at the same time;
    // writing the same byte twice is harmless.
    if (pwrite(mem_fd, &bp_orig[i], 1, addr) != 1) {
        die("restoring an instruction");
    }
    bp_hit[i] = HIT;
    uc->uc_mcontext.gregs[REG_RIP] = addr;
}

// Find where the program was loaded: the first object dl_iterate_phdr()
// reports is the program itself.
static int find_bias(struct dl_phdr_info* info, size_t size, void* data) {
    (void)size;
    (void)data;
    bias = info->dlpi_addr;
    return 1;
}

static void* map_file(const char* path, size_t* size, int writable, size_t new_size) {
    int fd = open(path, writable ? O_RDWR | O_CREAT | O_CLOEXEC : O_RDONLY | O_CLOEXEC, 0644);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) < 0) {
        return NULL;
    }
    if (writable && ftruncate(fd, new_size) < 0) {
        close(fd);
        return NULL;
    }
    *size = writable ? new_size : (size_t)st.st_size;
    void* p = mmap(NULL, *size ? *size : 1, writable ? PROT_READ | PROT_WRITE : PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    return p == MAP_FAILED ? NULL : p;
}

static double now(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

__attribute__((constructor)) static void init(void) {
    real_sigaction = dlsym(RTLD_NEXT, "sigaction");
    real_pthread_sigmask = dlsym(RTLD_NEXT, "pthread_sigmask");
    real_sigprocmask = dlsym(RTLD_NEXT, "sigprocmask");
    const char* program = getenv("BPCOV_PROGRAM");
    const char* breakpoints = getenv("BPCOV_BREAKPOINTS");
    const char* hits_dir = getenv("BPCOV_HITS_DIR");
    if (!program || !breakpoints || !hits_dir) {
        return;
    }
    // Only measure the program we were asked to: other programs it runs
    // inherit LD_PRELOAD too.
    char self[PATH_MAX], target[PATH_MAX];
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n < 0 || !realpath(program, target)) {
        return;
    }
    self[n] = '\0';
    if (strcmp(self, target) != 0) {
        return;
    }

    double start = now();
    size_t size;
    bp_addr = map_file(breakpoints, &size, 0, 0);
    if (!bp_addr) {
        die("cannot read the breakpoints file");
    }
    n_bp = size / sizeof(uint64_t);
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%d.bin", hits_dir, (int)getpid());
    bp_hit = map_file(path, &size, 1, n_bp);
    if (!bp_hit) {
        die("cannot create the hits file");
    }
    bp_orig = malloc(n_bp ? n_bp : 1);
    if (!bp_orig) {
        die("out of memory");
    }
    dl_iterate_phdr(find_bias, NULL);
    mem_fd = open("/proc/self/mem", O_RDWR | O_CLOEXEC);
    mem_pid = getpid();
    if (mem_fd < 0) {
        die("cannot open /proc/self/mem");
    }

    // Install the handler before the first int3 is written.
    struct sigaction sa = {.sa_sigaction = on_sigtrap, .sa_flags = SA_SIGINFO | SA_RESTART | SA_NODEFER};
    sigemptyset(&sa.sa_mask);
    real_sigaction(SIGTRAP, &sa, NULL);
    sigset_t trap;
    sigemptyset(&trap);
    sigaddset(&trap, SIGTRAP);
    real_pthread_sigmask(SIG_UNBLOCK, &trap, NULL);

    // Write the int3s, a chunk of up to 1 MB of code at a time: copy the
    // chunk, replace the first byte of each breakpoint's instruction in the
    // copy, and write the copy back with one system call.
    enum { CHUNK = 1 << 20 };
    uint8_t* buf = malloc(CHUNK);
    if (!buf) {
        die("out of memory");
    }
    size_t n_skipped = 0;
    for (size_t i = 0; i < n_bp;) {
        uint64_t start_addr = bp_addr[i] + bias;
        size_t j = i;
        while (j < n_bp && bp_addr[j] + bias < start_addr + CHUNK) {
            j++;
        }
        size_t len = bp_addr[j - 1] + bias + 1 - start_addr;
        memcpy(buf, (const void*)start_addr, len);
        for (size_t k = i; k < j; k++) {
            uint8_t* p = &buf[bp_addr[k] + bias - start_addr];
            bp_orig[k] = *p;
            if (*p == 0xcc) {
                // Already an int3, we could not tell our trap from the original.
                bp_hit[k] = UNUSABLE;
                n_skipped++;
            } else {
                *p = 0xcc;
            }
        }
        if (pwrite(mem_fd, buf, len, start_addr) != (ssize_t)len) {
            die("writing program text");
        }
        i = j;
    }
    free(buf);
    active = 1;

    snprintf(path, sizeof(path), "%s/%d.bin.log", hits_dir, (int)getpid());
    FILE* log = fopen(path, "w");
    if (log) {
        fprintf(log, "inserted %zu breakpoints in %.2f seconds", n_bp - n_skipped, now() - start);
        if (n_skipped) {
            fprintf(log, " (%zu skipped: already int3)", n_skipped);
        }
        fprintf(log, "\n");
        fclose(log);
    }
}

// The replacements of the functions that could block SIGTRAP, or replace its
// handler, in the program.

static const sigset_t* without_sigtrap(int how, const sigset_t* set, sigset_t* copy) {
    if (!active || !set || how == SIG_UNBLOCK || !sigismember(set, SIGTRAP)) {
        return set;
    }
    *copy = *set;
    sigdelset(copy, SIGTRAP);
    return copy;
}

int pthread_sigmask(int how, const sigset_t* set, sigset_t* old) {
    if (!real_pthread_sigmask) {
        real_pthread_sigmask = dlsym(RTLD_NEXT, "pthread_sigmask");
    }
    sigset_t copy;
    return real_pthread_sigmask(how, without_sigtrap(how, set, &copy), old);
}

int sigprocmask(int how, const sigset_t* set, sigset_t* old) {
    if (!real_sigprocmask) {
        real_sigprocmask = dlsym(RTLD_NEXT, "sigprocmask");
    }
    sigset_t copy;
    return real_sigprocmask(how, without_sigtrap(how, set, &copy), old);
}

int sigaction(int sig, const struct sigaction* act, struct sigaction* old) {
    if (!real_sigaction) {
        real_sigaction = dlsym(RTLD_NEXT, "sigaction");
    }
    if (sig != SIGTRAP || !active) {
        return real_sigaction(sig, act, old);
    }
    if (old) {
        *old = program_action;
    }
    if (act) {
        program_action = *act;
    }
    return 0;
}

sighandler_t signal(int sig, sighandler_t handler) {
    if (sig != SIGTRAP || !active) {
        static sighandler_t (*real_signal)(int, sighandler_t);
        if (!real_signal) {
            real_signal = dlsym(RTLD_NEXT, "signal");
        }
        return real_signal(sig, handler);
    }
    sighandler_t previous = program_action.sa_handler;
    program_action.sa_handler = handler;
    program_action.sa_flags = 0;
    return previous;
}
