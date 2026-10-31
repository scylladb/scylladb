/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

/*
 * bpcov-preload.so: measure which code programs run, using one-shot
 * breakpoints handled inside each program itself. bpcov.py uses it by
 * default; the alternative, bpcov-tracer, handles them from another process
 * with ptrace.
 *
 * Usage: LD_PRELOAD=bpcov-preload.so BPCOV_DIR=DIR BPCOV_PYTHON=PYTHON \
 *        BPCOV_PY=PATH BPCOV_VERSION=N COMMAND [ARGS...]
 *
 * The dynamic linker loads this library into every program that inherits
 * LD_PRELOAD, and runs its constructor before the program's own code
 * (including the program's static constructors). In a program whose
 * executable is under DIR (e.g., build/dev), the constructor writes an int3
 * instruction at each address of the program's breakpoints, and installs a
 * handler for the SIGTRAP signal that an int3 causes. In any other program,
 * it does nothing. So COMMAND can be a test runner, and every executable
 * under DIR that it runs, directly or not, is measured.
 *
 * The data of the executable DIR/X is in DIR/bpcov/X/ (see README.md): it is
 * ready if its "info" file has the executable's build-id and data version N.
 * If it isn't, the constructor first runs "PYTHON PATH prepare" (bpcov.py) to
 * prepare it, and waits; bpcov.py makes sure only one process prepares, also
 * when many runs of a new executable start at once. If the executable can't
 * be measured (e.g., it has no line tables), bpcov.py says so in "info", and
 * the constructor does nothing. PYTHON is the interpreter that ran bpcov.py,
 * not whatever "python3" the program's PATH has: test.py, for example, puts a
 * venv without numpy first. The output of the preparation, and our messages
 * if it fails, go to DIR/bpcov/errors.log, never to the program's output.
 *
 * When a thread reaches a breakpoint, the handler records the hit, restores
 * the original instruction, and moves the thread back to it. So each
 * breakpoint costs one signal the first time it is reached, and nothing after
 * that. Unlike bpcov-tracer, this involves no other process: no context
 * switches, and fewer system calls. It also leaves ptrace free, for a
 * debugger or for LeakSanitizer.
 *
 * The hits are in the file "hits", in the format of bpcov-tracer's HITS
 * file: one byte for each breakpoint, 1 if it was hit, 2 if it could not be
 * used (the original instruction was already an int3), and 0 otherwise. All
 * the runs of the executable share this file, also at the same time: a run
 * inserts only the breakpoints that no run hit yet, so it traps only on code
 * that no run reached before, and runs at the same time can only change
 * bytes from 0 to the same 1 or 2. The file is mapped into memory, so
 * recording a hit is just a write to memory, and the hits get to the file
 * even if the program is killed. A process that the program forks shares
 * this mapping, so its hits are recorded too. Each run appends a line to
 * "hits.log".
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
#include <errno.h>
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
#include <sys/wait.h>
#include <time.h>
#include <ucontext.h>
#include <unistd.h>

// The values in the hits file.
enum { NOT_HIT = 0, HIT = 1, UNUSABLE = 2 };

static const uint32_t* bp_addr;  // the breakpoints file, mapped into memory
// The original byte of each breakpoint that this process inserted, with
// INSERTED added; 0 for those it didn't insert (the hits file can't tell,
// as other runs change it).
static uint16_t* bp_orig;
enum { INSERTED = 0x100 };
static uint8_t* bp_hit;  // the hits file, mapped into memory
static size_t n_bp;
static uint64_t bias;  // where the program was loaded, relative to its link-time addresses
static const ElfW(Phdr)* phdr;  // the program's program headers, in memory
static size_t phnum;
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
    if (i < 0 || !(bp_orig[i] & INSERTED) || *(volatile uint8_t*)addr != 0xcc) {
        // Not one of our breakpoints - unless another thread has just
        // restored it, in which case the thread should run it again.
        if (i >= 0 && (bp_orig[i] & INSERTED)) {
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
    uint8_t orig = bp_orig[i];
    if (pwrite(mem_fd, &orig, 1, addr) != 1) {
        die("restoring an instruction");
    }
    bp_hit[i] = HIT;
    uc->uc_mcontext.gregs[REG_RIP] = addr;
}

// Find where the program was loaded, and its program headers: the first
// object dl_iterate_phdr() reports is the program itself.
static int find_program(struct dl_phdr_info* info, size_t size, void* data) {
    (void)size;
    (void)data;
    bias = info->dlpi_addr;
    phdr = info->dlpi_phdr;
    phnum = info->dlpi_phnum;
    return 1;
}

// The program's GNU build-id, in hex, from its PT_NOTE segments in memory.
static int get_build_id(char* out, size_t size) {
    for (size_t i = 0; i < phnum; i++) {
        if (phdr[i].p_type != PT_NOTE) {
            continue;
        }
        const uint8_t* p = (const uint8_t*)(bias + phdr[i].p_vaddr);
        const uint8_t* end = p + phdr[i].p_memsz;
        while (p + sizeof(ElfW(Nhdr)) <= end) {
            const ElfW(Nhdr)* n = (const ElfW(Nhdr)*)p;
            const uint8_t* name = p + sizeof(*n);
            const uint8_t* desc = name + ((n->n_namesz + 3) & ~3u);
            if (n->n_type == NT_GNU_BUILD_ID && n->n_namesz == 4 && !memcmp(name, "GNU", 4) &&
                2 * n->n_descsz < size) {
                for (size_t k = 0; k < n->n_descsz; k++) {
                    sprintf(out + 2 * k, "%02x", desc[k]);
                }
                return 1;
            }
            p = desc + ((n->n_descsz + 3) & ~3u);
        }
    }
    return 0;
}

// Whether the data directory's "info" file says its data is of this build
// (the build-id) and of this version of bpcov: 1 if it is and the program can
// be measured, -1 if it is but it can't (an "error" line), 0 if it isn't.
static int check_info(const char* data, const char* build_id, const char* version) {
    char path[PATH_MAX + 32], line[4096];
    snprintf(path, sizeof(path), "%s/info", data);
    FILE* f = fopen(path, "re");
    if (!f) {
        return 0;
    }
    int id_ok = 0, version_ok = 0, error = 0;
    while (fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        if (!strncmp(line, "build-id ", 9)) {
            id_ok = !strcmp(line + 9, build_id);
        } else if (!strncmp(line, "version ", 8)) {
            version_ok = !strcmp(line + 8, version);
        } else if (!strncmp(line, "error ", 6)) {
            error = 1;
        }
    }
    fclose(f);
    return id_ok && version_ok ? (error ? -1 : 1) : 0;
}

// Open DIR/bpcov/errors.log for appending.
static int open_errors_log(const char* dir) {
    char path[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/bpcov", dir);
    mkdir(path, 0755);
    snprintf(path, sizeof(path), "%s/bpcov/errors.log", dir);
    return open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
}

// Run "bpcov.py prepare" for this program, and wait for it. Returns its exit
// status, as waitpid() reports it, or -1.
static int prepare(const char* python, const char* py, const char* dir, const char* self) {
    pid_t pid = fork();
    if (pid < 0) {
        return -1;
    }
    if (pid == 0) {
        int log = open_errors_log(dir);
        int null = open("/dev/null", O_RDONLY);
        if (log < 0 || null < 0 || dup2(null, 0) < 0 || dup2(log, 1) < 0 || dup2(log, 2) < 0) {
            _exit(127);
        }
        execl(python, python, py, "prepare", "--quiet", "--dir", dir, "--binary", self, (char*)NULL);
        _exit(127);
    }
    int status;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            return -1;
        }
    }
    return status;
}

static void* map_file(const char* path, size_t* size, int writable, size_t min_size) {
    int fd = open(path, writable ? O_RDWR | O_CREAT | O_CLOEXEC : O_RDONLY | O_CLOEXEC, 0644);
    struct stat st;
    if (fd < 0 || fstat(fd, &st) < 0) {
        return NULL;
    }
    // Other runs may be using the hits file, so only extend it if it is new.
    if (writable && (size_t)st.st_size < min_size && ftruncate(fd, min_size) < 0) {
        close(fd);
        return NULL;
    }
    *size = writable ? min_size : (size_t)st.st_size;
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
    const char* dir = getenv("BPCOV_DIR");
    const char* python = getenv("BPCOV_PYTHON");
    const char* py = getenv("BPCOV_PY");
    const char* version = getenv("BPCOV_VERSION");
    if (!dir || !python || !py || !version) {
        return;
    }
    // Only measure the executables under DIR: all the programs that the
    // command runs inherit LD_PRELOAD.
    char self[PATH_MAX];
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    size_t dir_len = strlen(dir);
    if (n < 0) {
        return;
    }
    self[n] = '\0';
    if (strncmp(self, dir, dir_len) != 0 || self[dir_len] != '/') {
        return;
    }
    char data[PATH_MAX];
    snprintf(data, sizeof(data), "%s/bpcov/%s", dir, self + dir_len + 1);

    double start = now();
    dl_iterate_phdr(find_program, NULL);
    char build_id[256];
    if (!get_build_id(build_id, sizeof(build_id))) {
        return;
    }
    int ready = check_info(data, build_id, version);
    if (ready == 0) {
        int status = prepare(python, py, dir, self);
        ready = check_info(data, build_id, version);
        if (ready == 0) {
            // The program will run unmeasured; say why where the report will find it.
            int log = open_errors_log(dir);
            if (log >= 0) {
                char line[PATH_MAX + 128];
                int len = snprintf(line, sizeof(line), "%d: preparing %s failed (wait status %d)\n", (int)getpid(),
                                   self, status);
                if (write(log, line, len) < 0) {
                    // Nothing to do about it.
                }
                close(log);
            }
        }
    }
    if (ready != 1) {
        return;
    }

    size_t size;
    char path[PATH_MAX + 32];
    snprintf(path, sizeof(path), "%s/breakpoints.bin", data);
    bp_addr = map_file(path, &size, 0, 0);
    if (!bp_addr) {
        die("cannot read the breakpoints file");
    }
    n_bp = size / sizeof(uint32_t);
    snprintf(path, sizeof(path), "%s/hits", data);
    bp_hit = map_file(path, &size, 1, n_bp);
    if (!bp_hit) {
        die("cannot open the hits file");
    }
    bp_orig = calloc(n_bp ? n_bp : 1, sizeof(uint16_t));
    if (!bp_orig) {
        die("out of memory");
    }
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
    // copy, and write the copy back with one system call - unless all of
    // the chunk's breakpoints were already hit.
    enum { CHUNK = 1 << 20 };
    uint8_t* buf = malloc(CHUNK);
    if (!buf) {
        die("out of memory");
    }
    size_t n_inserted = 0, n_already_hit = 0;
    for (size_t i = 0; i < n_bp;) {
        uint64_t start_addr = bp_addr[i] + bias;
        size_t j = i;
        while (j < n_bp && bp_addr[j] + bias < start_addr + CHUNK) {
            j++;
        }
        size_t not_hit = 0;
        for (size_t k = i; k < j; k++) {
            not_hit += bp_hit[k] == NOT_HIT;
            n_already_hit += bp_hit[k] == HIT;
        }
        if (!not_hit) {
            i = j;
            continue;
        }
        size_t len = bp_addr[j - 1] + bias + 1 - start_addr;
        memcpy(buf, (const void*)start_addr, len);
        size_t n = 0;
        for (size_t k = i; k < j; k++) {
            if (bp_hit[k] != NOT_HIT) {
                continue;
            }
            uint8_t* p = &buf[bp_addr[k] + bias - start_addr];
            if (*p == 0xcc) {
                // Already an int3, we could not tell our trap from the original.
                bp_hit[k] = UNUSABLE;
            } else {
                bp_orig[k] = INSERTED | *p;
                *p = 0xcc;
                n++;
            }
        }
        if (n && pwrite(mem_fd, buf, len, start_addr) != (ssize_t)len) {
            die("writing program text");
        }
        n_inserted += n;
        i = j;
    }
    free(buf);
    active = 1;

    // One line, appended with one write(), so lines of runs at the same time
    // don't mix.
    snprintf(path, sizeof(path), "%s/hits.log", data);
    int log = open(path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (log >= 0) {
        char line[256];
        int len = snprintf(line, sizeof(line), "%d: inserted %zu breakpoints in %.2f seconds (%zu were already hit)\n",
                           (int)getpid(), n_inserted, now() - start, n_already_hit);
        if (write(log, line, len) < 0) {
            // Nothing to do about it.
        }
        close(log);
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
