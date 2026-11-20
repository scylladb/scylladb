/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

/*
 * USDT (user statically-defined tracing) probe points, provider "scylla".
 *
 * The probe site compiles to one nop plus an ELF note (.note.stapsdt);
 * a tracer such as bpftrace patches the nop into a trap when it attaches:
 *     SCYLLA_USDT(query_done, ks_name, cf_name, duration_us);
 *     bpftrace -e 'usdt:/path/to/scylla:scylla:query_done { ... }'
 *
 * The argument expressions run on every hit, with or without a tracer,
 * so they must be cheap and side-effect free: integers and pointers to
 * existing data. That evaluation is the only cost of an idle probe.
 * At most 6 arguments: bpftrace reads USDT arguments arg0 to arg5 only
 * on x86_64. See docs/dev/usdt-tracing.md for conventions.
 */

#include <chrono>
#include <cstdint>

namespace utils {
// Microseconds elapsed since `start`, for probe duration arguments.
inline int64_t usdt_us_since(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
}
// Never called; keeps probe arguments referenced when probes are compiled out.
template <typename... Args>
inline void usdt_unused(Args&&...) noexcept {}
}

#if __has_include(<sys/sdt.h>)
#define SCYLLA_USDT_ENABLED 1
#include <sys/sdt.h>
// Dispatch to STAP_PROBE<n> based on the number of probe arguments
// (older sys/sdt.h versions lack the variadic STAP_PROBEV).
#define _SCYLLA_USDT_NARG(...) _SCYLLA_USDT_NARG_(__VA_ARGS__, 6, 5, 4, 3, 2, 1,)
#define _SCYLLA_USDT_NARG_(_0, _1, _2, _3, _4, _5, _6, N, ...) N
#define _SCYLLA_USDT_CAT(a, b) _SCYLLA_USDT_CAT_(a, b)
#define _SCYLLA_USDT_CAT_(a, b) a##b
#define SCYLLA_USDT(name, ...) \
    _SCYLLA_USDT_CAT(STAP_PROBE, _SCYLLA_USDT_NARG(0 __VA_OPT__(,) __VA_ARGS__))(scylla, name __VA_OPT__(,) __VA_ARGS__)
#else
// The arguments are not evaluated, but stay referenced so that locals
// computed only for a probe do not trigger unused-variable warnings.
#define SCYLLA_USDT(name, ...) \
    do { if (false) { ::utils::usdt_unused(__VA_ARGS__); } } while (false)
#endif
