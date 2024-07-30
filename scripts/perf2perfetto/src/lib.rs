// SPDX-License-Identifier: GPL-2.0-or-later
// SPDX-FileCopyrightText: Copyright 2024 Michał Chojnowski <michal.chojnowski@scylladb.com>

mod perf {
    #![allow(non_camel_case_types)]
    #![allow(non_upper_case_globals)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use ::std::os::raw::{c_int, c_void};
use std::os::unix::prelude::OsStrExt;
use ftf::Caches;
use numtoa::NumToA;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;

#[no_mangle]
pub static mut perf_dlfilter_fns: std::mem::MaybeUninit<perf::perf_dlfilter_fns> =
    std::mem::MaybeUninit::<perf::perf_dlfilter_fns>::uninit();

mod libc_helpers {
    #![allow(dead_code)]

    extern "C" {
        static stdout: *mut libc::FILE;
    }

    unsafe fn libc_print(s: &str) {
        libc::fwrite(s.as_ptr() as *const libc::c_void, 1, s.len(), stdout);
        libc::fflush(stdout);
    }
}

fn merge_hashsets<K: Eq + std::hash::Hash>(mut a: HashSet<K>, mut b: HashSet<K>) -> HashSet<K> {
    if a.len() < b.len() {
        std::mem::swap(&mut a, &mut b)
    }
    a.extend(b.into_iter());
    a
}

type CacheLine = u64;

#[derive(Default)]
struct FrameData {
    start_insn_cnt: u64,
    start_cyc_cnt: u64,
    start_timestamp: u64,
    footprint: HashSet<CacheLine>,
}

struct ThreadState {
    insn_cnt: u64,
    cyc_cnt: u64,
    ip: u64,
    stack: Vec<FrameData>,
    last_seen_time: u64,
    pid_tid: (u64, u64),
}

#[derive(Clone, Copy)]
enum TimestampMode {
    Time,
    Cycles,
    Instructions,
}

impl TimestampMode {
    fn choose(&self, time: u64, cyc: u64, insns: u64) -> u64 {
        match &self {
            TimestampMode::Time => time,
            TimestampMode::Cycles => cyc,
            TimestampMode::Instructions => insns,
        }
    }
}

struct State {
    w: BufWriter<File>,
    c: ftf::Caches,
    has_insns_events: bool,
    threads: std::collections::HashMap<u64, ThreadState>,
    mode: TimestampMode,
}

unsafe fn resolve_addr<'a>(
    sample: &perf::perf_dlfilter_sample,
    ctx: *mut c_void,
    buf: &'a mut [u8],
) -> &'a str {
    if sample.addr_correlates_sym != 0 {
        let raw_symbol = perf_dlfilter_fns.assume_init().resolve_addr.unwrap()(ctx);
        if !(*raw_symbol).sym.is_null() {
            let symbol = std::ffi::CStr::from_ptr((*raw_symbol).sym);
            symbol.to_str().unwrap_or("Non UTF-8 symbol")
        } else {
            sample.addr.numtoa(16, buf);
            &std::str::from_utf8_unchecked(&buf[4..])
        }
    } else {
        sample.addr.numtoa(16, buf);
        &std::str::from_utf8_unchecked(&buf[4..])
    }
}

unsafe fn resolve_ip<'a>(
    sample: &perf::perf_dlfilter_sample,
    ctx: *mut c_void,
    buf: &'a mut [u8],
) -> &'a str {
    let raw_symbol = perf_dlfilter_fns.assume_init().resolve_ip.unwrap()(ctx);
    if !(*raw_symbol).sym.is_null() {
        let symbol = std::ffi::CStr::from_ptr((*raw_symbol).sym);
        symbol.to_str().unwrap_or("Non UTF-8 symbol")
    } else {
        sample.ip.numtoa(16, buf);
        &std::str::from_utf8_unchecked(&buf[4..])
    }
}
const CACHE_LINE_SIZE: u64 = 64;

unsafe fn pop_frame(w: &mut dyn Write, c: &mut Caches, mode: TimestampMode, tstate: &mut ThreadState, _ctx: *mut c_void) {
    let frame = tstate.stack.last().unwrap();
    ftf::write_frame_end(
        w,
        c,
        mode.choose(tstate.last_seen_time, tstate.cyc_cnt, tstate.insn_cnt),
        tstate.pid_tid,
        tstate.insn_cnt - frame.start_insn_cnt,
        tstate.cyc_cnt - frame.start_cyc_cnt,
        frame.footprint.len() as u64 * CACHE_LINE_SIZE,
        frame.start_timestamp,
        tstate.last_seen_time,
    )
    .unwrap();
    let top_footprint = tstate.stack.pop().unwrap().footprint;
    let merged = merge_hashsets(
        std::mem::take(&mut tstate.stack.last_mut().unwrap().footprint),
        top_footprint,
    );
    tstate.stack.last_mut().unwrap().footprint = merged;
}

unsafe fn pop_unknown_frame(w: &mut dyn Write, c: &mut Caches, mode: TimestampMode, tstate: &mut ThreadState, sample: &perf::perf_dlfilter_sample, ctx: *mut c_void) {
    let frame = tstate.stack.last().unwrap();
    let mut buffer = ['0' as u8; 20];
    let sym = resolve_ip(sample, ctx, &mut buffer[..]);
    let ts = mode.choose(tstate.last_seen_time, tstate.cyc_cnt, tstate.insn_cnt);
    ftf::write_frame_full(
        w,
        c,
        ts,
        tstate.pid_tid,
        tstate.insn_cnt - frame.start_insn_cnt,
        tstate.cyc_cnt - frame.start_cyc_cnt,
        frame.footprint.len() as u64 * CACHE_LINE_SIZE,
        sym,
        ts,
        frame.start_timestamp,
        sample.time,
    )
    .unwrap();
}

unsafe fn push_frame(w: &mut dyn Write, c: &mut Caches, mode: TimestampMode, tstate: &mut ThreadState, sample: &perf::perf_dlfilter_sample, ctx: *mut c_void) {
    let mut buffer = ['0' as u8; 20];

    let sym = if tstate.stack.len() > 1 {
        resolve_addr(sample, ctx, &mut buffer[..])
    } else {
        "TRACE"
    };

    ftf::write_frame_start(
        w,
        c,
        mode.choose(tstate.last_seen_time, tstate.cyc_cnt, tstate.insn_cnt),
        (sample.pid as u64, sample.tid as u64),
        sym,
    )
    .unwrap();

    tstate.stack.push(FrameData {
        start_cyc_cnt: tstate.cyc_cnt,
        start_insn_cnt: tstate.insn_cnt,
        start_timestamp: sample.time,
        footprint: HashSet::new(),
    });
}

#[no_mangle]
pub unsafe extern "C" fn filter_event_early(
    raw_state: *mut c_void,
    sample: &perf::perf_dlfilter_sample,
    ctx: *mut c_void,
) -> c_int {
    // The semantics of the `stack` in ThreadState are as follows:
    // Frame 0 contains counters for the entire trace.
    // Frame 1 contains counters for the current contiguous trace segment. It is closed
    // and reopened on `tr end` and errors.
    // Frames 2.. contain the call stack as it is known. They are opened on calls (or interrupts)
    // and closed (merged into the parent frame) on returns.
    //
    // Since the current trace segment could have started in the middle of a real
    // stack frame, ancestors of the starting frame are not here.
    // They will be only noticed when they return and printed with counters taken from frame 1.

    let state: &mut State = &mut *raw_state.cast::<State>();
    let tstate: &mut ThreadState =
        &mut state
            .threads
            .entry(sample.tid as u64)
            .or_insert_with(|| ThreadState {
                cyc_cnt: 0,
                insn_cnt: 0,
                ip: 0,
                stack: vec![FrameData::default(), FrameData::default()],
                last_seen_time: 0,
                pid_tid: (sample.pid as u64, sample.tid as u64),
            });

    tstate.last_seen_time = sample.time;

    if (*sample.event) == 'b' as i8 {
        // 'branches' event
        if !state.has_insns_events {
            // If the user has piped instruction events to the filter,
            // then we do an exact count of instructions.
            // Otherwise we use the approximate (updated on CYC packets) count 
            // provided by perf.
            tstate.insn_cnt += sample.insn_cnt;
        }
        tstate.cyc_cnt += sample.cyc_cnt;
        // Not all decoded errors cause a 'tr end'. Some are recovered from,
        // and the output continues.
        // Unfortunately perf doesn't notify the filter about that,
        // so we need to cope with gaps in the input that appear sometimes.
        // 
        // Here we try to guess when a gap occured. If `ip` became smaller
        // since the last sample or it grew by more than BAD_JUMP_HEURISITIC,
        // we guess that an error has occured.
        const BAD_JUMP_HEURISTIC: u64 = 0x1000;
        if sample.ip.wrapping_sub(tstate.ip) > BAD_JUMP_HEURISTIC {
            tstate.ip = 0;
        }

        if tstate.ip != 0 && sample.ip != 0 {
            // No errors. The normal path. We update the cache footprint info.
            const CACHE_LINE_MASK: u64 = !(CACHE_LINE_SIZE - 1);
            let cache_line_start = tstate.ip & CACHE_LINE_MASK;
            let cache_line_end = (sample.ip) & CACHE_LINE_MASK;
            let mut cache_line = cache_line_start;
            // Here we update the icache footprint set of the current frame.
            // The current implementation is dumb and just inserts all touched lines into
            // a set.
            while cache_line <= cache_line_end {
                tstate
                    .stack
                    .last_mut()
                    .unwrap()
                    .footprint
                    .insert(cache_line);
                cache_line += CACHE_LINE_SIZE;
            }
        } else {
            // `ip` equal to 0 means that a contiguous trace segment has ended (`tr end`)
            //  or an error has occured.
            // We close all open stack frames.
            // We also close the special frame 1, (the current contiguous trace segment) and reopen it.
            while tstate.stack.len() > 1 {
                pop_frame(&mut state.w, &mut state.c, state.mode, tstate, ctx);
            }
            push_frame(&mut state.w, &mut state.c, state.mode, tstate, sample, ctx);
        }

        tstate.ip = sample.addr;

        if sample.flags & perf::PERF_DLFILTER_FLAG_CALL != 0 {
            push_frame(&mut state.w, &mut state.c, state.mode, tstate, sample, ctx);
        } else if sample.flags & perf::PERF_DLFILTER_FLAG_RETURN != 0 {
            if tstate.stack.len() > 2 {
                // This return matches a previously seen call.
                pop_frame(&mut state.w, &mut state.c, state.mode, tstate, ctx);
            } else {
                // This return does not match a previous call, so the current trace fragment
                // started inside the frame.
                //
                // The current implementation handles that by writing a single point in time (the end of the frame)
                // to output.
                // It would be better to show the full known time span it in the trace, but we only learn about it at the end,
                // and ftf requires spans to be nested properly. In other words, to represent those front-truncated frames properly
                // we would have to delay all output until the current trace fragment ends.
                pop_unknown_frame(&mut state.w, &mut state.c, state.mode, tstate, sample, ctx);
            }
        }
    } else {
        // 'instructions' event
        state.has_insns_events = true;
        tstate.insn_cnt += 1;
    }
    return 1;
}

#[no_mangle]
pub unsafe extern "C" fn start(data: &mut *mut c_void, ctx: *mut c_void) -> c_int {
    let mut argc: c_int = 0;
    let argv = (perf_dlfilter_fns.assume_init().args.unwrap())(ctx, &mut argc as *mut c_int);
    let args = std::slice::from_raw_parts(argv, argc as usize);

    let filename = {
        if args.len() >= 1 {
            std::ffi::OsStr::from_bytes(std::ffi::CStr::from_ptr(args[0]).to_bytes())
        } else {
            std::ffi::OsStr::new("out.ftf")
        }
    };
    let mode = {
        let default_mode = TimestampMode::Instructions;
        if args.len() >= 2 {
            match *args[1] as u8 as char {
                'c' => TimestampMode::Cycles,
                't' => TimestampMode::Time,
                'i' => TimestampMode::Instructions,
                _ => default_mode,
            }
        } else {
            default_mode
        }
    };

    let file = File::create(filename).unwrap();
    let mut w = BufWriter::new(file);
    ftf::write_header(&mut w).unwrap();
    let state = State {
        threads: std::collections::HashMap::new(),
        w,
        c: ftf::Caches::default(),
        has_insns_events: false,
        mode,
    };
    *data = Box::into_raw(Box::new(state)).cast::<c_void>();
    0
}

#[no_mangle]
pub unsafe extern "C" fn stop(raw_state: *mut c_void, ctx: *mut c_void) -> c_int {
    let state: &mut State = &mut *raw_state.cast::<State>();
    for (_tid, tstate) in &mut state.threads {
        while tstate.stack.len() > 1 {
            pop_frame(&mut state.w, &mut state.c, state.mode, tstate, ctx);
        }
    }
    state.w.flush().unwrap();
    drop(Box::from_raw(raw_state));
    0
}

mod ftf {
    use lru::LruCache;
    use std::cmp::min;
    use core::num::NonZeroUsize;

    pub enum CacheRef {
        Idx(u64),
    }

    pub struct StringCache {
        lru: LruCache<String, u16>,
    }

    impl Default for StringCache {
        fn default() -> Self {
            const STRING_TABLE_SIZE: usize = 32 * 1024 - StringCache::RESERVED as usize;
            let lru = LruCache::new(NonZeroUsize::new(STRING_TABLE_SIZE).unwrap());
            Self { lru }
        }
    }

    fn write_string_record(w: &mut dyn Write, idx: u64, s: &str) -> std::io::Result<()> {
        const MAX_STRING_LEN: usize = 32000;
        let s_len = min(s.len(), MAX_STRING_LEN);
        let rsize = 1 + words_for_bytes(s_len);
        let rtype = 2;
        write_u64(
            w,
            rtype | rsize << 4 | (idx as u64) << 16 | (s_len as u64) << 32,
        )?;
        write_string(w, &s.as_bytes()[..s_len])?;
        Ok(())
    }

    impl StringCache {
        const RESERVED: u64 = InternalString::COUNT as u64;
        pub fn get_ref(&mut self, w: &mut dyn Write, s: &str) -> std::io::Result<CacheRef> {
            if s.is_empty() {
                return Ok(CacheRef::Idx(0));
            }
            if let Some(idx) = self.lru.get(s) {
                let out = *idx as u64 + Self::RESERVED;
                return Ok(CacheRef::Idx(out));
            } else {
                let idx = if self.lru.len() < self.lru.cap().into() {
                    self.lru.len() as u16
                } else {
                    self.lru.pop_lru().unwrap().1
                };
                self.lru.put(s.to_string(), idx);
                let out = idx as u64 + Self::RESERVED;
                write_string_record(w, out, s)?;
                return Ok(CacheRef::Idx(out));
            }
        }
    }

    pub struct ThreadCache {
        lru: LruCache<(u64, u64), u8>,
    }

    impl Default for ThreadCache {
        fn default() -> Self {
            const THREAD_TABLE_SIZE: usize = 256 - ThreadCache::RESERVED as usize;
            let lru = LruCache::new(NonZeroUsize::new(THREAD_TABLE_SIZE).unwrap());
            Self { lru }
        }
    }

    fn write_thread_record(
        w: &mut dyn Write,
        idx: u64,
        pid_tid: (u64, u64),
    ) -> std::io::Result<()> {
        let rsize = 3;
        let rtype = 3;
        write_u64(w, rtype | rsize << 4 | (idx as u64) << 16)?;
        write_u64(w, pid_tid.0)?;
        write_u64(w, pid_tid.1)?;
        Ok(())
    }

    impl ThreadCache {
        const RESERVED: u64 = 1;
        pub fn get_ref(
            &mut self,
            w: &mut dyn Write,
            pid_tid: (u64, u64),
        ) -> std::io::Result<CacheRef> {
            if let Some(idx) = self.lru.get(&pid_tid) {
                return Ok(CacheRef::Idx(*idx as u64 + Self::RESERVED));
            } else {
                let idx = if self.lru.len() < self.lru.cap().into() {
                    self.lru.len() as u8
                } else {
                    self.lru.pop_lru().unwrap().1
                };
                self.lru.put(pid_tid, idx);
                let out = idx as u64 + Self::RESERVED;
                write_thread_record(w, out, pid_tid)?;
                return Ok(CacheRef::Idx(out));
            }
        }
    }

    #[derive(Default)]
    pub struct Caches {
        string_cache: StringCache,
        thread_cache: ThreadCache,
    }

    use std::io::Write;

    struct EventHeader<'call> {
        name: &'call str,
        category: &'call str,
        pid_tid: (u64, u64),
        timestamp: u64,
        nargs: u8,
        etype: u8,
        extra_data_size: usize,
    }

    fn write_u64(w: &mut dyn Write, x: u64) -> std::io::Result<()> {
        w.write_all(&x.to_ne_bytes())?;
        Ok(())
    }
    fn write_string(w: &mut dyn Write, s: &[u8]) -> std::io::Result<()> {
        w.write_all(s)?;
        if s.len() % 8 != 0 {
            w.write_all(&[0; 8][..8 - s.len() % 8])?; // Pad to 8 bytes.
        }
        Ok(())
    }

    use strum::{EnumCount, IntoEnumIterator};
    use strum_macros::{EnumCount, EnumIter};

    #[derive(EnumIter, EnumCount, Clone, Copy)]
    enum InternalString {
        Empty,
        Instructions,
        Cycles,
        Footprint,
        Symbol,
        Timespan,
    }

    fn internal_string(x: InternalString) -> &'static str {
        match x {
            InternalString::Empty => "",
            InternalString::Instructions => "Instructions",
            InternalString::Cycles => "Cycles",
            InternalString::Footprint => "Footprint",
            InternalString::Symbol => "Symbol",
            InternalString::Timespan => "Timespan",
        }
    }

    fn write_event_header(
        c: &mut Caches,
        w: &mut dyn Write,
        e: EventHeader,
    ) -> std::io::Result<()> {
        let rtype = 4 as u64;
        let rsize = 2 + e.extra_data_size as u64;
        let CacheRef::Idx(name_ref) = c.string_cache.get_ref(w, e.name)?;
        let CacheRef::Idx(category_ref) = c.string_cache.get_ref(w, e.category)?;
        let CacheRef::Idx(thread_ref) = c.thread_cache.get_ref(w, e.pid_tid)?;
        write_u64(
            w,
            rtype
                | rsize << 4
                | (e.etype as u64) << 16
                | (e.nargs as u64) << 20
                | thread_ref << 24
                | category_ref << 32
                | name_ref << 48,
        )?;
        write_u64(w, e.timestamp)?;
        Ok(())
    }

    fn div_up<T: num_traits::Unsigned + num_traits::Num + std::cmp::PartialEq + Copy>(
        a: T,
        b: T,
    ) -> T {
        (a / b)
            + if a % b != T::zero() {
                T::one()
            } else {
                T::zero()
            }
    }

    fn words_for_bytes(x: usize) -> u64 {
        div_up(x, 8) as u64
    }

    // Prints a timestamp in this format: 1234.567890000
    fn print_timestamp(buf: &mut [u8], mut nanos: u64) -> &[u8] {
        let mut i = 0;
        while i < 9 {
            buf[buf.len() - i - 1] = '0' as u8 + (nanos % 10) as u8;
            nanos /= 10;
            i += 1;
        }
        buf[buf.len() - i - 1] = '.' as u8;
        i += 1;
        while i < 11 || nanos > 0 {
            buf[buf.len() - 1 - i] = '0' as u8 + (nanos % 10) as u8;
            nanos /= 10;
            i += 1;
        }
        &buf[buf.len() - i..]
    }

    // Prints a timespan in this format: 1234.567890000,2345.678912340
    pub fn print_timespan(buf: &mut [u8], timespan: (u64, u64)) -> &str {
        let len_1 = print_timestamp(buf, timespan.1).len();
        buf[buf.len() - len_1 - 1] = ',' as u8;
        let full_len = buf.len();
        let len_0 = print_timestamp(&mut buf[..full_len - len_1 - 1], timespan.0).len();
        unsafe { &std::str::from_utf8_unchecked(&buf[buf.len() - len_1 - 1 - len_0..]) }
    }

    pub fn write_info_args(
        w: &mut dyn Write,
        insns: u64,
        cycles: u64,
        footprint: u64,
        timespan: &str,
    ) -> std::io::Result<()> {
        write_u64(w, 4 | 2 << 4 | (InternalString::Instructions as u64) << 16)?;
        write_u64(w, insns)?;

        write_u64(w, 4 | 2 << 4 | (InternalString::Cycles as u64) << 16)?;
        write_u64(w, cycles)?;

        write_u64(w, 4 | 2 << 4 | (InternalString::Footprint as u64) << 16)?;
        write_u64(w, footprint)?;

        let ts_size = 1 + words_for_bytes(timespan.len());
        write_u64(
            w,
            6 | ts_size << 4
                | (InternalString::Timespan as u64) << 16
                | (timespan.len() as u64) << 32
                | 1 << 47,
        )?;
        write_string(w, timespan.as_bytes())?;

        Ok(())
    }

    // Number of arguments, total size (not counting strings)
    pub fn info_nargs_size(timespan: &str) -> (u8, usize) {
        (4, 7 + words_for_bytes(timespan.len()) as usize)
    }

    pub fn write_header(w: &mut dyn Write) -> std::io::Result<()> {
        // Magic number.
        write_u64(w, 0x0016547846040010_u64)?;

        // Provider info metadata.
        let rtype = 0;
        let mtype = 1;
        let name = "scylla";
        let name_len = name.len() as u64;
        let rsize = 1 + words_for_bytes(name.len());
        let provider_id = 0; // The only provider.
        write_u64(
            w,
            rtype | rsize << 4 | mtype << 16 | provider_id << 20 | name_len << 52,
        )?;
        write_string(w, name.as_bytes())?;

        // Provider section metadata.
        let rtype = 0;
        let rsize = 1;
        let mtype = 2;
        write_u64(w, rtype | rsize << 4 | mtype << 16 | provider_id << 20)?;

        // Internal strings
        for i in InternalString::iter().skip(1) {
            write_string_record(w, i as u64, internal_string(i))?;
        }
        Ok(())
    }

    pub fn write_frame_start(
        w: &mut dyn Write,
        c: &mut Caches,
        timestamp: u64,
        pid_tid: (u64, u64),
        symbol: &str,
    ) -> std::io::Result<()> {
        write_event_header(
            c,
            w,
            EventHeader {
                etype: 2, // Duration start
                nargs: 0,
                timestamp,
                pid_tid,
                category: "Misc",
                name: symbol,
                extra_data_size: 0,
            },
        )
    }

    pub fn write_frame_end(
        w: &mut dyn Write,
        c: &mut Caches,
        timestamp: u64,
        pid_tid: (u64, u64),
        insns: u64,
        cycles: u64,
        footprint: u64,
        ts_start: u64,
        ts_end: u64,
    ) -> std::io::Result<()> {
        let mut buf = [0u8; 48];
        let ts = print_timespan(&mut buf, (ts_start, ts_end));
        write_event_header(
            c,
            w,
            EventHeader {
                etype: 3, // Duration start
                nargs: info_nargs_size(ts).0,
                timestamp,
                pid_tid,
                category: "Misc",
                name: "",
                extra_data_size: info_nargs_size(ts).1,
            },
        )?;
        write_info_args(w, insns, cycles, footprint, ts)
    }

    pub fn write_frame_full(
        w: &mut dyn Write,
        c: &mut Caches,
        timestamp: u64,
        pid_tid: (u64, u64),
        insns: u64,
        cycles: u64,
        footprint: u64,
        symbol: &str,
        end_timestamp: u64,
        ts_start: u64,
        ts_end: u64,
    ) -> std::io::Result<()> {
        let mut buf = [0u8; 48];
        let ts = print_timespan(&mut buf, (ts_start, ts_end));
        write_event_header(
            c,
            w,
            EventHeader {
                etype: 4,
                nargs: info_nargs_size(ts).0,
                timestamp,
                pid_tid,
                category: "Misc",
                name: symbol,
                extra_data_size: 1 + info_nargs_size(ts).1,
            },
        )?;
        write_info_args(w, insns, cycles, footprint, ts)?;
        write_u64(w, end_timestamp)
    }
}
