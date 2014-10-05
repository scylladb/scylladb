/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

//
// Seastar memory allocator
//
// This is a share-nothing allocator (memory allocated on one cpu must
// be freed on the same cpu).
//
// Inspired by gperftools' tcmalloc.
//
// Memory map:
//
// 0x0000'sccc'vvvv'vvvv
//
// 0000 - required by architecture (only 48 bits of address space)
// s    - chosen to satisfy system allocator (1-7)
// ccc  - cpu number (0-12 bits allocated vary according to system)
// v    - virtual address within cpu (32-44 bits, according to how much ccc
//        leaves us
//
// Each page has a page structure that describes it.  Within a cpu's
// memory pool, the page array starts at offset 0, describing all pages
// within that pool.  Page 0 does not describe a valid page.
//
// Each pool can contain at most 2^32 pages (or 44 address bits), so we can
// use a 32-bit integer to identify a page.
//
// Runs of pages are organized into spans.  Free spans are organized into lists,
// by size.  When spans are broken up or coalesced, they may move into new lists.

#ifndef DEBUG

#include "bitops.hh"
#include "align.hh"
#include <new>
#include <cstdint>
#include <algorithm>
#include <limits>
#include <cassert>
#include <atomic>
#include <mutex>
#include <cstring>
#include <boost/intrusive/list.hpp>
#include <sys/mman.h>

namespace memory {

static constexpr const size_t page_bits = 12;
static constexpr const size_t page_size = 1 << page_bits;
static constexpr const unsigned cpu_id_shift = 36; // FIXME: make dynamic

using pageidx = uint32_t;

struct page;

namespace bi = boost::intrusive;

using page_list_link = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;


static char* mem_base() {
    static char* known;
    static std::once_flag flag;
    std::call_once(flag, [] {
        size_t alloc = size_t(1) << 44;
        auto r = ::mmap(NULL, 2 * alloc,
                    PROT_NONE,
                    MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
                    -1, 0);
        if (r == MAP_FAILED) {
            abort();
        }
        ::madvise(r, alloc, MADV_DONTDUMP);
        auto cr = reinterpret_cast<char*>(r);
        known = align_up(cr, alloc);
        ::munmap(cr, known - cr);
        ::munmap(known + alloc, cr + 2 * alloc - (known + alloc));
    });
    return known;
}

class small_pool;

struct free_object {
    free_object* next;
};

struct page {
    bool free;
    uint8_t offset_in_span;
    uint16_t nr_small_alloc;
    uint32_t span_size; // in pages, if we're the head or the tail
    page_list_link link;
    small_pool* pool;  // if used in a small_pool
    free_object* freelist;
};

// FIXME: use 32-bit pointers to save space
using page_list
        = bi::list<page,
                   bi::member_hook<page, page_list_link, &page::link>,
                   bi::constant_time_size<false>>;

class small_pool {
    unsigned _object_size;
    unsigned _span_size;
    free_object* _free = nullptr;
    size_t _free_count = 0;
    unsigned _min_free;
    unsigned _max_free;
    page_list _span_list;
    static constexpr unsigned idx_frac_bits = 2;
private:
    size_t span_bytes() const { return _span_size * page_size; }
public:
    explicit small_pool(unsigned object_size) noexcept;
    ~small_pool();
    void* allocate();
    void deallocate(void* object);
    unsigned object_size() const { return _object_size; }
    static constexpr unsigned size_to_idx(unsigned size);
    static constexpr unsigned idx_to_size(unsigned idx);
private:
    void add_more_objects();
    void trim_free_list();
    float waste();
};

// index 0b0001'1100 -> size (1 << 4) + 0b11 << (4 - 2)

constexpr unsigned
small_pool::idx_to_size(unsigned idx) {
    return (((1 << idx_frac_bits) | (idx & ((1 << idx_frac_bits) - 1)))
              << (idx >> idx_frac_bits))
                  >> idx_frac_bits;
}

static constexpr unsigned log2(unsigned n) {
    return std::numeric_limits<unsigned>::digits - count_leading_zeros(n - 1);
}

constexpr unsigned
small_pool::size_to_idx(unsigned size) {
    return ((log2(size) << idx_frac_bits) - ((1 << idx_frac_bits) - 1))
            + ((size - 1) >> (log2(size) - idx_frac_bits));
}

class small_pool_array {
public:
    static constexpr const unsigned nr_small_pools = small_pool::size_to_idx(4 * page_size) + 1;
private:
    union u {
        small_pool a[nr_small_pools];
        u() {
            for (unsigned i = 0; i < nr_small_pools; ++i) {
                new (&a[i]) small_pool(small_pool::idx_to_size(i));
            }
        }
        ~u() {
            // cannot really call destructor, since other
            // objects may be freed after we are gone.
        }
    } _u;
public:
    small_pool& operator[](unsigned idx) { return _u.a[idx]; }
};

static constexpr const size_t max_small_allocation
    = small_pool::idx_to_size(small_pool_array::nr_small_pools - 1);

struct cpu_pages {
    page* pages;
    uint32_t nr_pages;
    uint32_t nr_free_pages;
    unsigned cpu_id = -1U;
    static constexpr const unsigned nr_span_lists = 32;
    union pla {
        pla() {
            for (auto&& e : free_spans) {
                new (&e) page_list;
            }
        }
        ~pla() {
            // no destructor -- might be freeing after we die
        }
        page_list free_spans[nr_span_lists];  // contains spans with span_size >= 2^idx
    } fsu;
    small_pool_array small_pools;
    static std::atomic<unsigned> cpu_id_gen;
    char* mem() { return reinterpret_cast<char*>(pages); }

    void link(page_list& list, page* span);
    void unlink(page* span);
    struct trim {
        unsigned offset;
        unsigned nr_pages;
    };
    template <typename Trimmer>
    void* allocate_large_and_trim(unsigned nr_pages, Trimmer trimmer);
    void* allocate_large(unsigned nr_pages);
    void* allocate_large_aligned(unsigned align_pages, unsigned nr_pages);
    void free_large(void* ptr);
    void free_span(pageidx start, uint32_t nr_pages);
    void free_span_no_merge(pageidx start, uint32_t nr_pages);
    void* allocate_small(unsigned size);
    void free(void* ptr);
    void free(void* ptr, size_t size);
    size_t object_size(void* ptr);
    page* to_page(void* p) {
        return &pages[(reinterpret_cast<char*>(p) - mem()) / page_size];
    }

    bool initialize();
};

static thread_local cpu_pages cpu_mem;
std::atomic<unsigned> cpu_pages::cpu_id_gen;

static inline
unsigned index_of(unsigned pages) {
    return std::numeric_limits<unsigned>::digits - count_leading_zeros(pages) - 1;
}

void
cpu_pages::unlink(page* span) {
    span->link.unlink();
}

void
cpu_pages::link(page_list& list, page* span) {
    list.push_front(*span);
}

void cpu_pages::free_span_no_merge(uint32_t span_start, uint32_t nr_pages) {
    assert(nr_pages);
    nr_free_pages += nr_pages;
    auto span = &pages[span_start];
    auto span_end = &pages[span_start + nr_pages - 1];
    span->free = span_end->free = true;
    span->span_size = span_end->span_size = nr_pages;
    auto idx = index_of(nr_pages);
    link(fsu.free_spans[idx], span);
}

void cpu_pages::free_span(uint32_t span_start, uint32_t nr_pages) {
    page* before = &pages[span_start - 1];
    if (before->free) {
        auto b_size = before->span_size;
        assert(b_size);
        span_start -= b_size;
        nr_pages += b_size;
        nr_free_pages -= b_size;
        unlink(before - (b_size - 1));
    }
    page* after = &pages[span_start + nr_pages];
    if (after->free) {
        auto a_size = after->span_size;
        assert(a_size);
        nr_pages += a_size;
        nr_free_pages -= a_size;
        unlink(after);
    }
    free_span_no_merge(span_start, nr_pages);
}

template <typename Trimmer>
void*
cpu_pages::allocate_large_and_trim(unsigned n_pages, Trimmer trimmer) {
    auto idx = index_of(n_pages);
    assert(n_pages >= (1u << idx));
    assert(n_pages < (2u << idx));
    while (idx < nr_span_lists && fsu.free_spans[idx].empty()) {
        ++idx;
    }
    if (idx == nr_span_lists) {
        if (initialize()) {
            return allocate_large_and_trim(n_pages, trimmer);
        }
        // FIXME: request application to free memory
        throw std::bad_alloc();
    }
    auto& list = fsu.free_spans[idx];
    page* span = &list.front();
    auto span_size = span->span_size;
    auto span_idx = span - pages;
    unlink(span);
    nr_free_pages -= span->span_size;
    trim t = trimmer(span_idx, nr_pages);
    if (t.offset) {
        free_span_no_merge(span_idx, t.offset);
        span_idx += t.offset;
        span_size -= t.offset;
        span = &pages[span_idx];

    }
    if (t.nr_pages < span->span_size) {
        free_span_no_merge(span_idx + t.nr_pages, span_size - t.nr_pages);
        span_size = t.nr_pages;
    }
    auto span_end = &pages[span_idx + n_pages - 1];
    span->free = span_end->free = false;
    span->span_size = span_end->span_size = n_pages;
    span->pool = nullptr;
    return mem() + span_idx * page_size;
}

void*
cpu_pages::allocate_large(unsigned n_pages) {
    return allocate_large_and_trim(n_pages, [n_pages] (unsigned idx, unsigned n) {
        return trim{0, std::min(n, n_pages)};
    });
}

void*
cpu_pages::allocate_large_aligned(unsigned align_pages, unsigned n_pages) {
    return allocate_large_and_trim(n_pages + align_pages - 1, [=] (unsigned idx, unsigned n) {
        return trim{align_up(idx, align_pages) - idx, n_pages};
    });
}

void*
cpu_pages::allocate_small(unsigned size) {
    auto idx = small_pool::size_to_idx(size);
    auto& pool = small_pools[idx];
    assert(size <= pool.object_size());
    return pool.allocate();
}

void cpu_pages::free_large(void* ptr) {
    pageidx idx = (reinterpret_cast<char*>(ptr) - mem()) / page_size;
    page* span = &pages[idx];
    free_span(idx, span->span_size);
}

size_t cpu_pages::object_size(void* ptr) {
    pageidx idx = (reinterpret_cast<char*>(ptr) - mem()) / page_size;
    page* span = &pages[idx];
    if (span->pool) {
        return span->pool->object_size();
    } else {
        return size_t(span->span_size) * page_size;
    }
}

void cpu_pages::free(void* ptr) {
    assert(((reinterpret_cast<uintptr_t>(ptr) >> cpu_id_shift) & 0xff) == cpu_id);
    page* span = to_page(ptr);
    if (span->pool) {
        span->pool->deallocate(ptr);
    } else {
        free_large(ptr);
    }
}

void cpu_pages::free(void* ptr, size_t size) {
    assert(((reinterpret_cast<uintptr_t>(ptr) >> cpu_id_shift) & 0xff) == cpu_id);
    if (size <= max_small_allocation) {
        auto pool = &small_pools[small_pool::size_to_idx(size)];
        pool->deallocate(ptr);
    } else {
        free_large(ptr);
    }
}

bool cpu_pages::initialize() {
    if (nr_pages) {
        return false;
    }
    cpu_id = cpu_id_gen.fetch_add(1, std::memory_order_relaxed);
    auto base = mem_base() + (size_t(cpu_id) << cpu_id_shift);
    auto size = 1 << 30; // FIXME: determine dynamically
    auto r = ::mmap(base, size,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED,
            -1, 0);
    if (r == MAP_FAILED) {
        abort();
    }
    pages = reinterpret_cast<page*>(base);
    nr_pages = size / page_size;
    // we reserve the end page so we don't have to special case
    // the last span.
    auto reserved = align_up(sizeof(page) * (nr_pages + 1), page_size) / page_size;
    for (pageidx i = 0; i < reserved; ++i) {
        pages[i].free = false;
    }
    pages[nr_pages].free = false;
    free_span_no_merge(reserved, nr_pages - reserved);
    return true;
}

small_pool::small_pool(unsigned object_size) noexcept
    : _object_size(object_size), _span_size(1) {
    while (_object_size > span_bytes()
            || (_span_size < 32 && waste() > 0.05)
            || (span_bytes() / object_size < 32)) {
        _span_size *= 2;
    }
    _max_free = std::max<unsigned>(100, span_bytes() * 2 / _object_size);
    _min_free = _max_free / 2;
}

small_pool::~small_pool() {
    _min_free = _max_free = 0;
    trim_free_list();
}

void*
small_pool::allocate() {
    if (!_free) {
        add_more_objects();
    }
    auto* obj = _free;
    _free = _free->next;
    --_free_count;
    return obj;
}

void
small_pool::deallocate(void* object) {
    auto o = reinterpret_cast<free_object*>(object);
    o->next = _free;
    _free = o;
    ++_free_count;
    if (_free_count >= _max_free) {
        trim_free_list();
    }
}

void
small_pool::add_more_objects() {
    auto goal = (_min_free + _max_free) / 2;
    while (!_span_list.empty() && _free_count < goal) {
        page& span = _span_list.front();
        _span_list.pop_front();
        while (span.freelist) {
            auto obj = span.freelist;
            span.freelist = span.freelist->next;
            obj->next = _free;
            _free = obj;
            ++_free_count;
            ++span.nr_small_alloc;
        }
    }
    while (_free_count < goal) {
        auto data = reinterpret_cast<char*>(cpu_mem.allocate_large(_span_size));
        auto span = cpu_mem.to_page(data);
        for (unsigned i = 0; i < _span_size; ++i) {
            span[i].offset_in_span = i;
            span[i].pool = this;
        }
        span->nr_small_alloc = 0;
        span->freelist = nullptr;
        for (unsigned offset = 0; offset <= span_bytes() - _object_size; offset += _object_size) {
            auto h = reinterpret_cast<free_object*>(data + offset);
            h->next = _free;
            _free = h;
            ++_free_count;
            ++span->nr_small_alloc;
        }
    }
}

void
small_pool::trim_free_list() {
    auto goal = (_min_free + _max_free) / 2;
    while (_free && _free_count > goal) {
        auto obj = _free;
        _free = _free->next;
        --_free_count;
        page* span = cpu_mem.to_page(obj);
        span -= span->offset_in_span;
        if (!span->freelist) {
            new (&span->link) page_list_link();
            _span_list.push_front(*span);
        }
        obj->next = span->freelist;
        span->freelist = obj;
        if (--span->nr_small_alloc == 0) {
            span->link.unlink();
            cpu_mem.free_span(span - cpu_mem.pages, span->span_size);
        }
    }
}

float small_pool::waste() {
    return (span_bytes() % _object_size) / (1.0 * span_bytes());
}

void* allocate_large(size_t size) {
    unsigned size_in_pages = (size + page_size - 1) >> page_bits;
    assert((size_t(size_in_pages) << page_bits) >= size);
    return cpu_mem.allocate_large(size_in_pages);

}

void* allocate_large_aligned(size_t align, size_t size) {
    unsigned size_in_pages = (size + page_size - 1) >> page_bits;
    unsigned align_in_pages = std::max(align, page_size) >> page_bits;
    return cpu_mem.allocate_large_aligned(align_in_pages, size_in_pages);
}

void free_large(void* ptr) {
    return cpu_mem.free_large(ptr);
}

size_t object_size(void* ptr) {
    return cpu_mem.object_size(ptr);
}

void* allocate(size_t size) {
    if (size <= sizeof(free_object)) {
        size = sizeof(free_object);
    }
    if (size <= max_small_allocation) {
        return cpu_mem.allocate_small(size);
    } else {
        return allocate_large(size);
    }
}

void* allocate_aligned(size_t align, size_t size) {
    size = std::max(size, align);
    if (size <= sizeof(free_object)) {
        size = sizeof(free_object);
    }
    if (size <= max_small_allocation) {
        return cpu_mem.allocate_small(size);
    } else {
        return allocate_large_aligned(align, size);
    }
}

void free(void* obj) {
    cpu_mem.free(obj);
}

void free(void* obj, size_t size) {
    cpu_mem.free(obj, size);
}

}

using namespace memory;

extern "C"
void* malloc(size_t n) throw () {
    if (n == 0) {
        return nullptr;
    }
    try {
        return allocate(n);
    } catch (std::bad_alloc& ba) {
        return nullptr;
    }
}

extern "C"
[[gnu::alias("malloc")]]
void* __libc_malloc(size_t n) throw ();

extern "C"
void free(void* ptr) {
    if (ptr) {
        memory::free(ptr);
    }
}

extern "C"
[[gnu::alias("free")]]
void* __libc_free(void* obj) throw ();

extern "C"
void* calloc(size_t nmemb, size_t size) {
    auto s1 = __int128(nmemb) * __int128(size);
    assert(s1 == size_t(s1));
    size_t s = s1;
    auto p = malloc(s);
    if (p) {
        std::memset(p, 0, s);
    }
    return p;
}

extern "C"
[[gnu::alias("calloc")]]
void* __libc_calloc(size_t n, size_t m) throw ();

extern "C"
void* realloc(void* ptr, size_t size) {
    auto old_size = ptr ? object_size(ptr) : 0;
    auto nptr = malloc(size);
    if (!nptr) {
        return nptr;
    }
    std::memcpy(nptr, ptr, std::min(size, old_size));
    ::free(ptr);
    return nptr;
}

extern "C"
[[gnu::alias("realloc")]]
void* __libc_realloc(void* obj, size_t size) throw ();

extern "C"
int posix_memalign(void** ptr, size_t align, size_t size) {
    try {
        *ptr = allocate_aligned(align, size);
        return 0;
    } catch (std::bad_alloc&) {
        return ENOMEM;
    }
}

extern "C"
[[gnu::alias("posix_memalign")]]
int __libc_posix_memalign(void** ptr, size_t align, size_t size) throw ();

extern "C"
void* memalign(size_t align, size_t size) {
    try {
        return allocate_aligned(align, size);
    } catch (std::bad_alloc&) {
        return NULL;
    }
}

extern "C"
[[gnu::alias("memalign")]]
void* __libc_memalign(size_t align, size_t size);

extern "C"
void cfree(void* obj) {
    return ::free(obj);
}

extern "C"
[[gnu::alias("cfree")]]
void __libc_cfree(void* obj);

extern "C"
size_t malloc_usable_size(void* obj) {
    return object_size(obj);
}

extern "C"
int malloc_trim(size_t pad) {
    return 0;
}

void* operator new(size_t size) {
    if (size == 0) {
        size = 1;
    }
    return allocate(size);
}

void* operator new[](size_t size) {
    if (size == 0) {
        size = 1;
    }
    return allocate(size);
}

void operator delete(void* ptr) throw () {
    if (ptr) {
        memory::free(ptr);
    }
}

void operator delete[](void* ptr) throw () {
    if (ptr) {
        memory::free(ptr);
    }
}

void operator delete(void* ptr, size_t size) {
    if (ptr) {
        memory::free(ptr, size);
    }
}

void operator delete[](void* ptr, size_t size) {
    if (ptr) {
        memory::free(ptr, size);
    }
}

void* operator new(size_t size, std::nothrow_t) throw () {
    if (size == 0) {
        size = 1;
    }
    try {
        return allocate(size);
    } catch (...) {
        return nullptr;
    }
}

void* operator new[](size_t size, std::nothrow_t) throw () {
    if (size == 0) {
        size = 1;
    }
    try {
        return allocate(size);
    } catch (...) {
        return nullptr;
    }
}

void operator delete(void* ptr, std::nothrow_t) throw () {
    if (ptr) {
        memory::free(ptr);
    }
}

void operator delete[](void* ptr, std::nothrow_t) throw () {
    if (ptr) {
        memory::free(ptr);
    }
}

void operator delete(void* ptr, size_t size, std::nothrow_t) throw () {
    if (ptr) {
        memory::free(ptr, size);
    }
}

void operator delete[](void* ptr, size_t size, std::nothrow_t) throw () {
    if (ptr) {
        memory::free(ptr, size);
    }
}

#endif
