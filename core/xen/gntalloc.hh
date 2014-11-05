#ifndef _XEN_GNTALLOC_HH
#define _XEN_GNTALLOC_HH

#include "core/posix.hh"

class gntref {
public:
    int xen_id;
    void* page;
    bool operator==(const gntref &a) { return (xen_id == a.xen_id) && (page == a.page); }
    gntref& operator=(const gntref &a) { xen_id = a.xen_id; page = a.page; return *this; }
    gntref(int id, void *page) : xen_id(id), page(page) {}
    gntref() : xen_id(-1), page(nullptr) {}
    operator bool() const { return xen_id != -1 && page != nullptr; }
};

class gntalloc;

class grant_head {
protected:
    unsigned _id = 0;
    std::vector<gntref> _refs;
public:
    grant_head(std::vector<gntref> r) : _refs(r) {}
    virtual gntref& new_ref() = 0;
    virtual gntref& new_ref(void *addr, size_t size) = 0;
};

class gntalloc {
protected:
    static gntalloc *_instance;
    unsigned _otherend;
public:
    static gntalloc *instance(bool userspace, unsigned otherend);
    static gntalloc *instance();
    gntalloc(unsigned otherend) : _otherend(otherend) {}
    virtual gntref alloc_ref() = 0;
    // The kernel interface can defer allocation, userspace allocation
    // cannot. The boolean "alloc" tell us whether or not we should allocate
    // now or try to defer.
    virtual grant_head *alloc_ref(unsigned nr_ents, bool alloc) = 0;
    friend class grant_head;
};

extern gntref invalid_ref;
#endif
