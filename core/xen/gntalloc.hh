#ifndef _XEN_GNTALLOC_HH
#define _XEN_GNTALLOC_HH

#include "core/posix.hh"

typedef std::pair<int, void *> gntref;

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
#endif
