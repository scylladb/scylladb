/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#ifndef _XEN_GNTALLOC_HH
#define _XEN_GNTALLOC_HH

#include "core/posix.hh"

namespace xen {

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
public:
    virtual gntref new_ref() = 0;
    virtual gntref new_ref(void *addr, size_t size) = 0;
    virtual void free_ref(gntref& ref) = 0;
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
    virtual grant_head *alloc_ref(unsigned nr_ents) = 0;
    friend class grant_head;
};

extern gntref invalid_ref;

}

#endif
