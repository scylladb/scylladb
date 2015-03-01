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
/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef XENSTORE_HH_
#define XENSTORE_HH_

#include <list>

extern "C" {
    #include <xenstore.h>
}

class xenstore {
public:
    class xenstore_transaction {
    private:
        xenstore *_x;
    protected:
        xs_transaction_t _t = 0;
    public:
        explicit xenstore_transaction(xenstore *x) : _x(x)
                                                   , _t(x->start_transaction()) {}
        explicit xenstore_transaction() {}

        ~xenstore_transaction() { if (_t) { _x->end_transaction(_t);  } }

        xs_transaction_t t() { return _t; }
        friend class xenstore;
    };

protected:
    xs_transaction_t start_transaction();
    void end_transaction(xs_transaction_t t);

    static xenstore_transaction _xs_null; // having it here simplify forward decls

private:
    static xenstore *_instance;
    struct xs_handle *_h;
    explicit xenstore();
    ~xenstore();

public:
    static xenstore *instance();

    virtual void write(std::string path, std::string value, xenstore_transaction &t = _xs_null);
    virtual void remove(std::string path, xenstore_transaction &t = _xs_null);

    virtual std::string read(std::string path, xenstore_transaction &t = _xs_null);

    virtual std::list<std::string> ls(std::string path, xenstore_transaction &t = _xs_null);
    template <typename T>
    T read(std::string path, xenstore_transaction &t = _xs_null) { return boost::lexical_cast<T>(read(path, t)); }
    template <typename T>
    T read_or_default(std::string path, T deflt = T(), xenstore_transaction &t = _xs_null) {
        auto val = read(path, t);
        if (val.empty()) {
            return deflt;
        } else {
            return boost::lexical_cast<T>(val);
        }
    }

    template <typename T>
    void write(std::string path, T val, xenstore_transaction &t = _xs_null) { return write(path, std::to_string(val), t); }
};


#endif /* XENSTORE_HH_ */
