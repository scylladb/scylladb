/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "posix.hh"

void* posix_thread::start_routine(void* arg) {
    auto pfunc = reinterpret_cast<std::function<void ()>*>(arg);
    (*pfunc)();
    return nullptr;
}

posix_thread::posix_thread(std::function<void ()> func) : _func(std::make_unique<std::function<void ()>>(std::move(func))) {
    auto r = pthread_create(&_pthread, nullptr,
                &posix_thread::start_routine, _func.get());
    if (r) {
        throw std::system_error(r, std::system_category());
    }
}

posix_thread::posix_thread(posix_thread&& x)
    : _func(std::move(x._func)), _pthread(x._pthread), _valid(x._valid) {
    x._valid = false;
}

posix_thread::~posix_thread() {
    assert(!_valid);
}

void posix_thread::join() {
    assert(_valid);
    pthread_join(_pthread, NULL);
    _valid = false;
}




