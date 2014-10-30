/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef QUEUE_HH_
#define QUEUE_HH_

#include "circular_buffer.hh"
#include "future.hh"
#include <queue>
#include <experimental/optional>

template <typename T>
class queue {
    std::queue<T, circular_buffer<T>> _q;
    size_t _max;
    std::experimental::optional<promise<>> _not_empty;
    std::experimental::optional<promise<>> _not_full;
private:
    void notify_not_empty();
    void notify_not_full();
public:
    explicit queue(size_t size);

    // Push an item.
    //
    // Returns false if the queue was full and the item was not pushed.
    bool push(T&& a);

    // pops an item.
    T pop();

    // Consumes items from the queue, passing them to @func, until @func
    // returns false or the queue it empty
    //
    // Returns false if func returned false.
    template <typename Func>
    bool consume(Func&& func);

    // Returns true when the queue is empty.
    bool empty() const;

    // Returns true when the queue is full.
    bool full() const;

    // Returns a future<> that becomes available when pop() or consume()
    // can be called.
    future<> not_empty();

    // Returns a future<> that becomes available when push() can be called.
    future<> not_full();

    // Pops element now or when ther is some. Returns a future that becomes
    // available when some element is available.
    future<T> pop_eventually();

    // Pushes the element now or when there is room. Returns a future<> which
    // resolves when data was pushed.
    future<> push_eventually(T&& data);

    size_t size() const { return _q.size(); }
};

template <typename T>
inline
queue<T>::queue(size_t size)
    : _max(size) {
}

template <typename T>
inline
void queue<T>::notify_not_empty() {
    if (_not_empty) {
        _not_empty->set_value();
        _not_empty = std::experimental::optional<promise<>>();
    }
}

template <typename T>
inline
void queue<T>::notify_not_full() {
    if (_not_full) {
        _not_full->set_value();
        _not_full = std::experimental::optional<promise<>>();
    }
}

template <typename T>
inline
bool queue<T>::push(T&& data) {
    if (_q.size() < _max) {
        _q.push(std::move(data));
        notify_not_empty();
        return true;
    } else {
        return false;
    }
}

template <typename T>
inline
T queue<T>::pop() {
    if (_q.size() == _max) {
        notify_not_full();
    }
    T data = std::move(_q.front());
    _q.pop();
    return data;
}

template <typename T>
inline
future<T> queue<T>::pop_eventually() {
    if (empty()) {
        return not_empty().then([this] {
            return make_ready_future<T>(pop());
        });
    } else {
        return make_ready_future<T>(pop());
    }
}

template <typename T>
inline
future<> queue<T>::push_eventually(T&& data) {
    if (full()) {
        return not_full().then([this, data = std::move(data)] () mutable {
            _q.push(std::move(data));
            notify_not_empty();
        });
    } else {
        _q.push(std::move(data));
        notify_not_empty();
        return make_ready_future<>();
    }
}

template <typename T>
template <typename Func>
inline
bool queue<T>::consume(Func&& func) {
    if (_q.size() == _max) {
        notify_not_full();
    }
    bool running = true;
    while (!_q.empty() && running) {
        running = func(std::move(_q.front()));
        _q.pop();
    }
    return running;
}

template <typename T>
inline
bool queue<T>::empty() const {
    return _q.empty();
}

template <typename T>
inline
bool queue<T>::full() const {
    return _q.size() == _max;
}

template <typename T>
inline
future<> queue<T>::not_empty() {
    if (!empty()) {
        return make_ready_future<>();
    } else {
        _not_empty = promise<>();
        return _not_empty->get_future();
    }
}

template <typename T>
inline
future<> queue<T>::not_full() {
    if (!full()) {
        return make_ready_future<>();
    } else {
        _not_full = promise<>();
        return _not_full->get_future();
    }
}

#endif /* QUEUE_HH_ */
