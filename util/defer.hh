/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef UTIL_DEFER_HH_
#define UTIL_DEFER_HH_

template <typename Func>
class deferred_action {
    Func _func;
    bool _cancelled = false;
public:
    deferred_action(Func&& func) : _func(std::move(func)) {}
    ~deferred_action() { _func(); }
    void cancel() { _cancelled = true; }
};

template <typename Func>
inline
deferred_action<Func>
defer(Func&& func) {
    return deferred_action<Func>(std::forward<Func>(func));
}

#endif /* UTIL_DEFER_HH_ */
