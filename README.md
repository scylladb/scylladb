Seastar
=======

Introduction
------------

SeaStar is an event-driven framework allowing you to write non-blocking,
asynchronous code in a relatively straightforward manner (once understood).
It is based on [futures](http://en.wikipedia.org/wiki/Futures_and_promises).

Futures and promises
--------------------

A *future* is a result of a computation that may not be available yet.
Examples include:

  * a data buffer that we are reading from the network
  * the expiration of a timer
  * the completion of a disk write
  * the result computation that requires the values from
    one or more other futures.

a *promise* is an object or function that provides you with a future,
with the expectation that it will fulfill the future.

Promises and futures simplify asynchronous programming since they decouple
the event producer (the promise) and the event consumer (whoever uses the
future).  Whether the promise is fulfilled before the future is consumed,
or vice versa, does not change the outcome of the code.

Consuming a future
------------------

You consume a future by using its *then()* method, providing it with a
callback (typically a lambda).  For example, consider the following
operation:

```
future<int> get();   // promises an int will be produced eventually
future<> put(int)    // promises to store an int

void f() {
    get().then([] (int value) {
        put(value + 1).then([] {
            std::cout << "value stored successfully\n";
        });
    });
}
'''

Here, we initate a *get()* operation, requesting that when it completes, a
*put()* operation will be scheduled with an incremented value.  We also
request that when the *put()* completes, some text will be printed out.

Chaining futures
----------------

If a *then()* lambda returns a future (call it x), then that *then()*
will return a future (call it y) that will receive the same value.  This
removes the need for nesting lambda blocks; for example the code above
could be rewritten as:

``` 
future<int> get();   // promises an int will be produced eventually
future<> put(int)    // promises to store an int

void f() {
    get().then([] (int value) {
        return put(value + 1);
    }).then([] {
        std::cout << "value stored successfully\n";
    });
}
'''

Loops
-----

Loops are achieved with a tail call; for example:

```
future<int> get();   // promises an int will be produced eventually
future<> put(int)    // promises to store an int

future<> loop_to(int end) {
    get().then([end] (int value) {
        if (value == end) {
            return make_read_future<>();
        }
        return put(value + 1);
    }).then([end] {
        return loop_to(end);
    });
}
'''
 
The *make_ready_future()* function returns a future that is already
available --- corresponding to the loop termination condition, where
no further I/O needs to take place.