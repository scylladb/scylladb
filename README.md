Seastar
=======

Introduction
------------

SeaStar is an event-driven framework allowing you to write non-blocking,
asynchronous code in a relatively straightforward manner (once understood).
It is based on [futures](http://en.wikipedia.org/wiki/Futures_and_promises).

Building Seastar
--------------------


### Building seastar on Fedora 21

Installing required packages:
```
yum install gcc-c++ libaio-devel ninja-build ragel hwloc-devel numactl-devel libpciaccess-devel cryptopp-devel
```

You then need to run the following to create the "build.ninja" file:
```
./configure.py
```
Note it is enough to run this once, and you don't need to repeat it before
every build. build.ninja includes a rule which will automatically re-run
./configure.py if it changes.

Then finally:
```
ninja-build
```

### Building seastar on Fedora 20

Installing GCC 4.9 for gnu++1y:
* Beware that this installation will replace your current GCC version.
```
yum install fedora-release-rawhide
yum --enablerepo rawhide update gcc-c++
yum --enablerepo rawhide install libubsan libasan
```

Installing required packages:
```
yum install libaio-devel ninja-build ragel hwloc-devel numactl-devel libpciaccess-devel cryptopp-devel
```

You then need to run the following to create the "build.ninja" file:
```
./configure.py
```
Note it is enough to run this once, and you don't need to repeat it before
every build. build.ninja includes a rule which will automatically re-run
./configure.py if it changes.

Then finally:
```
ninja-build
```

### Building seastar on Ubuntu 14.04

Installing required packages:
```
sudo apt-get install libaio-dev ninja-build ragel libhwloc-dev libnuma-dev libpciaccess-dev libcrypto++-dev libboost-all-dev
```

Installing GCC 4.9 for gnu++1y. Unlike the Fedora case above, this will
not harm the existing installation of GCC 4.8, and will install an
additional set of compilers, and additional commands named gcc-4.9,
g++-4.9, etc., that need to be used explicitly, while the "gcc", "g++",
etc., commands continue to point to the 4.8 versions.

```
# Install add-apt-repository
sudo apt-get install software-properties-common python-software-properties
# Use it to add Ubuntu's testing compiler repository
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt-get update
# Install gcc 4.9 and relatives
sudo apt-get install g++-4.9
# Also set up necessary header file links and stuff (?)
sudo apt-get install gcc-4.9-multilib g++-4.9-multilib
```

To compile Seastar explicitly using gcc 4.9, use:
```
./configure.py --compiler=g++-4.9
```

To compile OSv explicitly using gcc 4.9, use:
```
make CC=gcc-4.9 CXX=g++-4.9 -j 24
```

### Building seastar in Docker container

To build a Docker image:

```
docker build -t seastar-dev docker/dev
```

Create an shell function for building insider the container (bash syntax given):

```
$ seabuild() { docker run -v $HOME/seastar/:/seastar -u $(id -u):$(id -g) -w /seastar -t seastar-dev "$@"; }
```

(it is recommended to put this inside your .bashrc or similar)

To build inside a container:

```    
$ seabuild ./configure.py
$ seabuild ninja-build
```

### Building with a DPDK network backend

1. Setup host to compile DPDK:
   - Ubuntu 
```
sudo apt-get install -y build-essential linux-image-extra-`uname -r` 
```
2. Prepare a DPDK SDK:
  - Download the latest DPDK release: `wget http://dpdk.org/browse/dpdk/snapshot/dpdk-1.8.0.tar.gz`
  - Untar it.
  - Edit config/common_linuxapp: set CONFIG_RTE_MBUF_REFCNT  to 'n'.
  - For DPDK 1.7.x: edit config/common_linuxapp: 
    - Set CONFIG_RTE_LIBRTE_PMD_BOND  to 'n'.
    - Set CONFIG_RTE_MBUF_SCATTER_GATHER to 'n'.
    - Set CONFIG_RTE_LIBRTE_IP_FRAG to 'n'.
  - Start the tools/setup.sh script as root.
  - Compile a linuxapp target (option 9).
  - Install IGB_UIO module (option 11).
  - Bind some physical port to IGB_UIO (option 17).
  - Configure hugepage mappings (option 14/15).
3. Run a configure.py: `./configure.py --dpdk-target <Path to untared dpdk-1.8.0 above>/x86_64-native-linuxapp-gcc --compiler=g++-4.9`.
4. Run `ninja-build`.

To run with the DPDK backend for a native stack give the seastar application `--dpdk-pmd 1` parameter.

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

```C++
future<int> get();   // promises an int will be produced eventually
future<> put(int)    // promises to store an int

void f() {
    get().then([] (int value) {
        put(value + 1).then([] {
            std::cout << "value stored successfully\n";
        });
    });
}
```

Here, we initiate a *get()* operation, requesting that when it completes, a
*put()* operation will be scheduled with an incremented value.  We also
request that when the *put()* completes, some text will be printed out.

Chaining futures
----------------

If a *then()* lambda returns a future (call it x), then that *then()*
will return a future (call it y) that will receive the same value.  This
removes the need for nesting lambda blocks; for example the code above
could be rewritten as:

```C++
future<int> get();   // promises an int will be produced eventually
future<> put(int)    // promises to store an int

void f() {
    get().then([] (int value) {
        return put(value + 1);
    }).then([] {
        std::cout << "value stored successfully\n";
    });
}
```

Loops
-----

Loops are achieved with a tail call; for example:

```C++
future<int> get();   // promises an int will be produced eventually
future<> put(int)    // promises to store an int

future<> loop_to(int end) {
    if (value == end) {
        return make_ready_future<>();
    }
    get().then([end] (int value) {
        return put(value + 1);
    }).then([end] {
        return loop_to(end);
    });
}
```
 
The *make_ready_future()* function returns a future that is already
available --- corresponding to the loop termination condition, where
no further I/O needs to take place.

Under the hood
--------------

When the loop above runs, both *then* method calls execute immediately
--- but without executing the bodies.  What happens is the following:

1. `get()` is called, initiates the I/O operation, and allocates a
   temporary structure (call it `f1`).
2. The first `then()` call chains its body to `f1` and allocates
   another temporary structure, `f2`.
3. The second `then()` call chains its body to `f2`.

Again, all this runs immediately without waiting for anything.

After the I/O operation initiated by `get()` completes, it calls the
continuation stored in `f1`, calls it, and frees `f1`.  The continuation
calls `put()`, which initiates the I/O operation required to perform
the store, and allocates a temporary object `f12`, and chains some glue
code to it.

After the I/O operation initiated by `put()` completes, it calls the
continuation associated with `f12`, which simply tells it to call the
continuation associated with `f2`.  This continuation simply calls
`loop_to()`.  Both `f12` and `f2` are freed. `loop_to()` then calls
`get()`, which starts the process all over again, allocating new versions
of `f1` and `f2`.

Handling exceptions
-------------------

If a `.then()` clause throws an exception, the scheduler will catch it
and cancel any dependent `.then()` clauses.  If you want to trap the
exception, add a `.then_wrapped()` clause at the end:

```C++
future<buffer> receive();
request parse(buffer buf);
future<response> process(request req);
future<> send(response resp);

void f() {
    receive().then([] (buffer buf) {
        return process(parse(std::move(buf));
    }).then([] (response resp) {
        return send(std::move(resp));
    }).then([] {
        f();
    }).then_wrapped([] (auto&& f) {
        try {
            f.get();
        } catch (std::exception& e) {
            // your handler goes here
        }
    });
}
```

The previous future is passed as a parameter to the lambda, and its value can
be inspected with `f.get()`. When the `get()` variable is called as a
function, it will re-throw the exception that aborted processing, and you can
then apply any needed error handling.  It is essentially a transformation of

```C++
buffer receive();
request parse(buffer buf);
response process(request req);
void send(response resp);

void f() {
    try {
        while (true) {
            auto req = parse(receive());
            auto resp = process(std::move(req));
            send(std::move(resp));
        }
    } catch (std::exception& e) {
        // your handler goes here
    }
}
```

Note, however, that the `.then_wrapped()` clause will be scheduled both when
exception occurs or not. Therefore, the mere fact that `.then_wrapped()` is
executed does not mean that an exception was thrown. Only the execution of the
catch block can guarantee that.


This is shown below:

```C++

future<my_type> my_future();

void f() {
    receive().then_wrapped([] (future<my_type> f) {
        try {
            my_type x = f.get();
            return do_something(x);
        } catch (std::exception& e) {
            // your handler goes here
        }
    });
}
```
### Setup notes

SeaStar is a high performance framework and tuned to get the best 
performance by default. As such, we're tuned towards polling vs interrupt
driven. Our assumption is that applications written for SeaStar will be
busy handling 100,000 IOPS and beyond. Polling means that each of our
cores will consume 100% cpu even when no work is given to it. 


Recommended hardware configuration for SeaStar
----------------------------------------------

* CPUs - As much as you need. SeaStar is highly friendly for multi-core and NUMA
* NICs - As fast as possible, we recommend 10G or 40G cards. It's possible to use
       1G to but you may be limited by their capacity.
       In addition, the more hardware queue per cpu the better for SeaStar. 
       Otherwise we have to emulate that in software.
* Disks - Fast SSDs with high number of IOPS.
* Client machines - Usually a single client machine can't load our servers.
       Both memaslap (memcached) and WRK (httpd) cannot over load their matching
       server counter parts. We recommend running the client on different machine
       than the servers and use several of them.
