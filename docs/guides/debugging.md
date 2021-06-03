# Debugging with GDB

## Introduction

GDB is a source level debugger for C, C++ and more languages. It allows
inspecting the internal state of a program as it is running as well the
post-mortem inspection of chrashed programs.

You can attach GDB to a running process, run a process inside GDB or
examine a coredump.

### Starting GDB

The two most common usages of GDB for scylla is running a process inside
it (e.g. a unit test):

    gdb /path/to/executable

You can specify command-line arguments that gdb will forward to the
executable:

    gdb /path/to/executable --args arg1 arg2 arg3

Another prevalent usage is to examine coredumps:

    gdb --core=/path/to/coredump /path/to/executable

You can also attach it to an already running process:

    gdb -p $pid

Where `$pid` is the PID of the running process you wish to attach GDB
to.

### Using GDB

GDB has excellent online documentation that you can find
[here](https://sourceware.org/gdb/onlinedocs/gdb/index.html).

Some of the more important topics:
* [Starting GDB](https://sourceware.org/gdb/onlinedocs/gdb/Invocation.html#Invocation)
* [Setting breakpoints](https://sourceware.org/gdb/onlinedocs/gdb/Set-Breaks.html#Set-Breaks)
* [Setting catchpoints](https://sourceware.org/gdb/onlinedocs/gdb/Set-Catchpoints.html#Set-Catchpoints)
* [Stepping through the code](https://sourceware.org/gdb/onlinedocs/gdb/Continuing-and-Stepping.html#Continuing-and-Stepping)
* [Examining the stack](https://sourceware.org/gdb/onlinedocs/gdb/Stack.html#Stack)
* [Examining data](https://sourceware.org/gdb/onlinedocs/gdb/Data.html#Data)

## Debugging Scylla with GDB

In general Scylla is quite hard to debug in GDB due to its asynchronous
nature. You will soon find that backtraces always lead to the reactor's
event loop and stepping through the code will not work as you expect as
soon as you leave or enter an asynchronous function.
That said GDB is an indispensable tool in debugging coredumps and when
used right can be of great help.

Over the years we have collected a set of tools for helping with debugging
scylla. These are collected in [``scylla-gdb.py``](https://github.com/scylladb/scylla/blob/master/scylla-gdb.py) and are in
the form of [commands](https://sourceware.org/gdb/onlinedocs/gdb/Commands.html#Commands),
[conveninence functions](https://sourceware.org/gdb/onlinedocs/gdb/Convenience-Funs.html#Convenience-Funs)
and [pretty printers](https://sourceware.org/gdb/onlinedocs/gdb/Pretty-Printing.html#Pretty-Printing).
To load the file issue the following command (inside gdb):

    (gdb) source /path/to/scylla-gdb.py

You should be now ready to use all of the tools contained therein. To
list all available commands do:

    (gdb) help scylla

To read the documentation of an individual command do:

    (gdb) help scylla $commandname

Some commands have self explanatory names, some have documentation, and
some have neither :( (contributions are welcome).

To get the list of the available convenience functions do:

    (gdb) help function

Note that this will list GDB internal functions as well as those added
by `scylla-gdb.py`.
Again, just like before, to see the documentation of an individual
function do:

    (gdb) help function $functionname

### Tips and tricks

#### Tell GDB to not stop on signals used by seastar

When running scylla (or any seastar application for that matter) inside
GDB it will get interrupted often due to catching some signals used by
seastar internally. This makes debugging almost impossible. To avoid
this, instruct GDB to not stop on these signals:

    (gdb) handle SIG34 SIG35 SIGUSR1 nostop noprint pass

#### Avoid (some) symbol parsing related crashes

GDB is known to crash when parsing some of scylla's symbols (especially
those related to futures). Usually telling it to not print static
members of classes and structs helps:

    (gdb) set print static-members no

#### Enable extended python diagnostics

When using the facilities from `scylla-gdb.py` it is very useful to know
the full stack of a failure in some of the provided tools, so that you
can fix it or report it. To enable this run:

    (gdb) set python print-stack full

#### Helping GDB find the source code for the executable

Often you find yourself debugging an executable, whose internal source
paths don't match those where they can be found on your machine. There
is an easy workaround for this:

    (gdb) set substitute-path /path/to/src/in/executable /path/to/src/on/your/machine

Note that the pattern that you supply to `set substitute-path` just has
to be a common prefix of the paths. Example: if the source location
inside the executable to some file is `/opt/src/scylla/database.hh` and
on your machine it is `/home/joe/work/scylla/database.hh`, you can make
GDB find the sources on your machine via:

    (gdb) set substitute-path /opt/src/scylla /home/joe/work/scylla

This method might not work if the sources do not have a prefix, e.g.
they are relative to the source tree root directory. In this case you can use the
`set directories` command to set the search path of sources for gdb:

    (gdb) set directories /path/to/scylla/source/tree

Multiple directories can be listed, separated with `:`.

#### .gdbinit

GDB supports writing arbitrary GDB commands in any file and sourcing it.
One can use this to place commands that one would have to issue every
time when debugging in a file, instead of typing them each time GDB is
started.
Conventionally this file is called `.gdbinit` and GDB in fact will look
for it in you current directory, in your $HOME directory and some other
places. You can always load it by hand if GDB refuses or fails to load it:

    (gdb) source /path/to/your/.gdbinit

#### TUI

GDB has a terminal based GUI called
[TUI](https://sourceware.org/gdb/onlinedocs/gdb/TUI.html#TUI).
This is extremely useful when you wish to see the source code while you
are debugging. The `TUI` mode can be activated by passing `-tui` to GDB
on the command line, or any time by executing the `tui enable` to
activate it and `tui disable` to deactivate it respectively.
By default the source window has the focus in TUI mode, meaning that command
completion, searching history and line editing doesn't work, e.g. if you use
the up and down keys, you will scroll the source file up and down respectively,
instead of moving in the command history. To focus the command window, issue
`focus cmd`. To move the focus to the source window again, issue `focus src`.

#### Thread Local Storage (TLS) variables

Thread local variables are saved in a special area of memory, at a negative
offset from `$fs_base`. Let's look at an example TLS variable, given the
following C++ code from seastar:

    namespace seastar::internal {

    inline
    scheduling_group*
    current_scheduling_group_ptr() noexcept {
        // Slow unless constructor is constexpr
        static thread_local scheduling_group sg;
        return &sg;
    }

    }

Let's have a look in GDB:

    (gdb) p &'seastar::internal::current_scheduling_group_ptr()::sg'
    $1 = (<thread local variable, no debug info> *) 0x7fc1f11e7c0c
    (gdb) p/x $fs_base
    $2 = 0x7fc1f11ff700
    (gdb) p/x 0x7fc1f11e7c0c - $fs_base
    $3 = 0xfffffffffffe850c
    (gdb) p/x -0xfffffffffffe850c
    $4 = 0x17af4

The variable `sg` is located at offset `0x17af4` beneath `$fs_base`. We
can also calculate the offset (and hence address) of a known TLS
variable in memory as follows:

    $fs_offset = $tls_entry - $sizeof_TLS_header

`$sizeof_TLS_header` can be obtained by listing the program headers of the binary:

    $ eu-readelf -l ./a.out
      Type           Offset    VirtAddr           PhysAddr           FileSiz  MemSiz   Flg Align
      [...]
      TLS            0x31ead40 0x00000000033ecd40 0x00000000033ecd40 0x000058 0x017bf0 R   0x40
      [...]

We are interested in the size of the TLS header, which is in the
`MemSiz` column and is `0x017bf0` in this example. The value of the
`$tls_entry` can be found in the process' symbol table:

    $ eu-readelf -s ./a.out

	Symbol table [ 5] '.dynsym' contains 1288 entries:
	 1 local symbol  String table: [ 9] '.dynstr'
	  Num:            Value   Size Type    Bind   Vis          Ndx Name
		[...]
	 1282: 000000000000010c      4 TLS     LOCAL  HIDDEN        23 _ZZN7seastar8internal28current_scheduling_group_ptrEvE2sg
		[...]

If we substitute these values in we can verify our theory:

    (gdb) set $tls_entry = 0x000000000000010c
    (gdb) set $sizeof_TLS_header = 0x017bf0
    (gdb) p/x $tls_entry - $sizeof_TLS_header
    $5 = 0xfffe851c
    (gdb) p/x -($tls_entry - $sizeof_TLS_header)
    $6 = 0x17ae4

We can also identify a TLS variable based on its address. We know the
value of `$sizeof_TLS_header` and we can easily calculate `$fs_offset`.
To identify the variable we need to calculate its `$tls_entry` based on
which we can find the matching entry in the symbol table. Remaining with
the above example of the address being `0x7fc1f11e7c0c`, we can
calculate this as:

    $tls_entry = $sizeof_TLS_header + $fs_offset

Do note however that `$fs_offset` is negative so this is in effect a
substituation:

    $tls_entry = 0x017bf0 - 0x17ae4

This yields `0x10c` which is exactly the value of the `Value` column in
the matching symbol table entry. This should work also if you don't have
the address to the start of the object. In this case you have to locate
the entry in the symbol table, whose value range includes the
calculated value. This can be made easier by sorting the symbol table by
the `Value` column.

#### Optimized-out variables

In release builds one will find that a significant portion of variables and
function parameters are optimized out. This is very annoying but often one can
find a way around to inspect the desired variables.

For non-local variables, there is a good chance that a few frames up one can
find another reference that wasn't optimized out. Or one can try to find another
object, which is not optimized out, and which is also known to hold a reference
to the variable.

If the variable is local, one can try to look at the registers (`info
registers`) and try to identify which one holds its value. This is relatively
easy for pointers to objects as heap pointers are easily identifiable (they
start with `0x6` and are composed of 12 digits, e.g.: `0x60f146e3fbc0`) and one
can check the size of the object they point to with `scylla ptr`. If the
pointed-to object is an instance of a class with virtual methods, the object can
be easily identified by looking at its vtable pointer (`x/1a $ptr`).

### Debugging coredumps

Up until release 3.0 we used to build and package Scylla separately for each
supported distribution. Starting with 3.1 we moved to relocatable binaries.
These are built with a common [frozen toolchain](https://github.com/scylladb/scylla/blob/master/tools/toolchain/README.md)
and packages are bundled with all dependencies. This means that post 3.1 there
is just one build across all supported distros and that the exact environment
the binaries were built with is available in the form of a Docker image. This
makes debugging cores generated from relocatable binaries much easier.
As of now, all releases except 2019.1 ship via relocatable packages, so in this
chapter we will focus on how to debug cores generated from relocatable binaries,
with a subsection later explaining how to debug cores generated by 2019.1
binaries.

#### Relocatable binaries

Cores produced by relocatable binaries can be simply opened in the
[dbuild](https://github.com/scylladb/scylla/blob/master/tools/toolchain/README.md) container they were built with. To do
that, two things (apart from the core itself of course) are needed:
1) The exact frozen toolchain (dbuild container).
2) The exact relocatable package the binary was part of.

##### Obtaining the frozen toolchain

The frozen toolchain is obtained based on the commit id of the version of the
scylla executable the core was produced with. The exact commit hash can be
obtained by running:
```
$ scylla --version
666.development-0.20200630.28c3d4f8e
````
The version can be divided into 4 parts:
* The version identifier, in this case: 666; in case of a release this
  will be something like 4.2.
* The build identifier, in this case: development-0.
* The date, in this case: 20200630.
* The commit hash, in this case: 28c3d4f8e.

Based on the latter, you can obtain the right frozen toolchain:

    $ cd /path/to/scylla
    $ git checkout $commit_hash

##### Obtaining the relocatable-package

Once we have the right toolchain, we have to obtain the relocatable package.
This is obtained based on the build-id, which can be obtained from the coredump
like this:

    $ eu-unstrip -n --core $corefile

Or from the executable like this:

    $ eu-unstrip -n --exec $executable

With the build-id you can find the relocatable using the
[scylla-pkg.git/scripts/scylla-s3-reloc (private repo)](https://github.com/scylladb/scylla-pkg/#looking-for-a-build)
script.

##### Loading the core

Move the coredump and the unpackaged relocatable package into some dir
`$dir` on your system, then:

```
(host)$ cd /path/to/scylla # with the right commit checked out
(host)$ ./tools/toolchain/dbuild -it -v $dir:/workdir:z -- bash -l
(dbuild)$ cd /workdir
(dbuild)$ cd unpackaged-relocatable-package && ./install.sh && cd ..
(dbuild)$ gdb --core=$corefile /opt/scylladb/libexec/scylla
```

You might need to add

    -ex 'set auto-load safe-path /opt/scylladb/libreloc'

to the command line, see [No thread debugging](#no-thread-debugging).

#### Non-relocatable binaries (2019.1)

The first step in opening any core is (after obtaining the core itself)
obtaining the *exact* executable and library versions it was produced with. This
is very important, it is not enough to get a binary with the same version, or to
build one with the same commit, the binaries and libraries *have to be the very
same ones the core was produced with*. This is because neither distros nor use
use [reproducible builds](https://en.wikipedia.org/wiki/Reproducible_builds),
which means that each build will produce a slightly different binary, which
might be enough to make debugging hard or downright impossible.
A foolproof method to match cores to their matching executables and libraries
is the *build-id*. This is automatically assigned to each binary when it is
built and it uniquely identifies it.

##### Collecting libraries

As stated above, just installing Scylla on another machine with the same OS
version is not enough to obtain the correct environment to load the core.
Packages might have newer versions, or the customer might be using different
packages. It is important that we obtain the exact packages installed on the
system where the core was produced on.

A method that should work regardless of the used distro is zipping all libs:
```
eu-unstrip -n --core corefile | awk '{print $3}' | grep -v '[-.]$' | zip scylla-libs -@
```
This might however produce a rather large zip or zip might not be available on
the node and it can't be installed (yes really). So below we are going to
look at methods that doesn't involve copying a huge zip from the node, nor
installing any new software.

##### CentOS/Redhat

CentOS and Redhat has means to obtain the binaries given a certain build-id.
TODO: document how that works.

##### Ubuntu/Debian

Run the following script to collect the version of each library used by Scylla:
```sh
function file_uri() {
    dpkg -S $1 | cut -d" " -f1 | cut -d: -f1,2 | xargs apt-get download --print-uris
}

for lib in $(ldd `which scylla` | cut -d'(' -f1 | awk '{print $3}'); do
    if [ -n "$lib" ]; then
        echo "${lib}: $(file_uri $lib)"
    fi
done
```

This will produce an output like:
```
/opt/scylladb/lib/libstdc++.so.6: 'http://some.mirror.example/mirrors/scylla/versioned/2019100201/ubuntu/pool/main/s/scylla-gcc73/scylla-gcc73-libstdc++6_7.3.0-3ubuntu2~xenialppa1_amd64.deb' scylla-gcc73-libstdc++6_7.3.0-3ubuntu2~xenialppa1_amd64.deb 368128 SHA256:7aa085e85c2a6bbd5b1517985e84cf280b74839d7ccb313f823f201ad162fccc
/usr/lib/libcrypto++.so.9: 'http://some.mirror.example/mirrors/debian/versioned/2018060600/ubuntu/pool/universe/libc/libcrypto++/libcrypto++9v5_5.6.1-9_amd64.deb' libcrypto++9v5_5.6.1-9_amd64.deb 885184 SHA256:888ce5da554200dac297d97b376d27607515689a79df944295e0e43cd0d94d31
/lib/x86_64-linux-gnu/librt.so.1: 'http://some.mirror.example/mirrors/debian/versioned/2018060600/ubuntu/pool/main/g/glibc/libc6_2.23-0ubuntu10_amd64.deb' libc6_2.23-0ubuntu10_amd64.deb 2580230 SHA256:bd05c3487325a4386dee6abb02ad904e1f2d8d3d0adc0df8e8f29168fbe2b5bb
```

Usually the URLs are accessible and the packages can be simply downloaded. In
some case however they are on some internal mirror that is only accessible from
within the internal network of the cluster. In this case one has to obtain the
exact name and version of the packages then download it themselves with
`apt-get`. Given the file with the output of the above script containing the
package versions (`packages.txt` from now on), you can use the below script to
parse the package name and version from it and download the appropriate `.deb`
package files:

```sh
while read l
do
    deb=$(echo $l | cut -f3 -d' ')
    pkg_name=$(echo $deb | cut -f1 -d_)
    pkg_version=$(echo $deb | cut -f2 -d_)
    apt-get download ${pkg_name}=${pkg_version}
done < /path/to/packages.txt
```

Note that in some cases the names of packages or their versions in
`packages.txt` will contain [encoded
characters](https://en.wikipedia.org/wiki/Percent-encoding), like `%3a`. This
will make `apt-get` fail to download the package. Be sure to decode any of these
before attempting the run the downloading script.
Also in some cases the exact version will not be available anymore. In this case
try to locate the closest version to the desired one and download that one. You
can use `apt-cache` to query available versions:

    apt-cache show package-name | grep Version:

##### Opening the core on the same OS

The packages obtained can be installed on the system, and the core can be simply
opened with GDB, which should find the libraries without issues. One thing to
note is that usually these distros will have older GDB:s available, which might
have problems handling Scylla symbols. For this reason we also build and package
GDB for all supported distros. This can be installed via the `scylla-gdb`
package, after adding the Scylla repositories. Run GDB via
`/opt/scylladb/bin/gdb`.

##### Opening the core on another OS

If you don't feel like struggling to get recent enough tools on a potentially
very old distro, a viable alternative is to just debug on your development box.
The first step is unpacking (not installing!) all the packages. This can be
easily done with a docker image, e.g. for ubuntu:
```sh
$ docker run -it --privileged -v .:/workspace ubuntu:16.04 bash -l
(docker) $ for pkg in $(ls *.deb); do dpkg -x $pkg .; done
```
After running the above snippet the current directory will contain a linux `/`
directory structure. To load the core in GDB:
```
$ gdb -q
(gdb) set sysroot .
(gdb) set solib-search-path ./lib64:./lib/x86_64-linux-gnu
(gdb) core /path/to/core
(gdb) file ./usr/bin/scylla
(gdb) set solib-search-path ./lib64:./lib/x86_64-linux-gnu
```
Note that although relative paths should work for `set sysroot` and `set
solib-search-path`, sometimes I could only get this working with absolute path.
If opening the core wasn't successful (thread debugging doesn't work) try using
absolute paths. Also note that issuing the `set solib-search-path` command has
to be done once before and once after the `file` command. Don't ask me why.
Another thing to keep in mind is that `set solib-search-path` has to contain all
directories that contain libraries. In the case of ubuntu:16.04 this is `/lib64`
and `/lib/x86_64-linux-gnu`. Other distros could have less or more such paths.
Multiple paths can be added separated with `:`.

You can check that GDB correctly loaded the libraries by using `info
sharedlibrary` and ensuring all libraries are loaded from the directory where
the packages were extracted to, and not from the host.

### Troubleshooting

#### Namespace issues

GDB complaints that it can't find `namespace seastar` or some other Scylla
or Seastar symbol that you know exists. This usually happens when GDB is in
the wrong context i.e. a frame is selected which is not in the Scylla executable
but in some other library. A typical situation is opening a coredump and
attempting to access Scylla symbols when the initial frame is in libc.
Move up the stack, or select a frame which is a Scylla or Seastar function to
fix.

#### No thread debugging

Unable to access thread-local variables. Example:

    (gdb) p seastar::local_engine
    Cannot find thread-local storage for LWP 22604, executable file /usr/lib/debug/usr/bin/scylla.debug:
    Cannot find thread-local variables on this target

The first step in finding out why thread debugging doesn't work is enabling
additional information about why thread debugging is not working:

    (gdb) set debug libthread-db 1

This has to be done right after starting GDB, *before* the core and the
executable are loaded. You can do this by adding
`-iex "set debug libthread-db 1"` to your gdb command line.

The usual cause is that GDB failed to find some libraries or that the library
versions of those libraries GDB loaded don't match those the core was generated
with.

Of special note is the `libthread_db.so` library, which is crucial for
thread debugging to work. Common causes of failing to find or load this library
are discussed below.

##### Loading denied by auto-load safe-path

You might see a message like this:

    warning: File "/opt/scylladb/libreloc/libthread_db.so.1" auto-loading has been declined by your `auto-load safe-path' set to "$debugdir:$datadir/auto-load"
    thread_db_load_search returning 0

To declare the directory this library is found at as safe to load from, do:

    set auto-load safe-path /opt/scylladb/libreloc

Use the path that is appropriate for your setup. Alternatively you can use `/`
as the path to declare your entire file-system as safe to load stuff from.
Note that `libthread_db.so` is packaged together with `libc`. So if you have the
build-id appropriate `libc` package, you can be sure you have the correct
`libthread_db.so` too.

##### Missing debug symbols for glibc

If you see a message like this:

    warning: Expected absolute pathname for libpthread in the inferior, but got .gnu_debugdata for /lib64/libpthread.so.0.
    Trying host libthread_db library: /lib64/libthread_db.so.1.
    td_ta_new failed: application not linked with libthread
    thread_db_load_search returning 0

Installing debug symbols for glibc might solve this. This issue was seen on
Fedora 34, for more details see the
[bug report](https://bugzilla.redhat.com/show_bug.cgi?id=1960867).
Debug symbols can be installed with:

    sudo dnf debuginfo-install glibc-2.32-6.fc33.x86_64

Adjust for the exact version you have installed.

##### Missing critical shared libraries

If you ensured `libthread_db.so` is present and is successfully loaded by GDB
but thread debugging still doesn't work, inspect the other libraries loaded by
GDB:

    (gdb) info sharedlibrary

The listing will contain the path of the loaded libraries. If a library wasn't
found by GDB that will also be visible in the listing. You can then use the
`file` utility to obtain the build-id of the libraries:

    file /path/to/libsomething.so

This build-id must match the one obtained from the core. The library build-ids
from the core can be obtained with:

    eu-unstrip -n --core=/path/to/core

In general you can get away some non-core libraries missing or having the wrong
version, but the core libraries like `libc.so`, `libgcc_s.so`, `librt.so` and
`ld.so` (often called something like `ld-linux-x86-64.so.2`) etc. must have the
correct version. Best to ensure all libraries are correct to minimize the chance
of something not working. Also, make sure the build-id of the executable matches
that the core was generated with. Again, you can use `file` to obtain the
build-id of the executable, then compare it with the build-id obtained from the
`eu-unstrip` listing.
For more information on how to obtain the correct version of libraries and how
to override the path GDB loads them from, see [Collecting libraries](#collecting-libraries)
and [Opening the core on another OS](#opening-the-core-on-another-os).

#### GDB crashes when priting the backtrace or some variable

See [Avoid (some) symbol parsing related crashes](#avoid-some-symbol-parsing-related-crashes).

GDB has trouble with frames inlined into the outermost frame in a seastar thread,
or any green threads in general -- where the outermost frame is annotated with
`.cfi_undefined rip`. See
[GDB#26387](https://sourceware.org/bugzilla/show_bug.cgi?id=26387).
To work around this, pass a limit to `bt`, such that it excludes the problematic
frame. E.g. if `bt` prints 10 frames before GDB crashing, use `bt 9` to avoid the
crash.

#### GDB keeps stopping on some signals

See [Tell GDB to not stop on signals used by seastar](#tell-gdb-to-not-stop-on-signals-used-by-seastar).

### Debugging guides

Guides focusing on different aspects of debugging Scylla. These guides
assume a release build of Scylla.

#### The seastar memory allocator

Seastar has its own memory allocator optimized for Seastar's
thread-per-core architecture. Memory, just like CPU, is sharded among
threads, meaning that each shard has its equally-sized exclusive memory area.

The seastar allocator operates on three levels:
* pages (4KB)
* spans of pages (2^N pages, N ∈  [0, 31])
* objects managed by small pools

Small allocations (<= 16KB) are served by small memory pools. There is
exactly one pool per supported size, and there is a limited number of
sizes available between 1B (8B in practice) and 16KB. Allocations are served
by the pool with the closest, but larger than equal size to the requested
allocation, but alignments complicate this. Pools allocate spans
themselves for the space to allocate objects in.

Large allocations are served by allocating an entire span.

The allocator keeps a description of all the pages and pools in memory
in thread-local variables. Using these, it is possible to arrive at the
metadata describing any allocation with a few steps:

    address -> page -> span -> pool?

This is exploited by the `scylla ptr` command which, given an address,
prints the following information:
* The allocation (small or large) this address is part of.
* The offset of the address from the beginning of the allocation.
* Is the object dead or live.

Example:

    (gdb) scylla ptr 0x6000000f3830
    thread 1, small (size <= 512), live (0x6000000f3800 +48)

It is possible to dump the state of the seastar allocator with the
`scylla memory` command. This prints a report containing the state of
all small pools as well as the availability of spans. It also prints
other Scylla specific information.

#### Continuations chains

Continuation chains are everywhere in Scylla. Every execution flow takes the
form of a continuation chain. This makes debugging very hard because the normal
GDB commands for inspecting and controlling execution flow (`backtrace`, `up`,
`down`, `step`, `next`, `return`, etc.) quickly fail to fulfill their purpose.
One will quickly find that every backtrace just leads to the same event loop in
the seastar reactor.

Continuation chains are formed by tasks. The tasks form an intrusive forward
linked list in memory. Each tasks links to the task that depends on it. Example:

    future<> bar() {
        return sleep(std::chrono::seconds(10)).then([] {
        });
    }

    future<> foo() {
        return bar().then([] {
        });
    }

When foo() is called a continuation chain of 3 tasks will be created:
* T0: sleep()
* T1: bar()::lambda#1
* T2: foo()::lambda#1

T1 depends on T0, and T2 depends on T1. In memory they form a forward linked
list like:

    T0 -> T1 -> T2

The links are provided by the promise-future pairs in these tasks. Each task
contains a future half of one such pair and a promise half of another one. The
future is for the value arriving from the previous task and the promise is for
the value calculated in the local task, that the next task waits on.

The `task` object have the following interface:

    class task {
        scheduling_group _sg;
    public:
        virtual void run_and_dispose() noexcept = 0;
        virtual task* waiting_task() noexcept = 0;
        scheduling_group group() const { return _sg; }
        shared_backtrace get_backtrace() const;
    };

The only thing stored at a known offset is the scheduling group. Each task
object also has an associated action that is executed when the task runs
(`run_and_dispose()`), as well as pointer to the next task (returned by
`waiting_task()`). However task being a polymorphic object the layout of the
different kind of tasks is not known, so all we can say about them is that
somewhere they contain a promise object, which has a pointer to the future object
of the task that depends on them. Also note that continuations are just one kind
of task, there are other kinds of tasks as well. Many seastar primites,
like `do_with()`, `repeat()`, `do_until()`, etc. have their own task
types.

##### Traversing the continuation chain forward

Or in other words finding out what are the continuations waiting on this one.
This involves searching for all outbound references in the task and identifying
the one which is also a task. As this is quite a labour-intensive task, there is
a command in scylla-gdb.py which automates it, called `scylla fiber`. Example
usage:

    (gdb) scylla fiber 0x60001a305910
    Starting task: (task*) 0x000060001a305910 0x0000000004aa5260 vtable for seastar::continuation<...> + 16
    #0  (task*) 0x0000600016217c80 0x0000000004aa5288 vtable for seastar::continuation<...> + 16
    #1  (task*) 0x000060000ac42940 0x0000000004aa2aa0 vtable for seastar::continuation<...> + 16
    #2  (task*) 0x0000600023f59a50 0x0000000004ac1b30 vtable for seastar::continuation<...> + 16

This is somewhat similar to a backtrace, in that it shows tasks that are waiting
for this continuation to finish, similar to how upstream functions are waiting
for the called function to finish before continuing their own execution.
See `help scylla fiber` and `scylla fiber --help` for more information on usage.

##### Traversing the continuation chain backward

Or in other words find the continuations the current continuation is waiting on.
This involves searching for all references to the task and identifying the one
which is also a task. This is made quite easy with the `scylla find` command
from `scylla-gdb.py`. By using the `--resolve` flag, the vtable symbol will be
printed next to each inbound reference, making spotting the other task easy.
Once found, repeat the same with the pointer of the other task, until a
non-continuation future is found, e.g. a I/O, `smp::submit()`, etc.

##### Seastar threads

Seastar threads are a special kind of continuation. Each seastar thread hosts a
stack but it can also be linked into a continuation chain. The stack of seastar
threads is a regular stack and all the normal stack related GDB commands can be
used in it. This can be used to inspect where exactly the seastar thread
stopped when it was suspended to wait on some future. Local variables can be
inspected too. The catch is how to make GDB context switch into the
stack of the seastar stack. Unfortunately there is no method that works
with GDB as of now, the `scylla thread` command crashes GDB and even if it
didn't, it'd only works in live processes.
To get this working a patched GDB is needed, see
https://github.com/denesb/seastar-gdb for instructions on how to use.

#### Debugging assert failures

Assert failures are the easiest (easiest but not easy) coredumps to debug
because we know the condition that failed, we know where and thus the
investigation has a clear scope -- to find out why. This is not always easy
though, especially if the root cause happened much earlier, and thus the state
the node was in at that time is not observable in the coredump. The root cause
might even be on another node altogether. In this case we try to gather as much
information as possible and write debug patches that hopefully catch the problem
earlier, and try to reproduce with them, hoping to get a new coredump that has
more information.

#### Debugging segmentation faults

Segmentation faults are usually caused by use-after-free,
use-after-move, dangling pointer/reference or memory corruption.
Unfortunately, coredumps often contain very little immediate information on what
exactly was wrong. It is rare to find something as obvious as a null pointer
trying to be dereferenced. So one has to dig a little to find out what
exactly triggered the SEGFAULT.
The most useful command in this is `scylla ptr`, as it allows
determining whether the address the current function is working with
belongs to a live object or not.

Once the immediate cause is found, only the "easy" part remains, finding
out how it happened.
In some cases this can be very difficult. For example in the case of a memory
corruption overwriting memory belonging to another object, the overwrite
could have happened much earlier, with no traces of what it was in the
coredump. In this case the same method has to be used that was mentioned
in the case of [debugging assert failures](#debugging-assert-failures):
adding additional debug code and trying to reproduce, hoping to catch
the perpetrator red-handed.

#### Debugging deadlocks

If the process that is stuck is known, start from there. Try to identify the
continuation-chain that is stuck, then
[follow it](#traversing-the-continuation-chain-backward)
to find the future that is blocking it all.
There is no way to differentiate a stuck continuation chain from one
that is making progress unfortunately, so there are not tried-and-proven
methods here either.

#### Debugging Out Of Memory (OOM) crashes

OOM crashes are usually the hardest to debug issues. Not only one has to
determine the immediate cause which is often already hard enough, as
usual one also has to determine what lead to this state, how did it happen.

That said, finding the immediate cause has a pretty standard procedure.
The first step is always issuing a `scylla memory` command and
determining where the memory is. Lets look at a concrete example:

    (gdb) scylla memory
    Used memory:    7452069888
    Free memory:      20082688
    Total memory:   7472152576

    LSA:
      allocated:    1067712512
      used:         1065353216
      free:            2359296

    Cache:
      total:            393216
      used:             160704
      free:             232512

    Memtables:
     total:          1067319296
     Regular:
      real dirty:    1064566784
      virt dirty:     811568656
     System:
      real dirty:        393216
      virt dirty:        393216
     Streaming:
      real dirty:             0
      virt dirty:             0

    Coordinator:
      bg write bytes:         42133 B
      hints:                      0 B
      view hints:                 0 B
      00 "main"
        fg writes:              0
        bg writes:              0
        fg reads:               0
        bg reads:              -7
      05 "statement"
        fg writes:             14
        bg writes:              5
        fg reads:              94
        bg reads:            2352

    Replica:
      Read Concurrency Semaphores:
        user sstable reads:       84/100, remaining mem:     138033377 B, queued: 0
        streaming sstable reads:   0/ 10, remaining mem:     149443051 B, queued: 0
        system sstable reads:      0/ 10, remaining mem:     149443051 B, queued: 0
      Execution Stages:
        data query stage:
             Total                            0
        mutation query stage:
             Total                            0
        apply stage:
          02 "streaming"                      287
             Total                            287
      Tables - Ongoing Operations:
        pending writes phaser (top 10):
                 12 cqlstress_lwt_example.blogposts
                  2 system.paxos
                 14 Total (all)
        pending reads phaser (top 10):
               1863 system.paxos
                809 cqlstress_lwt_example.blogposts
               2672 Total (all)
        pending streams phaser (top 10):
                  0 Total (all)

    Small pools:
    objsz spansz    usedobj       memory       unused  wst%
        1   4096          0            0            0   0.0
        1   4096          0            0            0   0.0
        1   4096          0            0            0   0.0
        1   4096          0            0            0   0.0
        2   4096          0            0            0   0.0
        2   4096          0            0            0   0.0
        3   4096          0            0            0   0.0
        3   4096          0            0            0   0.0
        4   4096          0            0            0   0.0
        5   4096          0            0            0   0.0
        6   4096          0            0            0   0.0
        7   4096          0            0            0   0.0
        8   4096      15285       126976         4696   3.7
       10   4096          0         8192         8192  99.9
       12   4096        173         8192         6116  74.6
       14   4096          0         8192         8192  99.8
       16   4096      11151       184320         5904   1.0
       20   4096       3570        77824         6424   7.9
       24   4096      19131       462848         3704   0.4
       28   4096       2572        77824         5808   7.3
       32   4096      27021       868352         3680   0.4
       40   4096      14680       593920         6720   0.7
       48   4096       3318       163840         4576   2.4
       56   4096      12077       692224        15912   0.9
       64   4096      52719      3375104         1088   0.0
       80   4096      16382      1323008        12448   0.6
       96   4096      17045      1667072        30752   0.3
      112   4096       3402       397312        16288   2.5
      128   4096      17767      2281472         7296   0.3
      160   4096      17722      2912256        76736   0.3
      192   4096       8094      1585152        31104   0.4
      224   4096      17087      3891200        63712   0.1
      256   4096      77945     21274624      1320704   0.1
      320   8192      13232      4366336       132096   0.7
      384   8192       5571      2203648        64384   1.0
      448   4096       4290      1986560        64640   1.7
      512   4096       2830      1503232        54272   2.8
      640  12288        960       655360        40960   3.9
      768  12288       5751      4489216        72448   0.1
      896   8192        326       311296        19200   4.6
     1024   4096       4320      5677056      1253376   0.7
     1280  20480        251       425984       104704  22.2
     1536  12288       3818      6373376       508928   1.7
     1792  16384       2711      4980736       122624   0.9
     2048   8192        594      1343488       126976   9.5
     2560  20480        122       458752       146432  25.7
     3072  12288       6596     21823488      1560576   0.9
     3584  28672          6       294912       273408  91.1
     4096  16384       2039      8372224        20480   0.2
     5120  20480       7885     43220992      2849792   0.3
     6144  24576       8188     54099968      3792896   0.8
     7168  28672         30       622592       407552  53.0
     8192  32768       8091     66781184       499712   0.7
    10240  40960      15058    165216256     11022336   0.4
    12288  49152       7034     92471296      6037504   0.3
    14336  57344       6815    111935488     14235648   0.2
    16384  65536      14046    230555648       425984   0.2
    Small allocations: 872148992 [B]
    Page spans:
    index      size [B]      free [B]     large [B] [spans]
        0          4096       1888256             0       0
        1          8192        663552             0       0
        2         16384             0             0       0
        3         32768         32768    2320334848   70811
        4         65536         65536    3031105536   46251
        5        131072        131072    1161822208    8864
        6        262144       3145728             0       0
        7        524288        524288             0       0
        8       1048576       1048576             0       0
        9       2097152             0       2097152       1
       10       4194304      12582912             0       0
       11       8388608             0             0       0
       12      16777216             0             0       0
       13      33554432             0             0       0
       14      67108864             0      67108864       1
       15     134217728             0             0       0
       16     268435456             0             0       0
       17     536870912             0             0       0
       18    1073741824             0             0       0
       19    2147483648             0             0       0
       20    4294967296             0             0       0
       21    8589934592             0             0       0
       22   17179869184             0             0       0
       23   34359738368             0             0       0
       24   68719476736             0             0       0
       25  137438953472             0             0       0
       26  274877906944             0             0       0
       27  549755813888             0             0       0
       28 1099511627776             0             0       0
       29 2199023255552             0             0       0
       30 4398046511104             0             0       0
       31 8796093022208             0             0       0
    Large allocations: 6582468608 [B]

We can see a couple of things at glance here: free memory is very low,
cache is fully evicted. These are sure signs of a real OOM. Note that
free memory doesn't have to be 0 in the case of an OOM. It is enough for
a size pool to not be able to allocate more memory spans and thus fail
a critical allocations we cannot recover from. Also cache is fully
evicted doesn't mean it has 0 memory, but when it has just a couple of
KB, it is considered fully evicted. Cache is evicted by the seastar memory
allocator's memory reclamation mechanism, which is hooked up with the
cache and will start trying to free up memory by evicting the cache,
once memory runs low.
The cause of the OOM in this case is too many reads (1863) on
`system.paxos`. This can be seen in the replica section of the report.
The Coordinator and replica sections contain high level stats of the
state of the coordinator and replica respectively. These stats summarize
the usual suspects. Sometimes just looking at these is enough to
determine what is the cause of the OOM. If not, one has to look at the
last section: the dump of the state of the small pools and the page
spans. What we are looking for is a small pool or a span size that owns
excessive amounts of memory. Once found (there can be more then one) the
next task is to identify what the objects owning that memory are. Note
that in the case of smaller allocations, the memory is usually occupied
directly by some C++ object, why in the case of larger allocations, these are
usually potentially fragmented buffers, owned by some other object.

If the `scylla memory` output alone is not enough to explain what
exactly is eating up all the memory, there are some further usual
suspects that should be examined.

##### Exploded task- and smp-queues and lots of objects

Look for exploded task- and smp-queues with:

    scylla task-queues        # lists all task queues on the local shard
    scylla smp-queues         # lists smp queues

Look for lots of objects with:

    scylla task_histogram -a  # histogram of all objects with a vtable

A huge number in any of these reports can indicate problems of exploding
concurrency, or a shard not being to keep up. This can easily lead to work
accumulating, in the form of tasks and associated objects, to the point of OOM.

##### Expensive reads

Another usual suspect is the number of sstables. This can be queried via
`scylla sstables`. A large number of sstables for a single table (in the
hundreds or more) can cause an otherwise non-problematic amount of reads to use
excessive amount of memory, potentially leading to OOM.

Reversed- and unpaged-reads (or both, combined) can also consume a huge amount
of memory, to the point of a fiew of such reads causing OOM. The way to find
these is to inspect readers in memory, trying to locate their partition slice
and having a look at their respective options:
* `partition_slice::option::reversed` is set for a reversed query
* `partition_slice::option::allow_short_read` is cleared for an unpaged
  query

Note that scylla have protections against reverse queries since 4.0, and against
unpaged queries since 4.3.

#####  Other reasons

If none of the usual suspects are present then all bets are off and one has to
try to identify who the objects of the exploded size-class or span-size belong
to. Unfortunately there are no proven methods here: some try to inspect
the memory patterns and try to make sense of it, some try to build an
object graph out of these objects and make sense of that. For the
latter, the following commands might be of help:

    scylla small-objects         # lists objects from a small pool
    scylla generate-object-graph # generates and visualizes an object graph

Good luck, you are off the charted path here.
