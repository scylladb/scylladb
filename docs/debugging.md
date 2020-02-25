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
scylla. These are collected in [scylla-gdb.py](../scylla-gdb.py) and are in
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

### Debugging coredumps

Up until release 3.0 we used to build and package Scylla separately for each
supported distribution. Starting with 3.1 we moved to relocatable binaries.
These are built with a common [frozen toolchain](../tools/toolchain/README.md)
and packages are bundled with all dependencies. This means that post 3.1 there
is just one build across all supported distros and that the exact environment
the binaries were built with is available in the form of a Docker image. This
makes debugging cores generated from relocatable binaries much easier, although
not without challenges. For this reason this chapter is split in two halves, the
first dealing with the old per-distro build and package system (that will be
here with us for many years still) and the second half dealing with relocatable
binaries.

#### Non-relocatable binaries (pre 3.1)

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

#### Relocatable binaries

Cores produced by relocatable binaries can be simply opened in the
[dbuild](../tools/toolchain/README.md) container they were built with. Simply
obtain the exact version or commit hash of the binary the core was produced
with, checkout said commit in the git repo, then start the `dbuild` container.
Note that you still need to obtain the exact executable the core was built with,
but all the libraries will be available inside the container.

As of 698b72b5018868df6a839d08fd24c642db97ffcd relocatable binaries installed on
any system will have their interpreter (`ld.so`) patched (with `patchelf`) to
point to the appropriate relocated interpreter, copied from the frozen
toolchain. As a consequence of this, when loading an executable and/or core file
with GDB inside the `dbuild` container, GDB will try to look up the
interpreter at the path it was found on the system it was installed on. As this
path is not the standard interpreter path, GDB will fail to load it. A
consequence of this is that thread debugging will not work. Luckily this can be
overcome with a simple workaround (inside the `dbuild` container):

```bash
# Find out the executable's interpreter path
$ patchelf --print-interpreter /path/to/scylla
/opt/scylladb/libreloc/ld.so

# Make sure the interpreter in dbuild is also accessible via this path
$ mkdir -p /opt/scylladb/libreloc
$ ln -s /lib64/ld-linux-x86-64.so.2 /opt/scylladb/libreloc/ld.so

# Start gdb, adding `/lib64` to the `solib-search-path`
$ gdb --core=/path/to/scylla.core /path/to/scylla -ex 'set solib-search-path /lib64'
```

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

The usual cause is that GDB failed to find some libraries or that the library
versions of those libraries GDB loaded don't match those the core was generated
with. To see which libraries GDB found do:

    (gdb) info sharedlibrary

The listing will contain the path of the loaded libraries. If a library wasn't
found by GDB that will also be visible in the listing. You can then use the
`file` utility to obtain the build-id of the libraries:

    file /path/to/libsomething.so

This build-id must match the one obtained from the core. The library build-ids
from the core can be obtained with:

    eu-unstrip -n --core=/path/to/core

In general you can get away some non-core libraries missing or having the wrong
version, but the core libraries `libc`, `libgcc`, etc. must have the correct
version. Best to ensure all libraries are correct to minimize the chance of
something not working. Also, make sure the build-id of the executable matches
that the core was generated with. Again, you can use `file` to obtain the
build-id of the executable, then compare it with the build-id obtained from the
`eu-unstrip` listing.
For more information on how to obtain the correct version of libraries and how
to override the path GDB loads them from, see [Collecting libraries](#collecting-libraries)
and [Opening the core on another OS](#opening-the-core-on-another-os).

It is also possible to make GDB print additional information about why thread
debugging is not working. To enable execute:

    (gdb) set debug libthread 1

Right after starting GDB, *before* the core and the executable are loaded.

#### GDB crashes when priting the backtrace or some variable

See [Avoid (some) symbol parsing related crashes](#avoid-some-symbol-parsing-related-crashes).

#### GDB keeps stopping on some signals

See [Tell GDB to not stop on signals used by seastar](#tell-gdb-to-not-stop-on-signals-used-by-seastar).

### Advanced guides

TODO: write guides for typical flows for debugging an OOM situation and
any other situation that contains typical steps.
