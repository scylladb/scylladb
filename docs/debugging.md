# Debugging with GDB

## Introduction

GDB is a source level debugger for C, C++ and more languages. It allows
inspecting the internal state of a program as it is running as well the
post-mortem inspection of chrashed programs.

You can attach GDB to a running process, run a process inside GDB or
examine a coredump.

## Starting GDB

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

## Using GDB

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

Over the years we have collected a set of tools for helping with
 debugging scylla. These are collected in
 [scylla-gdb.py](../scylla-gdb.py) and are in the form of
[commands](https://sourceware.org/gdb/onlinedocs/gdb/Commands.html#Commands),
[conveninence functions](https://sourceware.org/gdb/onlinedocs/gdb/Convenience-Funs.html#Convenience-Funs)
and [pretty
printers](https://sourceware.org/gdb/onlinedocs/gdb/Pretty-Printing.html#Pretty-Printing).
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

    (gdb) set substitute-path /path/to/src/in/executable
    /path/to/src/on/your/machine

Note that the pattern that you supply to `set substitute-path` just has
to be a common prefix of the paths. Example: if the source location
inside the executable to some file is `/opt/src/scylla/database.hh` and
on your machine it is `/home/joe/work/scylla/database.hh`, you can make
GDB find the sources on your machine via:

   (gdb) set substitute-path /opt/src/scylla /home/joe/work/scylla

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

### Debugging relocatable binaries built with the toolchain

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

### Advanced guides

TODO: write guides for typical flows for debugging an OOM situation and
any other situation that contains typical steps.
