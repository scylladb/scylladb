# Scylla Coding Style

Please see the [Seastar style document](https://github.com/scylladb/seastar/blob/master/coding-style.md).

Scylla code is written with "using namespace seastar", and should not
explicitly add the "seastar::" prefix to Seastar symbols.
There is usually no need to add "using namespace seastar" to Source files,
because most Scylla header files #include "seastarx.hh", which does this.
