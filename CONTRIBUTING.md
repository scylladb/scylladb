# Contributing to Scylla

## Asking questions or requesting help

Use the [Scylla Users mailing list](https://groups.google.com/g/scylladb-users) or the [Slack workspace](http://slack.scylladb.com) for general questions and help.

Join the [Scylla Developers mailing list](https://groups.google.com/g/scylladb-dev) for deeper technical discussions and to discuss your ideas for contributions.

## Reporting an issue

Please use the [issue tracker](https://github.com/scylladb/scylla/issues/) to report issues or to suggest features. Fill in as much information as you can in the issue template, especially for performance problems.

## Contributing code to Scylla

Before you can contribute code to Scylla for the first time, you should sign the [Contributor License Agreement](https://www.scylladb.com/open-source/contributor-agreement/) and send the signed form cla@scylladb.com. You can then submit your changes as patches to the to the [scylladb-dev mailing list](https://groups.google.com/forum/#!forum/scylladb-dev) or as a pull request to the [Scylla project on github](https://github.com/scylladb/scylla).
If you need help formatting or sending patches, [check out these instructions](https://github.com/scylladb/scylla/wiki/Formatting-and-sending-patches).

The Scylla C++ source code uses the [Seastar coding style](https://github.com/scylladb/seastar/blob/master/coding-style.md) so please adhere to that in your patches. Note that Scylla code is written with `using namespace seastar`, so should not explicitly add the `seastar::` prefix to Seastar symbols. You will usually not need to add `using namespace seastar` to new source files, because most Scylla header files have `#include "seastarx.hh"`, which does this.

Header files in Scylla must be self-contained, i.e., each can be included without having to include specific other headers first. To verify that your change did not break this property, run `ninja dev-headers`. If you added or removed header files, you must `touch configure.py` first - this will cause `configure.py` to be automatically re-run to generate a fresh list of header files.
