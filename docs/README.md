# ScyllaDB Documentation

This repository contains the source files for ScyllaDB Open Source documentation.

- The `dev` folder contains developer-oriented documentation related to the ScyllaDB code base. It is not published and is only available via GitHub.
- All other folders and files contain user-oriented documentation related to ScyllaDB Open Source and are sources for [docs.scylladb.com](https://docs.scylladb.com/).

To report a documentation bug or suggest an improvement, open an issue in [GitHub issues](https://github.com/scylladb/scylla/issues) for this project.

To contribute to the documentation, open a GitHub pull request.

## Key Guidelines for Contributors

- You need to fork this repository.
- Work on your fork's task branches, not the `master` branch.
- Send pull requests to the `master` branch in this repository.
- If you want to submit a knowledge base article, see [Submitting a KB Article](#submitting-a-kb-article).

The user documentation is written in reStructuredText (RST) - a plaintext markup language similar to Markdown. If you're not familiar with RST, see [ScyllaDB RST Examples](https://sphinx-theme.scylladb.com/stable/examples/index.html).

The developer documentation is written in Markdown. See [Basic Markdown Syntax](https://www.markdownguide.org/basic-syntax/) for reference.


## Submitting a KB Article

If you are submitting a Knowledgebase Article (KBA), use the following guidelines:
* In the `/kb_common` directory there is a template for KBAs. It is called `kb-article-template.rst`.
* Make a copy of this file in the `/kb directory`, saving it with a unique name.
* Open the template and fill in the required inforation. 
* Remove what is not needed. 
* Run`make preview` to make the docs and preview locally.
* Send a PR - add KBA in the title. 


## Building User Documentation

### Prerequisites

* Python 3. Check your version with `$ python --version`
* Vale CLI (optional to lint docs). [Install Vale](https://docs.errata.ai/vale/install) for your operative system.

#### Mac OS X

You must have a working [Homebrew](http://brew.sh/) in order to install the needed tools.

You also need the standard utility `make`.

Check if you have these two items with

```sh
brew help
make -h
```

#### Fedora 29/Debian-based Linux Distributions

Building the user docs should work out of the box on Fedora 29.

#### Windows

Use "Bash on Ubuntu on Windows" for the same tools and capabilites as on a debian-based Linux.

Note: livereload seems not working on Windows.

### Building the Docs 

1. Run `make preview` to build the documentation.
1. Preview the built documentation locally at http://127.0.0.1:5500/.

### Cleanup

You can clean up all the build products and auto-installed Python stuff with:

```sh
make pristine
```

### Lint

Lint all:

```sh
make proofread
```

Lint one file (e.g. README.md):

```sh
make proofread path=README.md
```

Lint one folder (e.g. getting-started):

```sh
make proofread path=getting-started
```

# Information for Contributors

If you are interested in contributing to Scylla
docs, please read the Scylla open source page at
http://www.scylladb.com/opensource/ and complete
a Scylla contributor agreement if needed.  We can
only accept documentation pull requests if we have
a contributor agreement on file for you


# Third-party Documentation

 * Do any copying as a separate commit.  Always commit an unmodified version first and then do any editing in a separate commit.

 * We already have a copy of the Apache license in our tree so you do not need to commit a copy of the license.

 * Include the copyright header from the source file in the edited version.  If you are copying an Apache Cassandra document with no copyright header, use:

```
This document includes material from Apache Cassandra.
Apache Cassandra is Copyright 2009-2014 The Apache Software Foundation.
```
