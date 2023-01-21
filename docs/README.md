# ScyllaDB Documentation

This repository contains the source files for ScyllaDB Open Source documentation.

- The `dev` folder contains developer-oriented documentation related to the ScyllaDB code base. It is not published and is only available via GitHub.
- All other folders and files contain user-oriented documentation related to ScyllaDB Open Source and are sources for [docs.scylladb.com](https://docs.scylladb.com/).

To report a documentation bug or suggest an improvement, open an issue in [GitHub issues](https://github.com/scylladb/scylla/issues) for this project.

To contribute to the documentation, open a GitHub pull request.

## Key Guidelines for Contributors

- Follow the [ScyllaDB Style Guide](https://docs.google.com/document/d/1lyHp1MKdyj0Hh3NprNFvEczA4dFSZIFoukGUvFJb9yE/edit?usp=sharing).
- The user documentation is written in reStructuredText (RST) - a plaintext markup language similar to Markdown. If you're not familiar with RST, see [ScyllaDB RST Examples](https://sphinx-theme.scylladb.com/stable/examples/index.html).
- The developer documentation is written in Markdown. See [Basic Markdown Syntax](https://www.markdownguide.org/basic-syntax/) for reference.


## Creating Knowledge Base Articles

The `kb/` directory holds source files for knowledge base articles in the [Knowledge Base section](https://docs.scylladb.com/stable/kb/index.html) of the ScyllaDB documentation. 

The `kb/kb_common` subdirectory contains a template for knowledge base articles to help you create new articles.

To create a new knowledge base article (KB):

1. Copy the `kb-article-template.rst` file from `/kb/kb_common` to `/kb` and rename it with a unique name.
1. Open the new file and fill in the required information. 
1. Remove what is not needed. 
1. Run `make preview` to build the docs and preview them locally.
1. Send a PR with "KB" in its title. 


## Building User Documentation

### Prerequisites

* Python 3. Check your version with `$ python --version`.
* [poetry](https://python-poetry.org/) 1.12 or later
* make

#### Mac OS X

You must have a working [Homebrew](http://brew.sh/) in order to install the needed tools.

You also need the standard utility `make`.

Check if you have these two items with the following commands:

```sh
brew help
make -h
```

#### Linux Distributions

Building the user docs should work out of the box  on most Linux distributions.

#### Windows

Use "Bash on Ubuntu on Windows" for the same tools and capabilities as on Linux distributions.

### Building the Docs 

1. Run `make preview` to build the documentation.
1. Preview the built documentation locally at http://127.0.0.1:5500/.

### Cleanup

You can clean up all the build products and auto-installed Python stuff with:

```sh
make pristine
```

## Information for Contributors

If you are interested in contributing to Scylla
docs, please read the Scylla open source page at
http://www.scylladb.com/opensource/ and complete
a Scylla contributor agreement if needed.  We can
only accept documentation pull requests if we have
a contributor agreement on file for you.


## Third-party Documentation

 * Do any copying as a separate commit.  Always commit an unmodified version first and then do any editing in a separate commit.

 * We already have a copy of the Apache license in our tree, so you do not need to commit a copy of the license.

 * Include the copyright header from the source file in the edited version.  If you are copying an Apache Cassandra document with no copyright header, use:

```
This document includes material from Apache Cassandra.
Apache Cassandra is Copyright 2009-2014 The Apache Software Foundation.
```
