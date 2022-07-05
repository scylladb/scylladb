# Scylla documentation

This is the repository for [Scylla documentation site](http://docs.scylladb.com/)

To report an issue with the documentation, please use GitHub issues.

This repository accepts GitHub pull requests.

**Send pull requests to the master branch, not gh-pages.  gh-pages will be overwritten by deploy without warning.**


## Prerequisites

* Python 3. Check your version with `$ python --version`
* Vale CLI (optional to lint docs). [Install Vale](https://docs.errata.ai/vale/install) for your operative system.

### Prerequisites: Mac OS X

You must have a working [Homebrew](http://brew.sh/) in order to install the needed tools.

You also need the standard utility `make`.  (I don't know if this comes with Mac OS X.)

Check if you have these two items with

```sh
brew help
make -h
```

### Prerequisites: Fedora 29/Debian-based Linux Distributions

This should work out of the box with Fedora 29.

### Prerequisites: Windows

Use "Bash on Ubuntu on Windows", everything should be same as on a debian-based Linux.
Note: livereload seems not working on Windows.

## Prerequisites: other systems

FIXME

# Working on the docs

Work on a task branch and send pull requests for
master.  Master is the default branch.

Run `make preview` to make the docs and preview locally.


# Deploy

If you have the rights to push to the live site, run `make deploy` to deploy.

# Lint

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

# Cleanup

You can clean up all the build products and auto-installed Python stuff with:

```sh
make pristine
```

# Contributor info

If you are interested in contributing to Scylla
docs, please read the Scylla open source page at
http://www.scylladb.com/opensource/ and complete
a Scylla contributor agreement if needed.  We can
only accept documentation pull requests if we have
a contributor agreement on file for you

# Submitting a KB Article

If you are submitting a Knowledgebase Article (KBA), use the following guidelines:
* In the `/kb_common` directory there is a template for KBAs. It is called `kb-article-template.rst`.
* Make a copy of this file in the `/kb directory`, saving it with a unique name.
* Open the template and fill in the required inforation. 
* Remove what is not needed. 
* Run`make preview` to make the docs and preview locally.
* Send a PR - add KBA in the title. 

# Third-party documentation

 * Do any copying as a separate commit.  Always commit an unmodified version first and then do any editing in a separate commit.

 * We already have a copy of the Apache license in our tree so you do not need to commit a copy of the license.

 * Include the copyright header from the source file in the edited version.  If you are copying an Apache Cassandra document with no copyright header, use:

```
This document includes material from Apache Cassandra.
Apache Cassandra is Copyright 2009-2014 The Apache Software Foundation.
```
