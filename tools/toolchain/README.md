# Official toolchain for ScyllaDB

While we aim to build out-of-the-box on recent distributions, this isn't
always possible and not everyone runs a recent distribution. For this reason
a version-controlled toolchain is provided as a docker image.

## Quick start

If your workstation supports docker (without requiring sudo), you can build and
run Scylla easily without setting up the build dependencies beforehand:

    ./tools/toolchain/dbuild ./configure.py
    ./tools/toolchain/dbuild ninja build/release/scylla
    ./tools/toolchain/dbuild ./build/release/scylla --developer-mode 1

## The `dbuild` script

The script `dbuild` allows you to run any in that toolchain with
the working directory mounted:

    ./tools/toolchain/dbuild ./configure.py
    ./tools/toolchain/dbuild ninja

You can adjust the `docker run` command by adding more flags before the
command to be executed, separating the flags and the command with `--`.
This can be useful to attach more volumes (for data or ccache) and to
set environment variables. For example, to use ccache:

    ./tools/toolchain/dbuild -e PATH=/usr/lib64/ccache:/usr/bin:/usr/local/bin -v $HOME/.ccache:$HOME/.ccache:z -- ninja

The above command works as follows:
1. Ccache comes pre-installed in the container, and the setting of PATH
   to put it first causes the ccache wrappers to be used for compilation.
2. The "-v" option mounts the user's regular ccache cache directory into the
   container, so the same directory can be reused.
3. The ":z" flag is necessary on systems with SELinux enabled, and causes a
   special selinux label to be applied to $HOME/.ccache so docker can write
   it. While this (rightfully) sounds fishy, note that dbuild already does
   the same thing to your current directory, where the build output is
   written - the current directory is also mounted with the ":z" flag.

To pass the same options to every run of dbuild, put them in the file
~/.config/scylladb/dbuild, which should contain a bash array assignment:

SCYLLADB_DBUILD=(-e PATH=/usr/lib64/ccache:/usr/bin:/usr/local/bin -v $HOME/.ccache:$HOME/.ccache:z)

The script also works from other directories, so if you have `scylla-ccm` checked
out alongside scylla, you can write


    ../scylla/tools/toolchain/dbuild ./ccm ...

You will have access to both scylla and scylla-ccm in the container.

Interactive mode is also supported: running `dbuild` with no arguments
will drop you into a shell, with all of the toolchain accessible.

## Obtaining the current toolchain

The toolchain is stored in a file called `tools/toolchain/image`. Normally,
`dbuild` will fetch the toolchain automatically. If you want to access
the toolchain explicitly, pull that image:

    docker pull $(<tools/toolchain/image)

## Building the toolchain

If you add dependencies (to `install-dependencies.sh` or
`seastar/install-dependencies.sh`) you should update the toolchain.

Run the command

    podman build --no-cache --pull -f tools/toolchain/Dockerfile .

and use the resulting image.

## Publishing an image

If you're a maintainer, you can tag the image and push it
using `podman push`. Tags follow the format
`scylladb/scylla-toolchain:fedora-29-[branch-3.0-]20181128`.

For master toolchains, the branch designation is omitted. In a branch, if
there is a need to update a toolchain, the branch designation is added to
the tag to avoid ambiguity.

Publishing an image is complicated since multiple architectures are supported.
There are two procedures, one using emulation (can run on any x86 machine) and
another using native systems, which requires access to aarch64 and s390x machines.

## Emulated publishing procedure (slow)

1. Pick a new name for the image (in `tools/toolchain/image`) and
   commit it. The commit updating install-dependencies.sh should
   include the toolchain change, for atomicity. Do not push the commit
   to `next` yet.
2. Run `tools/toolchain/prepare --clang-build-mode INSTALL_FROM --clang-archive <filename to the archive>` and wait.
   The clang archive need to be download prior to build.
   It requires `buildah` and `qemu-user-static` to be installed
   (and will complain if they are not).
3. Publish the image using the instructions printed by the previous step.
4. Push the `next` branch that refers to the new toolchain.

## Native publishing procedure (complicated)

1. Pick a new name for the image (in `tools/toolchain/image`) and
   commit it. The commit updating install-dependencies.sh should
   include the toolchain change, for atomicity. Do not push the commit
   to `next` yet.
2. Push the commit to a personal repository/branch.
3. Perform the following on an x86 and an ARM machine:
    1. check out the branch containing the new toolchain name
    2. Run `git submodule update --init --recursive` to make sure
       all the submodules are synchronized
    3. Run `tools/toolchain/prepare --clang-build-mode INSTALL_FROM --clang-archive <archive-path> --disable-multiarch`. This should complete relatively quickly.
4. Now, create a multiarch image with the following:
    1. Push one of the images using the `podman manifest push` command suggested by `tools/toolchain/prepare`.
    2. For the other image, first merge the other image into it. This is done by using the command from step 1, but replacing `push` with `add`. For example, if in step 1 you pushed the x86_64 image, in step 2 you add the x86_64 image to the local aarch64 image. This creates a local image supporting the two architectures.
    3. Push the combined image using the `podman manifest push` command suggested by `tools/toolchain/prepare`. This replaces the single-architecture image with a two-architecture image.
5. Now push the commit that updated the toolchain with `git push`.

## Troubleshooting

When running `sudo` inside the container fails like this:
```
$ tools/toolchain/dbuild /bin/bash
bash-4.4$ sudo dnf install gdb
sudo: unknown uid 1000: who are you?
```

You can work it around by disabling SELinux on the host before running `dbuild`:
```
$ sudo setenforce 0
```
