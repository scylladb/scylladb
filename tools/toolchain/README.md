# Official toolchain for ScyllaDB

While we aim to build out-of-the-box on recent distributions, this isn't
always possible and not everyone runs a recent distribution. For this reason
a version-controlled toolchain is provided as a docker image.

## Obtaining the current toolchain

The toolchain is stored in a file called `tools/toolchain/image`. To access
the toolchain, pull that image:

    docker pull $(<tools/toolchain/image)

A helper script `dbuild` allows you to run command in that toolchain with
the working directory mounted:

    ./tools/toolchain/dbuild ./configure.py
    ./tools/toolchain/dbuild ninja

You can adjust the `docker run` command by adding more flags before the
command to be executed, separating the flags and the command with `--`.
This can be useful to attach more volumes (for data or ccache) and to
set environment variables:

    ./tools/toolchain/dbuild -v /var/cache/ccache:/var/cache/ccache -- ninja

## Building the toolchain

Run the command

    docker build -f tools/toolchain/Dockerfile .

and use the resulting image.

## Publishing an image

If you're a maintainer, you can tag the image and push it
using `docker push`. Tags follow the format
`scylladb/scylla-toolchain:fedora-29-[branch-3.0-]20181128`. After the
image is pushed, update `tools/toolchain/image` so new
builds can use it automatically.

For master toolchains, the branch designation is omitted. In a branch, if
there is a need to update a toolchain, the branch designation is added to
the tag to avoid ambiguity.
