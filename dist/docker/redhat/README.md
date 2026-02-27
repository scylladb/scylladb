## Docker image with a self-built executable

The following instructions will allow you to build a Docker image which
contains a combination of some tools from the nightly build in
http://downloads.scylladb.com/ (as described above) but with a Scylla
executable which you build yourself.

Do the following in the top-level Scylla source directory:

1. Build your own Scylla in whatever build mode you prefer, e.g., dev.

2. Run `ninja dist-dev` (with the same mode name as above) to prepare
   the distribution artifacts.

3. Run `./dist/docker/debian/build_docker.sh --mode dev`
   
   This creates a docker image as a **file**, in the OCI format, and prints
   its name, looking something like:
   `oci-archive:build/dev/dist/docker/scylla-4.6.dev-0.20210829.4009d8b06`

4. This file can copied to a docker repository, or run directly with podman:
    
   `podman run oci-archive:build/dev/dist/docker/scylla-4.6.dev-0.20210829.4009d8b06`

   Often with additional parameters, as in docs/alternator/getting-started.md:
    `podman run --name scylla -d -p 8000:8000 oci-archive:... --alternator-port=8000 --alternator-write-isolation=always`
