## Docker image with a self-built executable

The following instructions will allow you to build a Docker image which
contains a combination of some tools from the nightly build in
http://downloads.scylladb.com/ (as described above) but with a Scylla
executable which you build yourself.

Do the following in the top-level Scylla source directory:

1. Build your own Scylla in whatever build mode you prefer, e.g., dev.

2. Run `ninja dist-deb`

3. Run `/dist/docker/debian/build_docker.sh`

4. Finally, run `docker build -t <image-name> .`
