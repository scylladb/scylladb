# Building a CentOS-based Docker image
Running
```
docker build -t <image-name> .
```
in this directory will build a Docker image of Scylla. You can then run
it with, for example,
```
docker run --name scylla -d -p 9042:9042 -t <image name>
```

However, it is important to note that while the resulting image will contain
some scripts taken from this directory, the actual Scylla executable will
**not** be taken from this build directory. Instead, our Dockerfile downloads
the Scylla executable and other Scylla tools (e.g., JMX, nodetool, etc.) from
http://downloads.scylladb.com/. If you want to build a Docker image which
includes a Scylla executable which you compiled yourself, please refer
to the next section.

## Docker image with a self-built executable

The following instructions will allow you to build a Docker image which
contains a combination of some tools from the nightly build in
http://downloads.scylladb.com/ (as described above) but with a Scylla
executable which you build yourself.

The following instructions are currently messy, but we hope to one day make
them as simple as "ninja docker".

Do the following in the top-level Scylla source directory:

1. Build your own Scylla in whatever build mode you prefer, e.g., dev.

2. Run `./reloc/build_reloc.sh --mode dev`

3. Run `./reloc/build_rpm.sh --reloc-pkg build/dev/scylla-package.tar.gz`

4. cd to `dist/docker/redhat`

5. Docker stubbornly refuses to allow using files from outside the current
   directory in preparing images, so we must copy the RPMs prepared above
   into the current directory (a symbolic link would _not_ work):
   `rm -r rpms; cp -a ../../../build/redhat/RPMS/x86_64 rpms`

6. Add the following lines near the end of Dockerfile, after the `RUN curl`:
   ```
   COPY rpms /rpms
   RUN yum install -y /rpms/*.rpm
   ```

7. Finally, run `docker build -t <image-name> .`
