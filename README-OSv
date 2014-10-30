Running Seastar on OSv
======================

1. Compiling Seastar for OSv
----------------------------

Before compiling Seastar, configure it with the following command:

./configure.py --so --disable-hwloc \
    --cflags="-DDEFAULT_ALLOCATOR -fvisibility=default -DHAVE_OSV -I../osv/include" \
    --mode release 

Or more easily, use the "--with-osv=..." shortcut for all the above settings:

./configure.py --mode release --with-osv=../osv


Explanation of these configuration options:
   * The "--so" option is needed so that the Seastar applications, such as
     httpd, are built as shared objects instead of ordinary executables.
     Note the "--pie" option also exists, but because of bug #352 in OSv,
     and the fact that Seastar uses thread_local in one place, these PIEs
     cannot be run on OSv.
   * The "--disable-hwloc" option is needed so that Seastar does not attempt
     to use the complex NUMA-discovery library, which isn't supported on OSv
     (and isn't relevant anyway, because VMs with NUMA are not (yet) common.
   * The "-DEFAULT_ALLOCATOR" uses in Seastar the system's regular malloc()
     and free(), instead of redefining them. Without this flag, what seems
     to happen is that some code compiled into the OSv kernel (notably,
     libboost_program_options.a) uses the standard malloc(), while inline
     code compiled into Seastar uses the Seastar free() to try and free that
     memory, resulting in a spectacular crash.
   * The "-fvisibility=default" option disables the "-fvisibility=hidden"
     option which is hard-coded into Seastar's build file. Supposedly
     "-fvisibility=hidden" provides some better optimization in some cases,
     but it also means OSv can't find main() in the generated shared object!
     So either we use "-fvisibility=default", as suggested here, or
     alternatively, make *only* main() visible - for example, to make httpd's
     main() visible add in apps/httpd/httpd.cc, before the main() definition,
     the chant: [[gnu::visibility("default")]]
   * The "-DHAVE_OSV" conditionally compiles code in Seastar that relies
     on OSv APIs (currently, this enables virtio device assignment).
     This OSv-specific code relies on some OSv header files, which is
     why the "-I../osv/include" is needed (where "../osv" is where the
     OSv source tree is open).
   * The "--mode release" is to compile only the release build. You'll
     usually not want to debug Seastar on OSv (it's easier to debug on Linux).


2. Building a Seastar-httpd module for OSv
------------------------------------------

As an example, we'll build a "seastar" module in OSv running Seastar's
httpd application.

In the OSv working directory, create a directory apps/seastar in it put:

* a link to the httpd binary in the Seastar working directory. I.e.,
  ln -s ../../../seastar/build/release/apps/httpd/httpd httpd

* A usr.manifest file, adding only this single "httpd" executable to the image:
  /httpd: ${MODULE_DIR}/httpd

* A module.py file with a default command line:

  from osv.modules import api
  default = api.run(cmdline="/httpd --no-handle-interrupt")  

The "--no-handle-interrupt" is needed so that Seastar does not attempt to
use signalfd() to capture ^C. signalfd is not yet available on OSv, and
the ^C handling is not a very important feature of Seastar anyway.

Also note that currently we cannot specify "--network-stack=native", because
neither vhost nor a more efficient mechanism for "virtio assignment" is yet
complete on OSv. So we must keep the default (which is "--network-stack=posix")
which uses the OS's Posix networking APIs, which OSv fully supports.


3. Running the seastar module on OSv
-------------------------------------

To run the Seastar httpd application, using the module defined above, do,
as usual, in the OSv working directory:

$ make image=seastar -j4
$ sudo scripts/run.py -nvV

This will open an HTTP server on port 10000 of the VM. For example, if the
above creates a VM with an IP address of 192.168.122.89, we can test it as
following:

$ curl 192.168.122.89:10000 
<html><head><title>this is the future</title></head><body><p>Future!!</p></body></html>

4. Debugging OSv with the Seastar application
---------------------------------------------

If you want to debug OSv (not the Seastar application) in relation to the
way it runs Seastar, you'll want the "httpd" shared object to be available
to gdb.
Unfortunately, the object lookup code in "osv syms" (translate() in loader.py)
does not seem to look for objects in apps/, so until we fix this, we need
to put a link to httpd in a well-known place, such as build/release. So
do this in the OSv top directory:
 ln -s ../../apps/seastar/httpd build/release/httpd

Note you'll need to repeat this if you do "make clean" (as "make clean"
removes everything in build/release).
