
S = $(src)
VPATH = $(src)

sanitize.debug = -fsanitize=address -fsanitize=leak -fsanitize=undefined
sanitize.release =

opt.debug = -O0
opt.release = -O2 -flto

sanitize = $(sanitize.$(mode))
opt = $(opt.$(mode))

libs = -laio -ltcmalloc

LDFLAGS = $(libs)

CXXFLAGS = -std=gnu++1y -g -Wall -Werror $(opt) -MD -MT $@ -MP $(sanitize) -fvisibility=hidden
CXXFLAGS += -pthread
CXXFLAGS += -I $(src)

# Ubuntu fails without this, see https://bugs.launchpad.net/ubuntu/+source/gcc-defaults/+bug/1228201
LDFLAGS += -Wl,--no-as-needed

tests = tests/test-reactor tests/fileiotest tests/virtiotest tests/l3_test tests/ip_test tests/timertest
tests += tests/tcp_test

link = mkdir -p $(@D) && $(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^
compile = mkdir -p $(@D) && $(CXX) $(CXXFLAGS) -c -o $@ $<

%: %.o
	$(link)

%.o: %.cc
	$(compile)

all: apps/seastar/seastar $(tests) apps/httpd/httpd

clean:
	rm seastar $(tests) *.o

apps/seastar/seastar: apps/seastar/main.o core/reactor.o
	$(link)

tests/test-reactor: tests/test-reactor.o core/reactor.o

apps/httpd/httpd: apps/httpd/httpd.o core/reactor.o

tests/fileiotest: tests/fileiotest.o core/reactor.o

tests/virtiotest: tests/virtiotest.o net/virtio.o core/reactor.o net/net.o net/ip.o net/ethernet.o net/arp.o

tests/l3_test: tests/l3_test.o net/virtio.o core/reactor.o net/net.o net/ip.o net/ethernet.o net/arp.o

tests/ip_test: tests/ip_test.o net/virtio.o core/reactor.o net/net.o net/ip.o net/arp.o net/ethernet.o

tests/tcp_test: tests/tcp_test.o net/virtio.o core/reactor.o net/net.o net/ip.o net/arp.o net/ethernet.o

tests/timertest: tests/timertest.o core/reactor.o

-include $(shell find -name '*.d')
