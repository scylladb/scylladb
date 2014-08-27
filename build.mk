
S = $(src)
VPATH = $(src)

sanitize.debug = -fsanitize=address -fsanitize=leak -fsanitize=undefined
sanitize.release =

opt.debug = -O0
opt.release = -O2 -flto

sanitize = $(sanitize.$(mode))
opt = $(opt.$(mode))

libs = -laio

CXXFLAGS = -std=gnu++1y -g -Wall -Werror $(opt) -MD -MT $@ -MP $(sanitize) -fvisibility=hidden $(libs)
CXXFLAGS += -pthread

tests = test-reactor fileiotest virtiotest

link = $(CXX) $(CXXFLAGS) -o $@ $^

%: %.o
	$(link)

all: seastar $(tests) httpd

clean:
	rm seastar $(tests) *.o

seastar: main.o reactor.o
	$(link)

test-reactor: test-reactor.o reactor.o

httpd: httpd.o reactor.o

fileiotest: fileiotest.o reactor.o

virtiotest: virtiotest.o virtio.o reactor.o ip.o

-include *.d
