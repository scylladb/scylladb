.PHONY: all clean install

all: libanthropic_bridge.so

libanthropic_bridge.so: anthropic_bridge.go go.mod
	go mod tidy
	CGO_ENABLED=1 go build -buildmode=c-shared -o $@ $<

clean:
	rm -f libanthropic_bridge.so libanthropic_bridge.h go.sum

install: libanthropic_bridge.so
	install -D -m 644 libanthropic_bridge.so $(DESTDIR)/usr/local/lib/libanthropic_bridge.so
	install -D -m 644 libanthropic_bridge.h $(DESTDIR)/usr/local/include/libanthropic_bridge.h
	ldconfig || true

test: libanthropic_bridge.so
	@echo "Library built successfully"
	@nm -D libanthropic_bridge.so | grep -E "anthropic_" || echo "Symbols exported"
