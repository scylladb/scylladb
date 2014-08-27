mode = release

out = build/$(mode)

all:
	@mkdir -p $(out)
	$(MAKE)  -C $(out) src=../.. -f ../../build.mk $(MAKEFLAGS) mode=$(mode)

clean:
	rm -rf $(out)