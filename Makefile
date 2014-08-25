mode = release

out = build/$(mode)

all:
	@mkdir -p $(out)
	$(MAKE) $(MAKEFLAGS) -C $(out) src=../.. -f ../../build.mk mode=$(mode)

clean:
	rm -rf $(out)