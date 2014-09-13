mode = release

out = build/$(mode)

all:
	@mkdir -p $(out)
	$(MAKE)  -C $(out) src=../.. -f ../../build.mk $(MAKEFLAGS) mode=$(mode)

clean:
	rm -rf $(out)

cscope:
	find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
	@echo cscope index created
.PHONY: cscope
