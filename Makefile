mode = release

out = build/$(mode)

all: $(out)/build.ninja
	@ninja-build -f $<

$(out)/build.ninja:
	python3 configure.py

clean:
	rm -rf $(out)

cscope:
	find -name '*.[chS]' -o -name "*.cc" -o -name "*.hh" | cscope -bq -i-
	@echo cscope index created
.PHONY: cscope
