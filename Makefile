out = build

all: build.ninja
	@ninja-build -f $<

build.ninja: configure.py
	python3 configure.py

clean:
	rm -rf $(out)
