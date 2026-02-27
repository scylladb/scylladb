# Global variables
# You can set these variables from the command line.
POETRY        := PYTHON_KEYRING_BACKEND=keyring.backends.null.Keyring poetry
SPHINXOPTS    := -j auto
SPHINXBUILD   := $(POETRY) run sphinx-build
PAPER         :=
BUILDDIR      := _build
SOURCEDIR     := .
PREVIEW_HOST  := 127.0.0.1
FLAG          := manual
CONF_PATH     := ./

# Internal variables
ifeq ($(FLAG), enterprise)
    CONF_PATH = ./_enterprise
endif

PAPEROPT_a4     := -D latex_paper_size=a4
PAPEROPT_letter := -D latex_paper_size=letter
ALLSPHINXOPTS   := -d $(BUILDDIR)/doctrees $(PAPEROPT_$(PAPER)) $(SPHINXOPTS) $(SOURCEDIR) -t $(FLAG) -c $(CONF_PATH)
TESTSPHINXOPTS  := $(ALLSPHINXOPTS) -W --keep-going
PROD_OPTS       := -D html_theme_options.collapse_navigation='false' -D html_theme_options.navigation_depth=3 -t $(FLAG) -c $(CONF_PATH)

.PHONY: all
all: dirhtml

# Setup commands
.PHONY: setupenv
setupenv:
	pip install -q 'poetry>=1.8.0'

.PHONY: setup
setup:
	$(POETRY) install

.PHONY: update
update:
	$(POETRY) update

# Clean commands
.PHONY: pristine
pristine: clean
	git clean -dfX

.PHONY: clean
clean:
	rm -rf $(BUILDDIR)/*
	rm -rf _data/*

# Generate output commands
.PHONY: dirhtml
dirhtml: setup
	FLAG=$(FLAG) $(SPHINXBUILD) -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: singlehtml
singlehtml: setup
	FLAG=$(FLAG) $(SPHINXBUILD) -b singlehtml $(ALLSPHINXOPTS) $(BUILDDIR)/singlehtml
	@echo
	@echo "Build finished. The HTML page is in $(BUILDDIR)/singlehtml."

.PHONY: epub
epub: setup
	FLAG=$(FLAG) $(SPHINXBUILD) -b epub $(ALLSPHINXOPTS) $(BUILDDIR)/epub
	@echo
	@echo "Build finished. The epub file is in $(BUILDDIR)/epub."

.PHONY: epub3
epub3: setup
	FLAG=$(FLAG) $(SPHINXBUILD) -b epub3 $(ALLSPHINXOPTS) $(BUILDDIR)/epub3
	@echo
	@echo "Build finished. The epub3 file is in $(BUILDDIR)/epub3."

.PHONY: multiversion
multiversion: setup
	FLAG=$(FLAG) $(POETRY) run sphinx-multiversion $(SOURCEDIR) $(BUILDDIR)/dirhtml $(PROD_OPTS)
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: redirects
redirects: setup
	FLAG=$(FLAG) $(POETRY) run redirects-cli fromfile --yaml-file ./_utils/redirects.yaml --output-dir $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

# Preview commands
.PHONY: preview
preview: setup
	FLAG=$(FLAG) $(POETRY) run sphinx-autobuild -b dirhtml $(ALLSPHINXOPTS) $(BUILDDIR)/dirhtml --host $(PREVIEW_HOST) --port 5500 --ignore *.csv --ignore *.json --ignore *.yaml

.PHONY: multiversionpreview
multiversionpreview: multiversion
	FLAG=$(FLAG) $(POETRY) run python -m http.server 5500 --directory $(BUILDDIR)/dirhtml

# Test commands
.PHONY: test
test: setup
	FLAG=$(FLAG) $(SPHINXBUILD) -b dirhtml $(TESTSPHINXOPTS) $(BUILDDIR)/dirhtml
	@echo
	@echo "Build finished. The HTML pages are in $(BUILDDIR)/dirhtml."

.PHONY: linkcheck
linkcheck: setup
	FLAG=$(FLAG) $(SPHINXBUILD) -b linkcheck $(SOURCEDIR) $(BUILDDIR)/linkcheck
