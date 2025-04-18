%.pdf: %.dot
	dot $< -Tpdf -o $@

%.png: %.dot
	dot $< -Tpng -o $@

DOTFILES = $(basename $(wildcard *.dot))
all: $(addsuffix .png, $(DOTFILES)) $(addsuffix .pdf, $(DOTFILES))
