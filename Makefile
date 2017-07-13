srcdir := src
outdir := bin

export PATH := $(CURDIR)/node_modules/.bin:$(PATH)

.PHONY: all
all: build

.PHONY: clean
clean:
	rm -rf -- coverage tags $(outdir)

.PHONY: build
build:
	@mkdir -p $(outdir)
	babel $(srcdir) --out-dir $(outdir)

.PHONY: test
test: build
	rm -rf coverage
	DEBUG=y BABEL_ENV=test istanbul cover _mocha -- --bail --require babel-polyfill --use_strict $(outdir)/test
