
.PHONY: all
all: version pyc

.PHONY : version
version:
	echo "version='$(shell git describe --tags --dirty)'" > python/samweb_client/_version.py

.PHONY : pyc
pyc:
	python -m compileall python/

.PHONY : dist
dist: all
	tar -czf dist.tar.gz bin python ups
	@if ! git diff-index --quiet HEAD; then echo "Warning! Uncommitted changes in working directory"; false; else \
	if ! git describe --tags --exact-match >& /dev/null; then echo "Warning! Tarball represents untagged version"; false; fi; fi

.PHONY: test
test:
	@test/testsuite.py
