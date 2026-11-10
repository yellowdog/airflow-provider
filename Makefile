.DEFAULT_GOAL := no_op

.PHONY: build clean install uninstall format lint test check update docs \
	docs-build-image docs-publish-image pypi-check-build no_op

BUILD_DIST = build dist yellowdog_airflow_provider.egg-info
PYCACHE = __pycache__

VERSION_FILE := yellowdog_provider/__init__.py
VERSION := $(shell grep "__version__ =" $(VERSION_FILE) | sed -E 's/.*"([^"]+)".*/\1/')

build:
	python -m build

clean:
	rm -rf $(BUILD_DIST) $(PYCACHE) .ruff_cache .pytest_cache
	$(MAKE) -C docs clean

install: build
	pip install -U -e '.[dev]'

uninstall:
	pip uninstall -y yellowdog-airflow-provider

format:
	ruff format . && ruff check --fix .

lint:
	ruff format --check . && ruff check .

test:
	pytest tests/unit

typecheck:
	pyright && python bin/verify_types.py

check: lint typecheck test docs

update:
	pip install -U -e '.[dev]'

docs:
	$(MAKE) -C docs html SPHINXOPTS="-W --keep-going"

docs-build-image: docs
	cd docs && docker build . -t yellowdogco/airflow-provider-docs:$(VERSION) --platform linux/amd64

docs-publish-image: docs-build-image
	docker push yellowdogco/airflow-provider-docs:$(VERSION)

pypi-check-build: clean build
	twine check dist/*

no_op:
	# Available targets are: build, clean, install, uninstall, format, lint, test,
	# check, update, docs, docs-build-image, docs-publish-image, pypi-check-build
