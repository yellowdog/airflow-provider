.DEFAULT_GOAL := no_op

SRC = yellowdog_provider/*.py yellowdog_provider/*/*.py docs/*.py
TESTS = tests/*.py tests/*/*.py
BUILD_DIST = build dist airflow_provider_yellowdog.egg-info
PYCACHE = __pycache__

VERSION_FILE := yellowdog_provider/__init__.py
VERSION := $(shell grep "__version__ =" $(VERSION_FILE) | sed -E 's/.*"([^"]+)".*/\1/')

build: $(SRC)
	python -m build

clean:
	rm -rf $(BUILD_DIST) $(PYCACHE)
	$(MAKE) -C docs clean

install: build
	pip install -U -e .

uninstall:
	pip uninstall -y airflow-provider-yellowdog

black: $(SRC) $(TESTS)
	black --preview $(SRC) $(TESTS)

isort: $(SRC)
	isort --profile black $(SRC) $(TESTS)

pyupgrade: $(SRC)
	pyupgrade --exit-zero-even-if-changed --py310-plus $(SRC) $(TESTS)

format: pyupgrade isort black

update:
	pip install -U pip -r requirements.txt -r requirements-dev.txt

.PHONY: docs

docs:
	$(MAKE) -C docs html

docs-build-image: docs
	cd docs && docker build . -t yellowdogco/airflow-provider-docs:$(VERSION) --platform linux/amd64

docs-publish-image: docs-build-image
	docker push yellowdogco/airflow-provider-docs:$(VERSION)

# See the ~.pypirc file for the repository index
pypi-check-build: clean build
	twine check dist/*

pypi-test-upload: clean build
	python -m twine upload --repository yellowdog-testpypi dist/*

pypi-prod-upload: clean build
	python -m twine upload --repository yellowdog-airflow-provider dist/*

no_op:
	# Available targets are: build, clean, install, uninstall, format, update, docs,
	# docs-build-image, docs-publish-image, pypi-check-build,
	# pypi-test-upload, pypi-prod-upload
