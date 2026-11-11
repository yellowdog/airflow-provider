.DEFAULT_GOAL := no_op

.PHONY: build clean install uninstall format lint test check update docs \
	docs-build-image docs-publish-image pypi-check-build no_op \
	typecheck system-test e2e-test system-test-cleanup release release-test

BUILD_DIST = build dist yellowdog_airflow_provider.egg-info
PYCACHE = __pycache__

VERSION_FILE := yellowdog_provider/__init__.py
VERSION := $(shell grep "__version__ =" $(VERSION_FILE) | sed -E 's/.*"([^"]+)".*/\1/')

# Airflow version labels for the live tests: min (pyproject floor), max (latest
# on PyPI) or X.Y.Z, separated by spaces or commas; e.g. `make system-test
# AIRFLOW=min,max`. Only an AIRFLOW given on the make command line counts (an
# exported AIRFLOW is ignored); it also overrides e2e.airflow_versions.
ifneq ($(origin AIRFLOW),command line)
AIRFLOW := min max
endif
comma := ,
AIRFLOW_LABELS = $(strip $(subst $(comma), ,$(AIRFLOW)))

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
	pytest tests/unit tests/release

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

release:
	python bin/release.py

release-test:
	python bin/release.py --test-pypi

# Component tests in a dedicated venv per Airflow version (bin/systest_env.py);
# every version runs even if one fails, and the target fails if any did.
system-test:
	@failed=""; \
	for label in $(AIRFLOW_LABELS); do \
		echo "==== System tests: Airflow $$label ===="; \
		if venv=$$(python bin/systest_env.py $$label) && \
			YD_SYSTEM_TEST_AIRFLOW=$$label "$$venv/bin/pytest" tests/system -m system -v; \
		then :; else failed="$$failed $$label"; fi; \
	done; \
	if [ -n "$$failed" ]; then echo "==== System tests FAILED for Airflow:$$failed"; exit 1; fi; \
	echo "==== System tests passed for Airflow: $(AIRFLOW_LABELS)"

e2e-test:
ifeq ($(origin AIRFLOW),file)
	pytest tests/system -m e2e -v -s
else
	YD_SYSTEM_TEST_AIRFLOW="$(AIRFLOW_LABELS)" pytest tests/system -m e2e -v -s
endif

system-test-cleanup:
	python bin/system-test-cleanup.py

no_op:
	# Available targets are: build, clean, install, uninstall, format, lint, test,
	# check, typecheck, update, docs, docs-build-image, docs-publish-image, pypi-check-build,
	# system-test, e2e-test, system-test-cleanup, release, release-test
