# Releasing the YellowDog Airflow Provider

Releases are made from `main` using the local release script, `bin/release.py`
(stdlib only). It needs `twine` and Docker, and `~/.pypirc` must define the
repositories `yellowdog-airflow-provider` (PyPI) and `yellowdog-testpypi`
(Test PyPI), with credentials for both stored there: the script pipes twine's
output, so an interactive credential prompt would not be visible and the upload
would appear to hang. You must be signed in to the YellowDog Dockerhub account.

## Steps

1. Merge `next-version` into `main` and make sure `main` is pushed.
2. On `main`, set `__version__ = "2.0.0"` (the release version, with no `.dev`,
   `rc` or local suffix) in `yellowdog_provider/__init__.py`, and date the
   heading in `docs/changelog.rst` as `2.0.0 (YYYY-MM-DD)`. Commit and push.
3. Optionally rehearse against Test PyPI (no tag, no docs image):

   ```commandline
   make release-test
   ```

4. Release:

   ```commandline
   make release
   ```

5. Notify the operations team that docs `v<version>` are available.
6. Back on `next-version`, bump `__version__` to the next `.dev0` version.

## What the script does

Preflight: on `main`; `HEAD` equals `origin/main` after `git fetch`; no
uncommitted changes to tracked files (untracked files are ignored); the version
is a final release version; the changelog has a dated heading; the tag does not
exist locally or on origin; the version is not already on the target index;
`make check` passes. It then prints a summary and asks you to type the version
to confirm, runs `make clean build` and `twine check dist/*`, tags
`v<version>`, uploads with `twine`, pushes the tag, runs
`make docs-publish-image`, and reminds you to notify operations. If the upload
fails or is interrupted (including Ctrl-C), the local tag is deleted. If a
later step fails, the version is already on PyPI: the script says so and prints
the command that finishes the release (`git push origin v<version>` and/or
`make docs-publish-image`). Failed commands' stderr is included in the error.

Options (`python bin/release.py ...`): `--test-pypi`, `--dry-run` (preflight and
build only, prints the remaining commands), `--skip-docs-image`, `--yes`
(skip confirmation).
