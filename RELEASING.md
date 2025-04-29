# Releasing a new version of the YellowDog Airflow Provider

When the Airflow provider is ready for release:

1. Ensure all required changes have been committed
2. Ensure the version number in [yellowdog_provider/__init__.py](yellowdog_provider/__init__.py) has been updated
3. Tag the repository
4. Upload the new package version to PyPI
5. Build the documentation container image and upload to Dockerhub


## 1. Tag the repository with the new version number

The repository can be tagged using:

```commandline
PROVIDER_VERSION=$(grep "__version__ =" yellowdog_provider/__init__.py | sed -E 's/.*"([^"]+)".*/\1/')
git tag -a v$PROVIDER_VERSION -m "Version $PROVIDER_VERSION"
```

## 2. Upload the updated package to PyPi

The [Makefile](Makefile) provides targets for uploading new package versions to PyPI. To upload to production PyPI:

```commandline
make pypi-prod-upload
```

It's also possible first to check the build using `twine`:

```commandline
make pypi-check-build
```

It's also possible to test the upload against the Test PyPI server:

```commandline
make pypi-test-upload
```

## 2. Build the documentation and the documentation container image

To build the container image, Docker must be running, and you must be signed in to the YellowDog Dockerhub account.

The following will build the image and push to the YellowDog Dockerhub repository:

```html
make docs-publish-image
```

Images are tagged with the version of the provider.

Finally, **notify the operations team** that a new version of the documentation is available.
