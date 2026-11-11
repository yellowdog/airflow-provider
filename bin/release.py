#!/usr/bin/env python3
"""Local release script for the YellowDog Airflow provider (stdlib only).

Runs preflight checks, builds, tags, uploads to PyPI (or Test PyPI), pushes the
tag, and publishes the documentation image. See RELEASING.md.
"""

import argparse
import re
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = "yellowdog-airflow-provider"
PYPI_URL = f"https://pypi.org/pypi/{PACKAGE}"
TEST_PYPI_URL = f"https://test.pypi.org/pypi/{PACKAGE}"
PROD_REPOSITORY = "yellowdog-airflow-provider"
TEST_REPOSITORY = "yellowdog-testpypi"
DOCS_IMAGE = "yellowdogco/airflow-provider-docs"

Runner = Callable[[list[str]], str]


class ReleaseError(Exception):
    pass


class CommandError(subprocess.CalledProcessError):
    """A failed command; ``stderr_echoed`` says whether its stderr was shown live."""

    def __init__(
        self, returncode: int, cmd: list[str], output: str, stderr: str, echoed: bool
    ) -> None:
        super().__init__(returncode, cmd, output, stderr)
        self.stderr_echoed = echoed


def default_runner(root: Path = ROOT) -> Runner:
    """
    Run a command in `root` and return its stdout; raise CalledProcessError (with
    stdout and stderr captured separately) on failure. Output of non-git
    commands is also echoed as it arrives.
    """

    def run(cmd: list[str]) -> str:
        proc = subprocess.Popen(
            cmd,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert proc.stdout is not None and proc.stderr is not None
        echo = cmd[0] != "git"
        out: list[str] = []
        err: list[str] = []

        def read_stderr() -> None:
            assert proc.stderr is not None
            for line in proc.stderr:
                err.append(line)
                if echo:
                    sys.stderr.write(line)

        # Drain stderr in a thread so neither pipe can fill and block
        reader = threading.Thread(target=read_stderr, daemon=True)
        reader.start()
        for line in proc.stdout:
            out.append(line)
            if echo:
                sys.stdout.write(line)
        reader.join()
        if proc.wait() != 0:
            # stderr_echoed tells describe() not to repeat stderr already shown
            raise CommandError(proc.returncode, cmd, "".join(out), "".join(err), echo)
        return "".join(out)

    return run


def describe(e: BaseException) -> str:
    """An error message, with a failed command's stderr if any."""
    stderr = getattr(e, "stderr", None)
    echoed = isinstance(e, CommandError) and e.stderr_echoed
    if (
        isinstance(e, subprocess.CalledProcessError)
        and stderr
        and stderr.strip()
        and not echoed
    ):
        return f"{e}\n{stderr.strip()}"
    return str(e)


def read_version(init_path: Path) -> str:
    match = re.search(
        r'^__version__\s*=\s*"([^"]+)"', init_path.read_text(), re.MULTILINE
    )
    if not match:
        raise ReleaseError(f"No __version__ found in {init_path}")
    return match.group(1)


def check_version_releasable(version: str) -> None:
    if not re.fullmatch(r"\d+(\.\d+)*", version):
        raise ReleaseError(
            f"Version {version!r} is a dev/pre-release/local version; "
            "set a final version such as 2.0.0 before releasing"
        )


def check_changelog(changelog: Path, version: str) -> None:
    if not changelog.exists():
        raise ReleaseError(f"Changelog not found: {changelog}")
    pattern = rf"^{re.escape(version)} \(\d{{4}}-\d{{2}}-\d{{2}}\)\s*$"
    if not re.search(pattern, changelog.read_text(), re.MULTILINE):
        raise ReleaseError(
            f"{changelog} needs a heading '{version} (YYYY-MM-DD)' (not 'unreleased')"
        )


def check_git_state(run: Runner, branch: str = "main") -> None:
    current = run(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip()
    if current != branch:
        raise ReleaseError(f"Must release from branch '{branch}' (on '{current}')")
    run(["git", "fetch", "origin"])
    head = run(["git", "rev-parse", "HEAD"]).strip()
    origin = run(["git", "rev-parse", f"origin/{branch}"]).strip()
    if head != origin:
        raise ReleaseError(f"HEAD is not up to date with origin/{branch}")
    # Untracked files (e.g. scratch scripts) are deliberately ignored.
    if run(["git", "status", "--porcelain", "--untracked-files=no"]).strip():
        raise ReleaseError("Tracked files have uncommitted changes")


def version_on_pypi(version: str, index_url: str) -> bool:
    url = f"{index_url}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            return response.status == 200
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def tag_exists(run: Runner, tag: str) -> bool:
    if run(["git", "tag", "-l", tag]).strip():
        return True
    return bool(
        run(["git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}"]).strip()
    )


def main(
    argv: list[str] | None = None,
    *,
    run: Runner | None = None,
    root: Path = ROOT,
    ask: Callable[[str], str] = input,
) -> int:
    parser = argparse.ArgumentParser(description="Release the YellowDog provider")
    parser.add_argument("--test-pypi", action="store_true", help="Upload to Test PyPI")
    parser.add_argument(
        "--dry-run", action="store_true", help="Preflight and build only"
    )
    parser.add_argument("--skip-docs-image", action="store_true")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation")
    args = parser.parse_args(argv)

    runner = run or default_runner(root)
    prod = not args.test_pypi
    repository = PROD_REPOSITORY if prod else TEST_REPOSITORY
    index_url = PYPI_URL if prod else TEST_PYPI_URL

    try:
        # Preflight: git state first so a wrong branch stops everything early.
        check_git_state(runner)
        version = read_version(root / "yellowdog_provider" / "__init__.py")
        tag = f"v{version}"
        check_version_releasable(version)
        check_changelog(root / "docs" / "changelog.rst", version)
        if prod and tag_exists(runner, tag):
            raise ReleaseError(f"Tag {tag} already exists")
        if version_on_pypi(version, index_url):
            raise ReleaseError(f"Version {version} already exists on {index_url}")
        runner(["make", "check"])
    except (ReleaseError, subprocess.CalledProcessError, OSError) as e:
        print(f"Preflight failed: {describe(e)}", file=sys.stderr)
        return 1

    docs = prod and not args.skip_docs_image
    upload_cmd = ["twine", "upload", "--repository", repository, "dist/*"]
    plan = []
    if prod:
        plan.append(["git", "tag", "-a", tag, "-m", f"Version {version}"])
    plan.append(upload_cmd)
    if prod:
        plan.append(["git", "push", "origin", tag])
    if docs:
        plan.append(["make", "docs-publish-image"])

    print(f"Version:     {version}")
    print(f"Tag:         {tag if prod else '(none: Test PyPI)'}")
    print(f"Target:      {repository}")
    print(f"Docs image:  {DOCS_IMAGE}:{version}" if docs else "Docs image:  (skipped)")

    confirm_needed = not args.dry_run and not args.yes
    if (
        confirm_needed
        and ask(f"Type the version ({version}) to confirm: ").strip() != version
    ):
        print("Confirmation did not match; aborting.", file=sys.stderr)
        return 1

    try:
        runner(["make", "clean", "build"])
        runner(["twine", "check", "dist/*"])
    except (subprocess.CalledProcessError, OSError) as e:
        # OSError: a tool such as twine is not installed
        print(f"Build failed: {describe(e)}", file=sys.stderr)
        return 1

    if args.dry_run:
        print("Dry run: would now execute:")
        for cmd in plan:
            print("  " + " ".join(cmd))
        return 0

    failures = (subprocess.CalledProcessError, OSError)
    docs_hint = "; then publish the docs with: make docs-publish-image" if docs else ""

    if prod:
        try:
            runner(plan[0])
        except KeyboardInterrupt:
            # The tag may have been created just before the interrupt. Local
            # check only (no network), and best-effort: always re-raise.
            try:
                if runner(["git", "tag", "-l", tag]).strip():
                    _delete_tag(runner, tag)
            except Exception:
                pass
            raise
        except failures as e:
            print(
                f"Creating tag {tag} failed: {describe(e)}\nNothing was uploaded.",
                file=sys.stderr,
            )
            return 1

    try:
        runner(upload_cmd)
    except BaseException as e:
        if isinstance(e, KeyboardInterrupt):
            tag_note = (
                f"The local tag {tag} was removed. If PyPI already has "
                f"version {version}, re-create and push the tag with: "
                f'git tag -a {tag} -m "Version {version}" && '
                f"git push origin {tag}"
                if prod
                else ""
            )
            print(
                "Interrupted: the upload may have completed. Check PyPI "
                f"({index_url}) before re-running the release. {tag_note}".rstrip(),
                file=sys.stderr,
            )
        # Includes KeyboardInterrupt: never leave a tag for an unpublished version
        if prod:
            _delete_tag(runner, tag)
        if not isinstance(e, failures):
            raise
        print(f"Upload failed: {describe(e)}", file=sys.stderr)
        return 1

    if prod:
        try:
            runner(["git", "push", "origin", tag])
        except failures as e:
            print(
                f"Pushing tag {tag} failed: {describe(e)}\n"
                f"Version {version} is already published on {repository}. "
                f"Push the tag with: git push origin {tag}{docs_hint}",
                file=sys.stderr,
            )
            return 1

    if docs:
        try:
            runner(["make", "docs-publish-image"])
        except failures as e:
            print(
                f"Publishing the docs image failed: {describe(e)}\n"
                f"Version {version} is already published and tag {tag} pushed. "
                "Retry with: make docs-publish-image",
                file=sys.stderr,
            )
            return 1

    if docs:
        print(f"Notify the operations team that docs v{version} are available")
    return 0


def _delete_tag(run: Runner, tag: str) -> None:
    """Best-effort removal of the local release tag."""
    try:
        run(["git", "tag", "-d", tag])
    except (subprocess.CalledProcessError, OSError) as e:
        print(
            f"Could not delete local tag {tag} ({describe(e)}); "
            f"delete it with: git tag -d {tag}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    sys.exit(main())
