#!/usr/bin/env python3
"""Local release script for the YellowDog Airflow provider (stdlib only).

Run from ``next-version``. Sets the release version and dates the changelog,
runs preflight checks, builds, commits, fast-forwards ``main``, tags, uploads to
PyPI (or Test PyPI), pushes the tag, publishes the documentation image, and
starts the next ``.dev0`` version on ``next-version``. See RELEASING.md.
"""

import argparse
import datetime
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
DEV_BRANCH = "next-version"
RELEASE_BRANCH = "main"
VERSION_FILE = Path("yellowdog_provider") / "__init__.py"
CHANGELOG = Path("docs") / "changelog.rst"
_FINAL_VERSION = re.compile(r"\d+\.\d+\.\d+")

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


def write_version(init_path: Path, version: str) -> None:
    """Set ``__version__`` in ``init_path``."""
    text = init_path.read_text()
    new, count = re.subn(
        r'^(__version__\s*=\s*)"[^"]+"', rf'\g<1>"{version}"', text, flags=re.MULTILINE
    )
    if count != 1:
        raise ReleaseError(f"No __version__ found in {init_path}")
    init_path.write_text(new)


def release_version(current: str, override: str | None) -> str:
    """
    The version to release: ``override`` if given, else ``current`` without a
    ``.devN`` suffix (``2.0.0.dev0`` -> ``2.0.0``; a final ``current`` is kept,
    so a release can be re-run after a failure).
    """
    version = override or re.sub(r"\.dev\d+$", "", current)
    if not _FINAL_VERSION.fullmatch(version):
        raise ReleaseError(
            f"Cannot release {version!r}: expected a final version such as 2.0.0 "
            f"(from __version__ {current!r}; or pass --version)"
        )
    return version


def next_dev_version(version: str, override: str | None) -> str:
    """``override`` (or the next patch version) with ``.dev0``."""
    if override is not None:
        if not _FINAL_VERSION.fullmatch(override):
            raise ReleaseError(
                f"--next-version {override!r}: expected a version such as 2.1.0"
            )
        return f"{override}.dev0"
    major, minor, patch = (int(p) for p in version.split("."))
    return f"{major}.{minor}.{patch + 1}.dev0"


def _heading(version: str, label: str) -> str:
    title = f"{version} ({label})"
    return f"{title}\n{'-' * len(title)}\n"


def date_changelog(changelog: Path, version: str, date: str) -> bool:
    """
    Turn the heading ``<version> (unreleased)`` into ``<version> (<date>)``
    (underline included). Return False, changing nothing, if the heading is
    already dated (a re-run).
    """
    if not changelog.exists():
        raise ReleaseError(f"Changelog not found: {changelog}")
    text = changelog.read_text()
    dated = rf"^{re.escape(version)} \(\d{{4}}-\d{{2}}-\d{{2}}\)\s*$"
    if re.search(dated, text, re.MULTILINE):
        return False
    pattern = rf"^{re.escape(version)} \(unreleased\)\n-+\n"
    new, count = re.subn(
        pattern, _heading(version, date), text, count=1, flags=re.MULTILINE
    )
    if count != 1:
        raise ReleaseError(
            f"{changelog} needs a heading '{version} (unreleased)' to date"
        )
    changelog.write_text(new)
    return True


def add_changelog_heading(changelog: Path, version: str) -> None:
    """Insert ``<version> (unreleased)`` above the first version heading."""
    text = changelog.read_text()
    match = re.search(r"^\d+\.\d+\.\d+ \(", text, re.MULTILINE)
    if match is None:
        raise ReleaseError(f"No version heading found in {changelog}")
    at = match.start()
    changelog.write_text(text[:at] + _heading(version, "unreleased") + "\n" + text[at:])


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


def check_git_state(run: Runner, branch: str = DEV_BRANCH) -> None:
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


def check_fast_forward(run: Runner) -> None:
    """``main`` (local, if it exists, and on origin) must be an ancestor of HEAD."""
    for ref in (f"origin/{RELEASE_BRANCH}", RELEASE_BRANCH):
        if (
            ref == RELEASE_BRANCH
            and not run(["git", "branch", "--list", RELEASE_BRANCH]).strip()
        ):
            continue
        try:
            run(["git", "merge-base", "--is-ancestor", ref, "HEAD"])
        except subprocess.CalledProcessError:
            raise ReleaseError(
                f"{ref} has commits that are not on {DEV_BRANCH}; "
                f"{RELEASE_BRANCH} cannot be fast-forwarded"
            ) from None


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
    today: Callable[[], datetime.date] = datetime.date.today,
) -> int:
    parser = argparse.ArgumentParser(description="Release the YellowDog provider")
    parser.add_argument(
        "--test-pypi",
        action="store_true",
        help="Upload to Test PyPI (no commit, push, tag or docs image)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preflight and build only (no commit, push, tag or upload)",
    )
    parser.add_argument(
        "--version",
        dest="release_version",
        help="Version to release (default: __version__ without .devN)",
    )
    parser.add_argument(
        "--next-version",
        help="Next version on next-version, without .dev0 (default: next patch)",
    )
    parser.add_argument("--skip-docs-image", action="store_true")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation")
    args = parser.parse_args(argv)

    runner = run or default_runner(root)
    prod = not args.test_pypi
    repository = PROD_REPOSITORY if prod else TEST_REPOSITORY
    index_url = PYPI_URL if prod else TEST_PYPI_URL
    init_path = root / VERSION_FILE
    changelog = root / CHANGELOG

    try:
        # Preflight: git state first so a wrong branch stops everything early.
        check_git_state(runner)
        check_fast_forward(runner)
        current = read_version(init_path)
        version = release_version(current, args.release_version)
        tag = f"v{version}"
        check_version_releasable(version)
        next_dev = next_dev_version(version, args.next_version)
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
    files = [str(VERSION_FILE), str(CHANGELOG)]
    plan: list[list[str]] = []
    if prod:
        plan += [
            ["git", "commit", "-m", f"release: {version}", "--", *files],
            ["git", "push", "origin", DEV_BRANCH],
            ["git", "push", "origin", f"HEAD:{RELEASE_BRANCH}"],
            ["git", "tag", "-a", tag, "-m", f"Version {version}"],
        ]
    plan.append(upload_cmd)
    if prod:
        plan.append(["git", "push", "origin", tag])
    if docs:
        plan.append(["make", "docs-publish-image"])
    if prod:
        plan += [
            ["git", "commit", "-m", f"chore: start {next_dev}", "--", *files],
            ["git", "push", "origin", DEV_BRANCH],
        ]

    print(f"Version:     {version} (from {current})")
    print(f"Tag:         {tag if prod else '(none: Test PyPI)'}")
    print(f"Target:      {repository}")
    print(f"Docs image:  {DOCS_IMAGE}:{version}" if docs else "Docs image:  (skipped)")
    if prod:
        print(f"Branches:    {DEV_BRANCH} and {RELEASE_BRANCH} (fast-forward)")
        print(f"Then:        {DEV_BRANCH} at {next_dev}")

    confirm_needed = not args.dry_run and not args.yes
    if (
        confirm_needed
        and ask(f"Type the version ({version}) to confirm: ").strip() != version
    ):
        print("Confirmation did not match; aborting.", file=sys.stderr)
        return 1

    # Set the version and date the changelog (no-ops on a re-run), then build
    try:
        changed = current != version
        if changed:
            write_version(init_path, version)
        changed = date_changelog(changelog, version, today().isoformat()) or changed
        check_changelog(changelog, version)
        runner(["make", "clean", "build"])
        runner(["twine", "check", "dist/*"])
    except BaseException as e:
        # Includes KeyboardInterrupt: never leave the files half-released
        _restore(runner, files)
        # OSError: a tool such as twine is not installed
        if not isinstance(e, (ReleaseError, subprocess.CalledProcessError, OSError)):
            raise
        print(f"Build failed: {describe(e)}", file=sys.stderr)
        return 1

    if args.dry_run:
        _restore(runner, files)
        print("Dry run: would now execute:")
        for cmd in plan:
            print("  " + " ".join(cmd))
        return 0

    failures = (subprocess.CalledProcessError, OSError)

    if prod:
        committed = False
        try:
            if changed:
                runner(plan[0])
                committed = True
            runner(["git", "push", "origin", DEV_BRANCH])
            runner(["git", "push", "origin", f"HEAD:{RELEASE_BRANCH}"])
            # Keep the local main in step (a fast-forward, checked in preflight)
            if runner(["git", "branch", "--list", RELEASE_BRANCH]).strip():
                runner(["git", "branch", "-f", RELEASE_BRANCH, "HEAD"])
        except failures as e:
            if not committed and changed:
                _restore(runner, files)
                hint = "Fix the cause and re-run."
            else:
                hint = (
                    f"The release commit is on the local {DEV_BRANCH}: push it "
                    f"with 'git push origin {DEV_BRANCH}', then re-run."
                )
            print(
                f"Committing or pushing the release failed: {describe(e)}\n"
                f"Nothing was tagged or uploaded. {hint}",
                file=sys.stderr,
            )
            return 1

    docs_hint = "; then publish the docs with: make docs-publish-image" if docs else ""

    if prod:
        try:
            runner(["git", "tag", "-a", tag, "-m", f"Version {version}"])
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
        else:
            _restore(runner, files)
        if not isinstance(e, failures):
            raise
        print(f"Upload failed: {describe(e)}", file=sys.stderr)
        return 1

    if not prod:
        _restore(runner, files)
        return 0

    try:
        runner(["git", "push", "origin", tag])
    except failures as e:
        print(
            f"Pushing tag {tag} failed: {describe(e)}\n"
            f"Version {version} is already published on {repository}. "
            f"Push the tag with: git push origin {tag}{docs_hint}; then start "
            f"{next_dev} on {DEV_BRANCH} (see RELEASING.md)",
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
                "Retry with: make docs-publish-image; then start "
                f"{next_dev} on {DEV_BRANCH} (see RELEASING.md)",
                file=sys.stderr,
            )
            return 1

    try:
        write_version(init_path, next_dev)
        add_changelog_heading(changelog, next_dev.removesuffix(".dev0"))
        runner(["git", "commit", "-m", f"chore: start {next_dev}", "--", *files])
        runner(["git", "push", "origin", DEV_BRANCH])
    except (ReleaseError, *failures) as e:
        print(
            f"Starting {next_dev} on {DEV_BRANCH} failed: {describe(e)}\n"
            f"Version {version} is released. Set __version__ to {next_dev}, add "
            "an unreleased changelog heading, then commit and push "
            f"{DEV_BRANCH} by hand.",
            file=sys.stderr,
        )
        return 1

    print(f"Released {version}; {DEV_BRANCH} is now at {next_dev}.")
    if docs:
        print(f"Notify the operations team that docs v{version} are available")
    return 0


def _restore(run: Runner, files: list[str]) -> None:
    """Best-effort restore of the version and changelog files to HEAD."""
    try:
        run(["git", "checkout", "--", *files])
    except (subprocess.CalledProcessError, OSError) as e:
        print(
            f"Could not restore {', '.join(files)} ({describe(e)}); restore "
            f"them with: git checkout -- {' '.join(files)}",
            file=sys.stderr,
        )


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
