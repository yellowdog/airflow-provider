"""Tests for bin/release.py. No network access; the only real subprocesses are
the Python interpreter (to test the default runner)."""

import importlib.util
import subprocess
import sys
import urllib.error
from pathlib import Path
from typing import Any

import pytest

_SPEC = importlib.util.spec_from_file_location(
    "release", Path(__file__).resolve().parents[2] / "bin" / "release.py"
)
assert _SPEC is not None and _SPEC.loader is not None
release: Any = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(release)


class FakeRunner:
    def __init__(
        self,
        branch: str = "main",
        head: str = "abc",
        origin: str = "abc",
        porcelain: str = "",
        fail_on: str | None = None,
        tags: str = "",
        error: BaseException | None = None,
    ) -> None:
        self.branch, self.head, self.origin = branch, head, origin
        self.porcelain, self.fail_on, self.tags = porcelain, fail_on, tags
        self.error = error
        self.commands: list[list[str]] = []

    def __call__(self, cmd: list[str]) -> str:
        self.commands.append(cmd)
        text = " ".join(cmd)
        if self.fail_on and self.fail_on in text:
            if self.error is not None:
                raise self.error
            raise subprocess.CalledProcessError(1, cmd, "", "stderr-detail")
        if text == "git rev-parse --abbrev-ref HEAD":
            return self.branch + "\n"
        if text == "git rev-parse HEAD":
            return self.head + "\n"
        if text == "git rev-parse origin/main":
            return self.origin + "\n"
        if text.startswith("git status --porcelain"):
            return self.porcelain
        if text.startswith("git tag -l") or text.startswith("git ls-remote"):
            return self.tags
        return ""

    def joined(self) -> list[str]:
        return [" ".join(c) for c in self.commands]


def _write_project(tmp_path: Path, version: str = "2.0.0") -> Path:
    pkg = tmp_path / "yellowdog_provider"
    pkg.mkdir()
    (pkg / "__init__.py").write_text(f'__version__ = "{version}"\n')
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "changelog.rst").write_text(f"{version} (2026-11-10)\n------\n\n- x\n")
    return tmp_path


def _patch_pypi(monkeypatch: pytest.MonkeyPatch, present: bool = False) -> None:
    monkeypatch.setattr(release, "version_on_pypi", lambda v, url: present)


def test_read_version(tmp_path: Path) -> None:
    init = tmp_path / "__init__.py"
    init.write_text('"""doc"""\n__version__ = "2.0.0"\n')
    assert release.read_version(init) == "2.0.0"


def test_read_version_missing(tmp_path: Path) -> None:
    init = tmp_path / "__init__.py"
    init.write_text("x = 1\n")
    with pytest.raises(release.ReleaseError):
        release.read_version(init)


@pytest.mark.parametrize("version", ["2.0.0.dev0", "2.0.0rc1", "2.0.0+local"])
def test_dev_version_rejected(version: str) -> None:
    with pytest.raises(release.ReleaseError):
        release.check_version_releasable(version)


def test_final_version_accepted() -> None:
    release.check_version_releasable("2.0.0")


def test_changelog_heading_required(tmp_path: Path) -> None:
    changelog = tmp_path / "changelog.rst"
    with pytest.raises(release.ReleaseError):
        release.check_changelog(changelog, "2.0.0")  # missing file
    changelog.write_text("2.0.0 (unreleased)\n------\n")
    with pytest.raises(release.ReleaseError):
        release.check_changelog(changelog, "2.0.0")
    changelog.write_text("2.0.0 (2026-11-10)\n------\n")
    release.check_changelog(changelog, "2.0.0")


def test_git_state_ok() -> None:
    release.check_git_state(FakeRunner())


def test_git_state_wrong_branch() -> None:
    with pytest.raises(release.ReleaseError, match="branch"):
        release.check_git_state(FakeRunner(branch="next-version"))


def test_git_state_behind_origin() -> None:
    with pytest.raises(release.ReleaseError, match="origin"):
        release.check_git_state(FakeRunner(origin="def"))


def test_git_state_ignores_untracked() -> None:
    run = FakeRunner(porcelain="")  # --untracked-files=no hides dag-test.py
    release.check_git_state(run)
    assert "git status --porcelain --untracked-files=no" in run.joined()


def test_git_state_tracked_change_blocks() -> None:
    with pytest.raises(release.ReleaseError, match="uncommitted"):
        release.check_git_state(FakeRunner(porcelain=" M Makefile\n"))


def test_tag_exists() -> None:
    assert not release.tag_exists(FakeRunner(), "v2.0.0")
    assert release.tag_exists(FakeRunner(tags="v2.0.0\n"), "v2.0.0")


class _Resp:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *a: object) -> None:
        return None


def test_version_on_pypi(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(release.urllib.request, "urlopen", lambda *a, **k: _Resp(200))
    assert release.version_on_pypi("2.0.0", release.PYPI_URL) is True

    def raise_404(*a: object, **k: object) -> None:
        raise urllib.error.HTTPError("u", 404, "nf", None, None)  # type: ignore[arg-type]

    monkeypatch.setattr(release.urllib.request, "urlopen", raise_404)
    assert release.version_on_pypi("2.0.0", release.PYPI_URL) is False


def test_dry_run_does_not_tag_or_upload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner()
    assert release.main(["--dry-run"], run=run, root=root) == 0
    joined = run.joined()
    assert "make check" in joined
    assert "make clean build" in joined
    for text in joined:
        assert "tag -a" not in text and "upload" not in text and "push" not in text


def test_dry_run_fails_on_wrong_branch_before_side_effects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner(branch="provider-2.0-phase-b")
    assert release.main(["--dry-run"], run=run, root=root) != 0
    assert run.joined() == ["git rev-parse --abbrev-ref HEAD"]


def test_full_release_order(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner()
    assert release.main(["--yes"], run=run, root=root) == 0
    joined = run.joined()
    steps = [
        "make check",
        "make clean build",
        "twine check dist/*",
        "git tag -a v2.0.0 -m Version 2.0.0",
        "twine upload --repository yellowdog-airflow-provider dist/*",
        "git push origin v2.0.0",
        "make docs-publish-image",
    ]
    idx = [joined.index(s) for s in steps]
    assert idx == sorted(idx)


def test_confirmation_must_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner()
    assert release.main([], run=run, root=root, ask=lambda p: "nope") == 1
    assert not any("upload" in c for c in run.joined())


def test_upload_failure_deletes_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner(fail_on="twine upload")
    assert release.main(["--yes"], run=run, root=root) == 1
    joined = run.joined()
    assert "git tag -d v2.0.0" in joined
    assert not any("push" in c for c in joined)
    assert "make docs-publish-image" not in joined


def test_test_pypi_skips_tag_push_and_docs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner()
    assert release.main(["--test-pypi", "--yes"], run=run, root=root) == 0
    joined = run.joined()
    assert "twine upload --repository yellowdog-testpypi dist/*" in joined
    for text in joined:
        assert "tag -a" not in text and "push" not in text
        assert "docs-publish-image" not in text


def test_skip_docs_image(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    run = FakeRunner()
    assert release.main(["--yes", "--skip-docs-image"], run=run, root=root) == 0
    assert "make docs-publish-image" not in run.joined()
    assert "git push origin v2.0.0" in run.joined()


def test_existing_tag_or_pypi_version_blocks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    _patch_pypi(monkeypatch)
    assert release.main(["--yes"], run=FakeRunner(tags="v2.0.0"), root=root) == 1
    _patch_pypi(monkeypatch, present=True)
    assert release.main(["--yes"], run=FakeRunner(), root=root) == 1


def _release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    run: FakeRunner,
    argv: list[str] | None = None,
    version: str = "2.0.0",
) -> int:
    root = _write_project(tmp_path, version)
    _patch_pypi(monkeypatch)
    return release.main(["--yes"] if argv is None else argv, run=run, root=root)


def test_main_rejects_dev_version(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner()
    assert _release(tmp_path, monkeypatch, run, version="2.0.0.dev0") == 1
    assert "dev/pre-release" in capsys.readouterr().err
    assert "make check" not in run.joined()


def test_main_requires_changelog_heading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _write_project(tmp_path)
    (root / "docs" / "changelog.rst").write_text("2.0.0 (unreleased)\n------\n")
    _patch_pypi(monkeypatch)
    run = FakeRunner()
    assert release.main(["--yes"], run=run, root=root) == 1
    assert "2.0.0 (YYYY-MM-DD)" in capsys.readouterr().err
    assert "make check" not in run.joined()


def test_main_check_failure_stops_before_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="make check")
    assert _release(tmp_path, monkeypatch, run) == 1
    assert "stderr-detail" in capsys.readouterr().err
    assert not any("build" in c or "twine" in c for c in run.joined())


def test_main_rejects_head_ahead_of_origin(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = FakeRunner(head="local-commit", origin="abc")
    assert _release(tmp_path, monkeypatch, run) == 1
    assert "make check" not in run.joined()


def test_main_test_pypi_checks_test_pypi_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _write_project(tmp_path)
    urls: list[str] = []

    def on_pypi(version: str, url: str) -> bool:
        urls.append(url)
        return False

    monkeypatch.setattr(release, "version_on_pypi", on_pypi)
    assert release.main(["--test-pypi", "--yes"], run=FakeRunner(), root=root) == 0
    assert urls == ["https://test.pypi.org/pypi/yellowdog-airflow-provider"]


def test_build_tool_missing_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="twine check", error=FileNotFoundError("twine"))
    assert _release(tmp_path, monkeypatch, run) == 1
    assert "Build failed" in capsys.readouterr().err
    assert not any("tag" in c for c in run.joined()[-2:])


def test_tag_create_failure_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="git tag -a")
    assert _release(tmp_path, monkeypatch, run) == 1
    err = capsys.readouterr().err
    assert "Creating tag v2.0.0 failed" in err and "stderr-detail" in err
    assert not any("upload" in c for c in run.joined())


@pytest.mark.parametrize("error", [OSError("no twine"), KeyboardInterrupt()])
def test_upload_interrupted_deletes_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: BaseException
) -> None:
    run = FakeRunner(fail_on="twine upload", error=error)
    if isinstance(error, KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            _release(tmp_path, monkeypatch, run)
    else:
        assert _release(tmp_path, monkeypatch, run) == 1
    joined = run.joined()
    assert joined[-1] == "git tag -d v2.0.0"
    assert not any("push" in c for c in joined)


def test_upload_failure_message_includes_stderr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="twine upload")
    assert _release(tmp_path, monkeypatch, run) == 1
    err = capsys.readouterr().err
    assert "Upload failed" in err and "stderr-detail" in err


def test_push_failure_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="git push")
    assert _release(tmp_path, monkeypatch, run) == 1
    err = capsys.readouterr().err
    assert "Pushing tag v2.0.0 failed" in err
    assert "already published" in err
    assert "git push origin v2.0.0" in err
    assert "make docs-publish-image" in err
    assert "make docs-publish-image" not in run.joined()
    assert "git tag -d v2.0.0" not in run.joined()


def test_docs_image_failure_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="docs-publish-image")
    assert _release(tmp_path, monkeypatch, run) == 1
    err = capsys.readouterr().err
    assert "docs image failed" in err
    assert "already published" in err
    assert "make docs-publish-image" in err


def test_default_runner_captures_stderr_separately(tmp_path: Path) -> None:
    run = release.default_runner(tmp_path)
    code = "import sys; print('out'); print('err', file=sys.stderr)"
    assert run([sys.executable, "-c", code]) == "out\n"
    with pytest.raises(subprocess.CalledProcessError) as exc:
        run([sys.executable, "-c", code + "; sys.exit(3)"])
    assert exc.value.output == "out\n"
    assert exc.value.stderr == "err\n"


def test_tag_creation_interrupted_deletes_tag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class TagRunner(FakeRunner):
        def __call__(self, cmd: list[str]) -> str:
            text = " ".join(cmd)
            if text.startswith("git tag -a"):
                self.commands.append(cmd)
                self.tags = "v2.0.0"  # the tag was created before the interrupt
                raise KeyboardInterrupt
            return super().__call__(cmd)

    run = TagRunner()
    with pytest.raises(KeyboardInterrupt):
        _release(tmp_path, monkeypatch, run)
    joined = run.joined()
    assert joined[-1] == "git tag -d v2.0.0"
    assert not any("twine upload" in c for c in joined)


def test_tag_creation_interrupted_without_tag_does_not_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = FakeRunner(fail_on="git tag -a", error=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        _release(tmp_path, monkeypatch, run)
    assert not any("git tag -d" in c for c in run.joined())


def test_upload_interrupted_warns_it_may_have_completed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run = FakeRunner(fail_on="twine upload", error=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        _release(tmp_path, monkeypatch, run)
    err = capsys.readouterr().err
    assert "may have completed" in err and "PyPI" in err
    assert "local tag v2.0.0 was removed" in err
    assert 'git tag -a v2.0.0 -m "Version 2.0.0" && git push origin v2.0.0' in err


def test_describe_omits_stderr_already_echoed(tmp_path: Path) -> None:
    run = release.default_runner(tmp_path)
    code = "import sys; print('err-' + 'detail', file=sys.stderr); sys.exit(1)"
    with pytest.raises(subprocess.CalledProcessError) as exc:
        run([sys.executable, "-c", code])
    assert exc.value.stderr == "err-detail\n"
    error: Any = exc.value
    assert isinstance(error, release.CommandError)
    assert error.stderr_echoed is True
    assert "err-detail" not in release.describe(exc.value)


def test_describe_includes_stderr_not_echoed(tmp_path: Path) -> None:
    run = release.default_runner(tmp_path)
    with pytest.raises(subprocess.CalledProcessError) as exc:
        run(["git", "no-such-subcommand"])  # git output is not echoed live
    error: Any = exc.value
    assert isinstance(error, release.CommandError)
    assert error.stderr_echoed is False
    assert exc.value.stderr.strip() in release.describe(exc.value)


def test_describe_includes_stderr_of_plain_called_process_error() -> None:
    e = subprocess.CalledProcessError(1, ["x"], "", "plain-detail\n")
    assert "plain-detail" in release.describe(e)


def test_tag_interrupt_cleanup_is_local_and_best_effort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class BrokenCleanup(FakeRunner):
        def __call__(self, cmd: list[str]) -> str:
            text = " ".join(cmd)
            if text.startswith("git tag -a"):
                self.commands.append(cmd)
                raise KeyboardInterrupt
            if text.startswith("git tag -l") and len(self.commands) > 6:
                self.commands.append(cmd)
                raise subprocess.CalledProcessError(1, cmd, "", "boom")
            return super().__call__(cmd)

    run = BrokenCleanup()
    with pytest.raises(KeyboardInterrupt):
        _release(tmp_path, monkeypatch, run)
    joined = run.joined()
    assert joined[-1] == "git tag -l v2.0.0"
    assert not any("ls-remote" in c for c in joined[joined.index("make clean build") :])
