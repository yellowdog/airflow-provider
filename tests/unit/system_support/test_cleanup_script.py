"""Tests for bin/system-test-cleanup.py's handling of a bad config."""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

_PATH = Path(__file__).resolve().parents[3] / "bin" / "system-test-cleanup.py"
_SPEC = importlib.util.spec_from_file_location("system_test_cleanup", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
script: Any = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(script)


@pytest.mark.parametrize(
    "content", [None, "not = [valid toml", '[platform]\nkey_id = "k"\n']
)
def test_bad_config_exits_2_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    content: str | None,
) -> None:
    path = tmp_path / "config.toml"
    if content is not None:
        path.write_text(content)
    monkeypatch.setenv("YD_SYSTEM_TEST_CONFIG", str(path))
    assert script.main(["--yes"]) == 2
    err = capsys.readouterr().err
    assert str(path) in err
    assert "Traceback" not in err
