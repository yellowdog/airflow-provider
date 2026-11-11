from unittest.mock import MagicMock

import pytest
from yellowdog_client.model import (
    ComputeRequirement,
    ComputeRequirementStatus,
    WorkerPoolStatus,
    WorkerPoolSummary,
    WorkRequirementStatus,
    WorkRequirementSummary,
)
from yellowdog_client.model.exceptions import InvalidRequestException
from yellowdog_client.platform_client import PlatformClient

from tests.system.cleanup import PROVISIONED_TYPE, sweep

NS = "ns"
FRAG = "systest-1"


def wr(id: str, name: str, status: WorkRequirementStatus) -> WorkRequirementSummary:
    return WorkRequirementSummary(id=id, name=name, namespace=NS, status=status)


def wp(
    id: str, name: str, status: WorkerPoolStatus, type: str = PROVISIONED_TYPE
) -> WorkerPoolSummary:
    return WorkerPoolSummary(id=id, name=name, namespace=NS, status=status, type=type)


def cr(id: str, name: str, status: ComputeRequirementStatus) -> ComputeRequirement:
    c = ComputeRequirement(namespace=NS, name=name, provisionStrategy=MagicMock())
    c.id = id
    c.status = status
    return c


def make_client(
    wrs: list[WorkRequirementSummary],
    wps: list[WorkerPoolSummary],
    crs: list[ComputeRequirement],
) -> MagicMock:
    client = MagicMock(spec=PlatformClient)
    client.work_client = MagicMock()
    client.worker_pool_client = MagicMock()
    client.compute_client = MagicMock()
    work, pools, compute = (
        client.work_client,
        client.worker_pool_client,
        client.compute_client,
    )
    work.get_work_requirements.return_value.list_all.return_value = wrs
    pools.get_worker_pools.return_value.list_all.return_value = wps
    compute.get_compute_requirements.return_value.list_all.return_value = crs
    return client


def test_sweep_matches_fragment_only() -> None:
    client = make_client(
        [
            wr("w1", f"{FRAG}-a", WorkRequirementStatus.RUNNING),
            wr("w2", "other", WorkRequirementStatus.RUNNING),
        ],
        [
            wp("p1", f"x-{FRAG}", WorkerPoolStatus.IDLE),
            wp("p2", "other", WorkerPoolStatus.IDLE),
        ],
        [
            cr("c1", f"{FRAG}", ComputeRequirementStatus.RUNNING),
            cr("c2", "other", ComputeRequirementStatus.RUNNING),
        ],
    )
    report = sweep(client, NS, FRAG, MagicMock())
    assert report.cancelled_work_requirements == ["w1"]
    assert report.shut_down_worker_pools == ["p1"]
    assert report.terminated_compute_requirements == ["c1"]
    client.work_client.cancel_work_requirement_by_id.assert_called_once_with(
        "w1", abort=True
    )
    client.worker_pool_client.shutdown_worker_pool_by_id.assert_called_once_with("p1")
    client.compute_client.terminate_compute_requirement_by_id.assert_called_once_with(
        "c1"
    )


def test_sweep_skips_terminal_states() -> None:
    client = make_client(
        [
            wr("w1", FRAG, WorkRequirementStatus.COMPLETED),
            wr("w2", FRAG, WorkRequirementStatus.CANCELLED),
        ],
        [
            wp("p1", FRAG, WorkerPoolStatus.TERMINATED),
            wp("p2", FRAG, WorkerPoolStatus.SHUTDOWN),
            wp("p3", FRAG, WorkerPoolStatus.IDLE, type="configured"),
        ],
        [
            cr("c1", FRAG, ComputeRequirementStatus.TERMINATED),
            cr("c2", FRAG, ComputeRequirementStatus.TERMINATING),
        ],
    )
    report = sweep(client, NS, FRAG, MagicMock())
    assert report.cancelled_work_requirements == []
    assert report.shut_down_worker_pools == []
    assert report.terminated_compute_requirements == []
    client.work_client.cancel_work_requirement_by_id.assert_not_called()
    client.worker_pool_client.shutdown_worker_pool_by_id.assert_not_called()
    client.compute_client.terminate_compute_requirement_by_id.assert_not_called()


def test_sweep_continues_after_item_error() -> None:
    client = make_client(
        [
            wr("w1", FRAG, WorkRequirementStatus.RUNNING),
            wr("w2", FRAG, WorkRequirementStatus.RUNNING),
        ],
        [wp("p1", FRAG, WorkerPoolStatus.IDLE)],
        [cr("c1", FRAG, ComputeRequirementStatus.RUNNING)],
    )
    client.work_client.cancel_work_requirement_by_id.side_effect = [
        RuntimeError("boom"),
        None,
    ]
    client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = RuntimeError("x")
    log = MagicMock()
    report = sweep(client, NS, FRAG, log)
    assert report.cancelled_work_requirements == ["w2"]
    assert report.shut_down_worker_pools == []
    assert report.terminated_compute_requirements == ["c1"]
    assert log.warning.call_count == 2


def test_sweep_treats_invalid_transition_as_already_finishing() -> None:
    client = make_client(
        [wr("w1", FRAG, WorkRequirementStatus.RUNNING)],
        [wp("p1", FRAG, WorkerPoolStatus.IDLE)],
        [cr("c1", FRAG, ComputeRequirementStatus.RUNNING)],
    )
    error = InvalidRequestException("Invalid Transition from TERMINATING")
    client.work_client.cancel_work_requirement_by_id.side_effect = error
    client.worker_pool_client.shutdown_worker_pool_by_id.side_effect = error
    client.compute_client.terminate_compute_requirement_by_id.side_effect = error
    log = MagicMock()
    report = sweep(client, NS, FRAG, log)
    log.warning.assert_not_called()
    assert log.info.call_count == 3
    assert all("already finishing" in c.args[0] for c in log.info.call_args_list)
    # the resource's current status is shown so a misclassification is visible
    messages = [c.args[0] for c in log.info.call_args_list]
    assert "RUNNING" in messages[0]
    assert "IDLE" in messages[1]
    assert "RUNNING" in messages[2]
    assert report.cancelled_work_requirements == []
    assert report.shut_down_worker_pools == []
    assert report.terminated_compute_requirements == []


def test_sweep_report_shown_in_terminal_summary(pytester: pytest.Pytester) -> None:
    pytester.makeconftest(
        "from tests.system.conftest import (\n"
        "    pytest_terminal_summary as pytest_terminal_summary,\n"
        ")\n"
    )
    pytester.makepyfile(
        test_sweep=(
            "from tests.system.conftest import record_sweep\n"
            "def test_x(pytestconfig):\n"
            "    record_sweep(pytestconfig, 'sweep report line')\n"
        )
    )
    result = pytester.runpytest()  # output capture on (no -s)
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*sweep report line*"])


def test_sweep_item_messages_shown_in_terminal_summary(
    pytester: pytest.Pytester,
) -> None:
    pytester.makeconftest(
        "from tests.system.conftest import (\n"
        "    pytest_terminal_summary as pytest_terminal_summary,\n"
        ")\n"
    )
    pytester.makepyfile(
        test_sweep=(
            "from tests.system.conftest import SweepLog\n"
            "def test_x(pytestconfig):\n"
            "    log = SweepLog(pytestconfig)\n"
            "    log.info('info-item-line')\n"
            "    log.warning('warn-item-line')\n"
        )
    )
    result = pytester.runpytest()  # output capture on (no -s)
    result.assert_outcomes(passed=1)
    result.stdout.fnmatch_lines(["*info-item-line*", "*WARNING*warn-item-line*"])


def test_system_tests_skipped_without_config(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The repo's own default config may exist, so point at a missing file
    monkeypatch.setenv("YD_SYSTEM_TEST_CONFIG", str(pytester.path / "missing.toml"))
    pytester.makeconftest(
        "from tests.system.conftest import (\n"
        "    pytest_collection_modifyitems as pytest_collection_modifyitems,\n"
        ")\n"
    )
    pytester.makepyfile(
        **{
            "tests/system/test_dummy": (
                "import pytest\n"
                "@pytest.mark.system\n"
                "def test_dummy():\n"
                "    raise AssertionError('must not run')\n"
            )
        }
    )
    result = pytester.runpytest("-m", "system", "-rs")
    result.assert_outcomes(skipped=1)
    result.stdout.fnmatch_lines(["*system-test config not found at*"])
