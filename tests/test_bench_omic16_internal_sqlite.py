import sys

import pytest
from scripts import bench_omic16_internal_sqlite


def test_invalid_arguments_exit_before_changing_process_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["bench_omic16_internal_sqlite", "--records", "0"])
    monkeypatch.setattr(bench_omic16_internal_sqlite.os, "nice", lambda _: pytest.fail("priority changed"))

    with pytest.raises(SystemExit) as error:
        bench_omic16_internal_sqlite.main()

    assert error.value.code == 2
