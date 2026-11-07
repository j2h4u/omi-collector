from __future__ import annotations

import json
from pathlib import Path
from typing import Literal, cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcomeState,
    finalize_public_audio_v2,
    retire_acknowledged_public_audio_v2,
)
from test_public_audio_v2_recovery import _checkpoint, _options, _set_acknowledged, _source
from test_publication_fsm import _store


@pytest.mark.parametrize("entrypoint", ("publication", "maintenance"))
@pytest.mark.parametrize("invalid", ("malformed", "wrong_identity"))
def test_committed_retirement_resumes_before_invalid_new_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, entrypoint: str, invalid: str
) -> None:
    store = _store(tmp_path)
    ready = tmp_path / "ready"
    checkpoint = _checkpoint(tmp_path)
    with InternalAudioStore(store.state_db) as audio:
        first = finalize_public_audio_v2(audio, (_source(audio, "a" * 32, 10),), ready, _options()).published[0]
        second = finalize_public_audio_v2(
            audio, (_source(audio, "b" * 32, 20),), ready, _options(frontier=21)
        ).published[0]
    first_manifest = cast(dict[str, object], json.loads((first.path / "manifest.json").read_bytes()))
    _set_acknowledged(checkpoint, ((first.bundle_id, cast(str, first_manifest["audio_sha256"])),))
    original_remove = ready_bundles._remove_v2_artifact

    def leave_residue(path: Path, label: Literal["retired public audio v2 artifact", "v2 ready stage"]) -> None:
        if path != first.path:
            original_remove(path, label)

    with monkeypatch.context() as interrupted:
        interrupted.setattr(ready_bundles, "_remove_v2_artifact", leave_residue)
        retire_acknowledged_public_audio_v2(ready, store.state_db, checkpoint)

    assert first.path.exists()
    before = {path.name: path.read_bytes() for path in second.path.iterdir()}
    if invalid == "malformed":
        checkpoint.write_text("{", encoding="utf-8")
    else:
        _set_acknowledged(checkpoint, ((second.bundle_id, "0" * 64),))

    if entrypoint == "publication":
        outcome = store.recover_and_publish()
        assert outcome.state == ReadyOutcomeState.BLOCKED
    else:
        with pytest.raises(ReadyBundleError):
            store.retire_acknowledged_ready(should_defer=lambda: False)

    assert not first.path.exists()
    assert {path.name: path.read_bytes() for path in second.path.iterdir()} == before
    entry = store.state_db.get(StateKind.READY_PUBLICATION, second.bundle_id)
    assert entry is not None and entry["state"] == "ready"
