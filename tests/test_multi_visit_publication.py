from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from hashlib import sha256
from pathlib import Path
from struct import pack

from fakes import ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.application.collector import CollectionResult, TransferTimeouts
from omi_collector.capture.application.opportunistic_sync import run_opportunistic_collector
from omi_collector.capture.application.ring_transport import RingTransportDisconnectedError
from omi_collector.capture.application.session_lifecycle import ActivityEvent, OpportunisticOptions, RetryPolicy
from omi_collector.capture.domain.ring_protocol import (
    RECORD_SIZE,
    RingStatus,
    encode_advance_command,
    encode_read_command,
    encode_stop_command,
)
from omi_collector.config import CollectorConfig, ReadyConfig
from staging_test_factory import StagingStore, attempt_records


def _ogg_packets(data: bytes) -> tuple[bytes, ...]:
    packets: list[bytes] = []
    pending = bytearray()
    offset = 0
    while offset < len(data):
        assert data[offset : offset + 4] == b"OggS"
        count = data[offset + 26]
        lacing = data[offset + 27 : offset + 27 + count]
        cursor = offset + 27 + count
        for size in lacing:
            pending.extend(data[cursor : cursor + size])
            cursor += size
            if size < 255:
                packets.append(bytes(pending))
                pending.clear()
        offset = cursor
    assert not pending
    return tuple(packets)


def test_interrupted_visits_publish_one_complete_ordered_bundle_after_drain(tmp_path: Path) -> None:
    async def scenario() -> None:
        ready = tmp_path / "ready"
        ready.mkdir(mode=0o2750)
        ready.chmod(0o2750)
        draft = tmp_path / "draft"
        draft.mkdir()
        config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.12))
        bootstrap = StagingStore(tmp_path / "spool", draft, config=config)
        store = StagingStore.from_paths(
            bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config
        )
        records = tuple(
            pack(">I", sequence) + bytes((2, 8, sequence)) + bytes(RECORD_SIZE - 7) for sequence in range(10, 16)
        )

        def info(read: int) -> bytes:
            return b"\x02" + pack(">QQIQH", read, 16, 10000, 0, RECORD_SIZE)

        sessions = []
        for start, end in ((10, 12), (12, 14), (14, 16)):
            count = 16 - start
            notifications = (
                b"\x05" + start.to_bytes(8, "big") + count.to_bytes(4, "big"),
                b"\x03" + b"".join(records[start - 10 : end - 10]),
            )
            if end < 16:
                notifications += (RingTransportDisconnectedError("visit ended"),)
                steps = (
                    WriteStep(b"\x10", (info(10),)),
                    WriteStep(encode_read_command(start, count), notifications),
                    WriteStep(encode_stop_command()),
                )
            else:
                notifications += (b"\x04\x00" + (16).to_bytes(8, "big"),)
                steps = (
                    WriteStep(b"\x10", (info(10),)),
                    WriteStep(encode_read_command(start, count), notifications),
                    WriteStep(b"\x10", (info(10),)),
                    WriteStep(encode_advance_command(16), (b"\x01\x00",)),
                    WriteStep(b"\x10", (info(16),)),
                )
            sessions.append(ScriptedRingSession(RingStatus(0, 0, 0, 1), steps))

        @asynccontextmanager
        async def provider(_candidate: object | None) -> AsyncIterator[ScriptedRingSession]:
            yield sessions.pop(0)

        snapshots: list[tuple[bytes, ...]] = []

        def activity(event: ActivityEvent) -> None:
            if event.state == "away":
                attempts = store.pending_attempts()
                assert len(attempts) == 1
                snapshots.append(attempt_records(store, attempts[0].attempt_id))
                assert not tuple(ready.glob("*/manifest.json"))

        async def sleep(_delay: float) -> None:
            await asyncio.sleep(0)

        result = await run_opportunistic_collector(
            provider,
            store,
            OpportunisticOptions(
                TransferTimeouts(30, 600),
                RetryPolicy(backoff=(0.001,), batch_records=6, stop_after_drained=True),
                activity=activity,
                sleep=sleep,
                config=config,
            ),
            runtime=OpportunisticRuntime(),
        )
        assert isinstance(result, CollectionResult)
        assert snapshots == [records[:2], records[:4]]
        store = StagingStore.from_paths(
            bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config
        )
        outcome = await asyncio.to_thread(store.recover_and_publish)
        artifacts = tuple(ready.glob("*/manifest.json"))
        assert len(artifacts) == 1, outcome
        manifest = json.loads(artifacts[0].read_text())
        audio = (artifacts[0].parent / "audio.ogg").read_bytes()
        assert manifest["format"] == "omi-audio-v2"
        assert manifest["id"] == artifacts[0].parent.name
        assert manifest["packet_count"] == 6
        assert manifest["audio_sha256"] == sha256(audio).hexdigest()
        assert _ogg_packets(audio)[2:] == tuple(bytes((8, sequence)) for sequence in range(10, 16))
        await asyncio.to_thread(store.recover_and_publish)
        assert tuple(ready.glob("*/manifest.json")) == artifacts
        assert (artifacts[0].parent / "audio.ogg").read_bytes() == audio

    asyncio.run(scenario())
