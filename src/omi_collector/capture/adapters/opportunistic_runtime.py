"""Concrete infrastructure runtime for opportunistic capture coordination."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from threading import Thread
from typing import cast

from ...config import FirmwareObservationConfig, WriterConfig
from ..application.ports import (
    BatchWriterPort,
    CaptureRuntimePort,
    ObservationWriterPort,
    QuarantineErrorKind,
    StagingPort,
)
from ..domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from .attempt_writer import AttemptWriter, WriterError, WriterFailedError, WriterProgress
from .attempts import RecordDisposition
from .debug_logging import debug_event, debug_exception
from .firmware_observations import FirmwareObservationStore, FirmwareObservationWriter
from .publication import SealResult
from .quarantine_publish import (
    QuarantineOutputCollisionError,
    QuarantinePublishError,
    QuarantineSalvageDeferredError,
    publish_quarantined_prefix,
)
from .staging_contract import AttemptDescriptor, DurablePrefix, StagingError
from .staging_store import StagingStore
from .staging_writer import StagingWriter


class _StagingWriterAdapter:
    """Checked ``AttemptWriter`` target over the public staging-writer API."""

    def __init__(self, store: StagingStore, device_slug: str, start: int, count: int, source_start: int) -> None:
        self._writer = StagingWriter(store, device_slug, start, count)
        self._leg_base = 0
        self._source_start = source_start

    def prepare(self) -> AttemptDescriptor:
        return self._writer.prepare()

    @property
    def attempt_id(self) -> str:
        return self._writer.attempt_id

    def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefix:
        self._leg_base = (start_sequence - self._source_start) * RECORD_SIZE
        return self._writer.prepare_leg(start_sequence, record_count)

    def read_begin(self, notice: object) -> None:
        if not isinstance(notice, ReadBeginNotification):
            raise TypeError("notice must be a ReadBeginNotification")
        self._writer.read_begin(notice)

    def append_chunk(self, offset: int, chunk: memoryview) -> tuple[RecordDisposition, ...]:
        if offset < self._leg_base:
            raise ValueError("arena chunk precedes prepared READ leg")
        return self._writer.append_chunk(offset - self._leg_base, chunk)

    def checkpoint(self) -> DurablePrefix:
        return self._writer.checkpoint()

    def seal(self, done_notice: object) -> SealResult:
        if not isinstance(done_notice, DoneNotification):
            raise TypeError("done_notice must be a DoneNotification")
        return self._writer.seal(done_notice)

    def publish_prefix(self) -> SealResult | None:
        return self._writer.publish_prefix()

    def close(self) -> None:
        self._writer.close()


class _BatchWriter:
    """Expose writer operations and the prepared target identity together."""

    def __init__(self, writer: AttemptWriter, target: _StagingWriterAdapter) -> None:
        self._writer = writer
        self._target = target

    @property
    def attempt_id(self) -> str:
        return self._target.attempt_id

    @property
    def thread(self) -> Thread:
        return self._writer.thread

    @property
    def progress(self) -> WriterProgress:
        return self._writer.progress

    @property
    def failure(self) -> BaseException | None:
        return self._writer.failure

    @property
    def submitted_high_water(self) -> int:
        """Expose event-loop submitted bytes to the collector's typed counter seam."""
        return self._writer.submitted_high_water

    @property
    def written_high_water(self) -> int:
        """Expose writer-thread committed bytes to the collector's typed counter seam."""
        return self._writer.written_high_water

    async def start(self) -> None:
        await self._writer.start()

    async def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        return await self._writer.prepare_leg(start_sequence, record_count)

    async def read_begin(self, notice: ReadBeginNotification) -> object:
        return await self._writer.read_begin(notice)

    async def checkpoint(self) -> object:
        return await self._writer.checkpoint()

    async def barrier(self) -> object:
        return await self._writer.barrier()

    async def seal(self, done_notice: DoneNotification) -> object:
        return await self._writer.seal(done_notice)

    async def publish_prefix(self) -> object:
        return await self._writer.publish_prefix()

    async def close(self, *, timeout: float) -> None:
        await self._writer.close(timeout=timeout)

    def publish(self, high_water: int) -> bool:
        return self._writer.publish(high_water)

    def submit_read_begin(self, notice: ReadBeginNotification) -> object:
        return self._writer.submit_read_begin(notice)


class OpportunisticRuntime(CaptureRuntimePort):
    """Compose the existing capture adapters for one application run."""

    def make_batch_writer(  # noqa: PLR0913 - transfer intent values stay explicit
        self,
        staging: StagingPort,
        device_slug: str,
        start: int,
        count: int,
        *,
        source_start: int,
        source: memoryview,
        config: WriterConfig,
    ) -> BatchWriterPort:
        target = _StagingWriterAdapter(cast(StagingStore, staging), device_slug, start, count, source_start)
        return _BatchWriter(
            AttemptWriter(target, source, config=config),
            target,
        )

    def make_observation_writer(
        self, staging: StagingPort, config: FirmwareObservationConfig, on_error: Callable[[Exception], None]
    ) -> ObservationWriterPort:
        store = FirmwareObservationStore(cast(StagingStore, staging).device_state_path)
        return FirmwareObservationWriter(
            store,
            config,
            on_error=on_error,
        )

    def publish_quarantined_prefix(
        self,
        source: Path,
        staging: StagingPort,
        device_slug: str,
        should_defer: Callable[[], bool],
    ) -> object:
        return publish_quarantined_prefix(
            source,
            cast(StagingStore, staging).paths,
            device_slug,
            should_defer=should_defer,
        )

    def classify_quarantine_error(self, error: BaseException) -> QuarantineErrorKind | None:
        if isinstance(error, (QuarantinePublishError, QuarantineOutputCollisionError)):
            return "unprocessable"
        if isinstance(error, QuarantineSalvageDeferredError):
            return "deferred"
        if isinstance(error, (OSError, StagingError)):
            return "deferred"
        return None

    def is_writer_error(self, error: BaseException) -> bool:
        return isinstance(error, WriterError)

    def is_writer_failed(self, error: BaseException) -> bool:
        return isinstance(error, WriterFailedError)

    def is_staging_error(self, error: BaseException) -> bool:
        return isinstance(error, StagingError)

    def debug_event(self, event: str, **fields: object) -> None:
        cast(Callable[..., None], debug_event)(event, **fields)

    def debug_exception(self, event: str, error: BaseException, **fields: object) -> None:
        cast(Callable[..., None], debug_exception)(event, error, **fields)
