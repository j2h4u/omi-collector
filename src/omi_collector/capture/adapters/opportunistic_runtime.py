"""Concrete infrastructure runtime for opportunistic capture coordination."""

from __future__ import annotations

from collections.abc import Callable
from threading import Thread
from typing import cast

from ...config import FirmwareObservationConfig, WriterConfig
from ..application.owned_work import join_owned, joined_to_thread
from ..application.ports import (
    BatchWriterPort,
    CaptureRuntimePort,
    DurablePrefixShape,
    DurableSealShape,
    ObservationWriterPort,
    QuarantineAdmissionShape,
    QuarantineErrorKind,
    SealResultShape,
    StagingPort,
    StagingWriterTargetPort,
)
from ..application.storage_fault import FatalInternalAudioStorageError
from ..domain.operational_status_machine import PublicationOutcome
from ..domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from .attempt_writer import AttemptWriter, WriterError, WriterFailedError, WriterProgress
from .clock_state_store import ClockStateStore
from .debug_logging import debug_event, debug_exception
from .firmware_observations import FirmwareObservationStore, FirmwareObservationWriter
from .quarantine import QuarantineCustodyHoldError, QuarantinePublishError, QuarantineSalvageDeferredError
from .staging_contract import DeviceAlreadyRunningError, StagingError
from .staging_store import StagingStore


class _StagingWriterAdapter:
    """Checked ``AttemptWriter`` target over the public staging-writer API."""

    def __init__(self, writer: StagingWriterTargetPort, source_start: int) -> None:
        self._writer = writer
        self._leg_base = 0
        self._source_start = source_start

    def prepare(self) -> object:
        self._writer.prepare()
        return self._writer.durable_prefix

    @property
    def attempt_id(self) -> str:
        return self._writer.attempt_id

    def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefixShape:
        self._leg_base = (start_sequence - self._source_start) * RECORD_SIZE
        return self._writer.prepare_leg(start_sequence, record_count)

    def read_begin(self, notice: object) -> None:
        if not isinstance(notice, ReadBeginNotification):
            raise TypeError("notice must be a ReadBeginNotification")
        self._writer.read_begin(notice)

    def append_chunk(self, offset: int, chunk: memoryview, tail: bytes = b"") -> object:
        if offset < self._leg_base:
            raise ValueError("arena chunk precedes prepared READ leg")
        return self._writer.append_chunk(offset - self._leg_base, chunk, tail)

    def checkpoint(self) -> DurablePrefixShape:
        return self._writer.checkpoint()

    def interrupt(self) -> DurablePrefixShape:
        return self._writer.interrupt()

    def seal(self, done_notice: object) -> SealResultShape | DurableSealShape:
        if not isinstance(done_notice, DoneNotification):
            raise TypeError("done_notice must be a DoneNotification")
        return self._writer.seal(done_notice)

    def publish_prefix(self) -> SealResultShape | DurableSealShape | None:
        return self._writer.publish_prefix()

    def close(self) -> None:
        self._writer.close()


class _BatchWriter:
    """Expose writer operations and the prepared target identity together."""

    def __init__(self, writer: AttemptWriter, target: _StagingWriterAdapter) -> None:
        self._writer = writer
        self._target = target
        self._attempt_id: str | None = None
        self._initial_prefix: DurablePrefixShape | None = None
        self._interrupted_prefix: DurablePrefixShape | None = None

    @property
    def attempt_id(self) -> str:
        if self._attempt_id is None:
            raise RuntimeError("writer has not been started")
        return self._attempt_id

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
        if self._initial_prefix is not None:
            return

        async def settle() -> None:
            result = await self._writer.start()
            if not isinstance(result, DurablePrefixShape):
                raise TypeError("writer returned an invalid initial durable prefix")
            self._attempt_id = self._target.attempt_id
            self._initial_prefix = result

        await join_owned(settle())

    @property
    def initial_prefix(self) -> DurablePrefixShape:
        if self._initial_prefix is None:
            raise RuntimeError("writer has not been started")
        return self._initial_prefix

    @property
    def interrupted_prefix(self) -> DurablePrefixShape | None:
        """Return the current-leg interruption acknowledgment, when committed."""
        return self._interrupted_prefix

    async def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefixShape:
        async def settle() -> DurablePrefixShape:
            result = await self._writer.prepare_leg(start_sequence, record_count)
            if not isinstance(result, DurablePrefixShape):
                raise TypeError("writer returned an invalid durable prefix")
            self._interrupted_prefix = None
            return result

        return await join_owned(settle())

    async def read_begin(self, notice: ReadBeginNotification) -> object:
        return await self._writer.read_begin(notice)

    async def checkpoint(self) -> DurablePrefixShape:
        result = await self._writer.checkpoint()
        if not isinstance(result, DurablePrefixShape):
            raise TypeError("writer returned an invalid durable prefix")
        return result

    async def barrier(self) -> DurablePrefixShape:
        result = await self._writer.barrier()
        if not isinstance(result, DurablePrefixShape):
            raise TypeError("writer returned an invalid durable prefix")
        return result

    async def interrupt(self) -> DurablePrefixShape:
        async def settle() -> DurablePrefixShape:
            result = await self._writer.interrupt()
            if not isinstance(result, DurablePrefixShape):
                raise TypeError("writer returned an invalid interrupted prefix")
            self._interrupted_prefix = result
            return result

        return await join_owned(settle())

    async def seal(self, done_notice: DoneNotification) -> SealResultShape | DurableSealShape:
        result = await self._writer.seal(done_notice)
        if not isinstance(result, (SealResultShape, DurableSealShape)):
            raise TypeError("writer returned an invalid seal result")
        return result

    async def await_seal_result(self) -> SealResultShape | DurableSealShape | None:
        result = await self._writer.await_seal_result()
        if result is not None and not isinstance(result, (SealResultShape, DurableSealShape)):
            raise TypeError("writer retained an invalid seal result")
        return result

    async def publish_prefix(self) -> SealResultShape | DurableSealShape | None:
        result = await self._writer.publish_prefix()
        if result is not None and not isinstance(result, SealResultShape | DurableSealShape):
            raise TypeError("writer returned an invalid prefix publication")
        return result

    async def close(self) -> None:
        await self._writer.close()

    def publish(self, high_water: int, tail: bytes = b"") -> bool:
        return self._writer.publish(high_water, tail)

    def submit_read_begin(self, notice: ReadBeginNotification) -> object:
        return self._writer.submit_read_begin(notice)


class OpportunisticRuntime(CaptureRuntimePort):
    """Compose the existing capture adapters for one application run."""

    def __init__(self, record_publication: Callable[[PublicationOutcome], None] | None = None) -> None:
        self._record_publication = record_publication

    def make_batch_writer(  # noqa: PLR0913 - transfer intent values stay explicit
        self,
        staging: StagingPort,
        start: int,
        count: int,
        *,
        source_start: int,
        source: memoryview,
        config: WriterConfig,
    ) -> BatchWriterPort:
        target = _StagingWriterAdapter(staging.make_staging_writer(start, count), source_start)
        return _BatchWriter(
            AttemptWriter(target, source, config=config),
            target,
        )

    def make_observation_writer(
        self, staging: StagingPort, config: FirmwareObservationConfig, on_error: Callable[[Exception], None]
    ) -> ObservationWriterPort:
        store = FirmwareObservationStore(cast(StagingStore, staging).state_db)
        return FirmwareObservationWriter(
            store,
            config,
            on_error=on_error,
        )

    def make_clock_state_store(self, staging: StagingPort) -> ClockStateStore:
        return ClockStateStore(cast(StagingStore, staging).state_db)

    def admit_quarantined_prefix(
        self,
        source: str,
        staging: StagingPort,
        should_defer: Callable[[], bool],
    ) -> QuarantineAdmissionShape:
        return staging.admit_quarantined_prefix(source, should_defer=should_defer)

    def classify_quarantine_error(self, error: BaseException) -> QuarantineErrorKind | None:
        if isinstance(error, QuarantineCustodyHoldError):
            return "deferred"
        if isinstance(error, QuarantinePublishError):
            return "deferred"
        if isinstance(error, (QuarantineSalvageDeferredError, DeviceAlreadyRunningError)):
            return "deferred"
        if isinstance(error, OSError):
            return "retryable"
        if isinstance(error, StagingError):
            return "deferred"
        return None

    def is_writer_error(self, error: BaseException) -> bool:
        return isinstance(error, WriterError)

    def is_writer_failed(self, error: BaseException) -> bool:
        return isinstance(error, WriterFailedError)

    def is_staging_error(self, error: BaseException) -> bool:
        return isinstance(error, StagingError)

    def is_device_busy_error(self, error: BaseException) -> bool:
        return isinstance(error, DeviceAlreadyRunningError)

    def debug_event(self, event: str, **fields: object) -> None:
        if self._record_publication is not None:
            if event == "ready_publication_blocked":
                self._record_publication(PublicationOutcome.BLOCKED)
            elif event == "ready_publication_published":
                self._record_publication(PublicationOutcome.PUBLISHED)
            elif event == "ready_publication_transient":
                self._record_publication(PublicationOutcome.TRANSIENT)
            elif event == "ready_publication_waiting":
                self._record_publication(PublicationOutcome.WAITING)
        cast(Callable[..., None], debug_event)(event, **fields)

    def debug_exception(self, event: str, error: BaseException, **fields: object) -> None:
        cast(Callable[..., None], debug_exception)(event, error, **fields)


async def create_quality_metrics_owned[T](factory: Callable[..., T], *args: object, **kwargs: object) -> T:
    """Own a quality factory result across cancellation and join any untransferred worker."""
    settled: list[T] = []

    def construct() -> T:
        result = factory(*args, **kwargs)
        settled.append(result)
        return result

    try:
        return await joined_to_thread(construct)
    except BaseException as primary:
        if settled:
            close = cast(Callable[[], object] | None, getattr(settled[0], "close", None))
            if close is not None:
                try:
                    await joined_to_thread(close)
                except FatalInternalAudioStorageError:
                    raise
                except BaseException:  # noqa: BLE001 - retain the primary factory outcome unless cleanup is fatal
                    if isinstance(primary, FatalInternalAudioStorageError):
                        raise primary from None
        raise
