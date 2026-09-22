"""Application-owned structural ports for capture infrastructure."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from threading import Thread
from typing import Literal, Protocol

from ...config import FirmwareObservationConfig, WriterConfig
from ..domain.ring_protocol import DoneNotification, ReadBeginNotification, RingInfo


class AttemptDescriptorShape(Protocol):
    attempt_id: str
    start_sequence: int
    packet_count: int


class DurablePrefixShape(Protocol):
    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str


class SealResultShape(Protocol):
    bundle_path: Path
    deduplicated: bool


class QuarantinePublicationShape(Protocol):
    bundle_path: Path
    deduplicated: bool


class RecoveryShape(Protocol):
    valid_records: int


class StagedAttemptShape(Protocol):
    def recover(self) -> RecoveryShape: ...

    def close(self) -> None: ...


class WriterProgressShape(Protocol):
    @property
    def submitted(self) -> int: ...


class BatchWriterPort(Protocol):
    """Asynchronous writer surface needed by opportunistic coordination."""

    @property
    def attempt_id(self) -> str: ...

    @property
    def thread(self) -> Thread: ...

    @property
    def progress(self) -> WriterProgressShape: ...

    @property
    def failure(self) -> BaseException | None: ...

    @property
    def submitted_high_water(self) -> int: ...

    @property
    def written_high_water(self) -> int: ...

    async def start(self) -> None: ...

    async def prepare_leg(self, start_sequence: int, record_count: int) -> object: ...

    async def read_begin(self, notice: ReadBeginNotification) -> object: ...

    async def checkpoint(self) -> object: ...

    async def barrier(self) -> object: ...

    async def seal(self, done_notice: DoneNotification) -> object: ...

    async def publish_prefix(self) -> object: ...

    async def close(self, *, timeout: float) -> None: ...

    def publish(self, high_water: int) -> bool: ...

    def submit_read_begin(self, notice: ReadBeginNotification) -> object: ...


class ObservationWriterPort(Protocol):
    """Best-effort firmware observation sink for one coordinator run."""

    def observe(self, info: RingInfo) -> None: ...

    def close(self) -> None: ...


class PublicationAuthorityPort(Protocol):
    """One lifecycle-bound authority to project captured bundles into ``source/current``.

    Ownership is explicit throughout one collector run.  Before transport, the
    coordinator holds this authority; while a writer owns the active device
    lease, it passes that lease directly for its sealed-capture publication.
    Clock telemetry may invoke this authority from a bounded child task.  After
    shutdown the coordinator revokes it, so a stale authority cannot mutate
    storage.  The authority is capability-based rather than tied to an
    ``asyncio`` task identity.
    """

    def publish(self) -> object | None: ...

    def close(self) -> None: ...


class StagingPort(Protocol):
    """Synchronous local staging operations used by the coordinator."""

    @property
    def attempts_root(self) -> Path: ...

    @property
    def device_state_path(self) -> Path: ...

    @property
    def paths(self) -> object: ...

    def create_publication_authority(
        self, on_failure: Callable[[], None] | None = None
    ) -> PublicationAuthorityPort: ...

    def clock_mutation_lease(self) -> AbstractContextManager[object]: ...

    def pending_attempts(self) -> tuple[object, ...]: ...

    def quarantine_pending(self, reason: str) -> tuple[Path, ...]: ...

    def quarantine_attempt_source(self, attempt_id: str) -> Path: ...

    def open_attempt(self, attempt_id: str) -> object: ...

    def open_attempt_for_resume(self, attempt_id: str) -> object: ...

    def retain_validated_attempt(self, attempt_id: str, attempt: StagedAttemptShape) -> None: ...

    def terminalize_prefix_attempt(self, attempt_id: str) -> None: ...

    def sweep_terminal_retired(self, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]: ...

    def sweep_terminal_quarantine(self, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]: ...

    def quarantined_attempts(self, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]: ...

    def mark_quarantine_unprocessable(self, source: Path, reason: str) -> None: ...

    def mark_quarantine_published(self, source: Path) -> None: ...


QuarantineErrorKind = Literal["unprocessable", "deferred"]


class CaptureRuntimePort(Protocol):
    """Composition and infrastructure seam injected into the application."""

    def make_batch_writer(  # noqa: PLR0913 - port mirrors bounded writer construction inputs
        self,
        staging: StagingPort,
        start: int,
        count: int,
        *,
        source_start: int,
        source: memoryview,
        config: WriterConfig,
    ) -> BatchWriterPort: ...

    def make_observation_writer(
        self, staging: StagingPort, config: FirmwareObservationConfig, on_error: Callable[[Exception], None]
    ) -> ObservationWriterPort: ...

    def make_clock_correction_sink(self, staging: StagingPort) -> object: ...

    def publish_quarantined_prefix(
        self,
        source: Path,
        staging: StagingPort,
        should_defer: Callable[[], bool],
    ) -> object: ...

    def classify_quarantine_error(self, error: BaseException) -> QuarantineErrorKind | None: ...

    def is_writer_error(self, error: BaseException) -> bool: ...

    def is_writer_failed(self, error: BaseException) -> bool: ...

    def is_staging_error(self, error: BaseException) -> bool: ...

    def is_device_busy_error(self, error: BaseException) -> bool: ...

    def debug_event(self, event: str, **fields: object) -> None: ...

    def debug_exception(self, event: str, error: BaseException, **fields: object) -> None: ...
