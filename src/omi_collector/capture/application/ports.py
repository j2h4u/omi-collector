"""Application-owned structural ports for capture infrastructure."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from threading import Thread
from typing import Literal, Protocol, runtime_checkable

from ...config import FirmwareObservationConfig, WriterConfig
from ..domain.ring_protocol import DoneNotification, ReadBeginNotification, RingInfo


class StorageLeasePort(Protocol):
    """Active storage mutation capability held across a consuming operation."""

    def require_active(self) -> None: ...


type StorageLeaseContext = AbstractContextManager[StorageLeasePort]
type StorageLeaseFactory = Callable[[], StorageLeaseContext]


class AttemptDescriptorShape(Protocol):
    @property
    def attempt_id(self) -> str: ...

    @property
    def start_sequence(self) -> int: ...

    @property
    def packet_count(self) -> int: ...


@runtime_checkable
class DurablePrefixShape(Protocol):
    @property
    def start_sequence(self) -> int: ...

    @property
    def next_sequence(self) -> int: ...

    @property
    def record_count(self) -> int: ...

    @property
    def raw_sha256(self) -> str: ...


@runtime_checkable
class SealResultShape(Protocol):
    @property
    def bundle_path(self) -> Path: ...

    @property
    def deduplicated(self) -> bool: ...


class QuarantinePublicationShape(SealResultShape, Protocol):
    pass


class ClockCorrectionShape(Protocol):
    @property
    def operation_id(self) -> str: ...

    @property
    def state(self) -> str: ...

    @property
    def boundary_sequence_min(self) -> int: ...


class ClockObservationShape(Protocol):
    @property
    def observation_id(self) -> str: ...

    @property
    def evidence_kind(self) -> str: ...

    @property
    def session_id(self) -> str: ...

    @property
    def host_boot_id(self) -> str: ...

    @property
    def host_realtime_start(self) -> float: ...

    @property
    def host_realtime_end(self) -> float: ...

    @property
    def host_monotonic_start(self) -> float: ...

    @property
    def host_monotonic_end(self) -> float: ...

    @property
    def device_epoch(self) -> int: ...

    @property
    def info_sequence_min(self) -> int: ...

    @property
    def info_sequence_max(self) -> int: ...

    @property
    def operation_id(self) -> str | None: ...

    @property
    def effective_boundary_sequence(self) -> int | None: ...

    @property
    def observation_role(self) -> str: ...

    @property
    def parent_observation_id(self) -> str | None: ...


class ClockObservationPort(Protocol):
    """Durable clock-evidence ledger used by collection telemetry."""

    def append(  # noqa: PLR0913 - the durable schema is intentionally explicit
        self,
        *,
        evidence_kind: str,
        session_id: str,
        host_boot_id: str,
        host_realtime_start: float,
        host_realtime_end: float,
        host_monotonic_start: float,
        host_monotonic_end: float,
        device_epoch: int,
        info_sequence_min: int,
        info_sequence_max: int,
        operation_id: str | None = None,
        effective_boundary_sequence: int | None = None,
        observation_id: str | None = None,
        observation_role: str = "standalone",
        parent_observation_id: str | None = None,
    ) -> ClockObservationShape: ...

    def records(self) -> tuple[ClockObservationShape, ...]: ...


class ClockCorrectionPort(Protocol):
    """Durable clock-intent and reconciliation ledger."""

    def prepare(
        self, observed_epoch: int, target_epoch: int, drift_seconds: float, boundary_sequence_min: int
    ) -> ClockCorrectionShape: ...

    def mark_unresolved(self, correction: ClockCorrectionShape) -> ClockCorrectionShape: ...

    def finish(
        self,
        correction: ClockCorrectionShape,
        *,
        state: str,
        boundary_sequence_max: int | None,
        verified_epoch: int | None,
    ) -> ClockCorrectionShape: ...

    def reconcile_causal_observation(
        self, observation: ClockObservationShape, *, near_zero_threshold: float
    ) -> tuple[ClockCorrectionShape, ...]: ...

    def records(self) -> tuple[ClockCorrectionShape, ...]: ...


class ClockDurabilityPort(ClockCorrectionPort, Protocol):
    """The complete required clock capability supplied in production."""

    @property
    def observation_store(self) -> ClockObservationPort: ...


class RecoveryShape(Protocol):
    @property
    def valid_records(self) -> int: ...

    @property
    def raw_bytes(self) -> int: ...


class StagedAttemptShape(Protocol):
    def recover(self) -> RecoveryShape: ...

    def close(self) -> None: ...


class ResumeAttemptShape(Protocol):
    @property
    def attempt_id(self) -> str: ...

    @property
    def durable_prefix(self) -> DurablePrefixShape: ...

    def close(self) -> None: ...


class StagingWriterTargetPort(Protocol):
    """Synchronous writer target constructed by the storage capability."""

    @property
    def attempt_id(self) -> str: ...

    def prepare(self) -> AttemptDescriptorShape: ...

    def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefixShape: ...

    def read_begin(self, notice: ReadBeginNotification) -> None: ...

    def append_chunk(self, offset: int, chunk: memoryview) -> object: ...

    def checkpoint(self) -> DurablePrefixShape: ...

    def seal(self, done_notice: DoneNotification) -> SealResultShape: ...

    def publish_prefix(self) -> SealResultShape | None: ...

    def publish_timeline(self) -> object | None: ...

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

    async def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefixShape: ...

    async def read_begin(self, notice: ReadBeginNotification) -> object: ...

    async def checkpoint(self) -> DurablePrefixShape: ...

    async def barrier(self) -> DurablePrefixShape: ...

    async def seal(self, done_notice: DoneNotification) -> SealResultShape: ...

    async def await_seal_result(self) -> SealResultShape | None: ...

    async def publish_prefix(self) -> SealResultShape | None: ...

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


@runtime_checkable
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

    def clock_mutation_lease(self) -> StorageLeaseContext: ...

    def recover_and_publish(self) -> object | None: ...

    def pending_attempts(self) -> tuple[AttemptDescriptorShape, ...]: ...

    def quarantine_pending(self, reason: str) -> tuple[Path, ...]: ...

    def quarantine_attempt_source(self, attempt_id: str) -> Path: ...

    def open_attempt(self, attempt_id: str) -> StagedAttemptShape: ...

    def open_attempt_for_resume(self, attempt_id: str) -> StagedAttemptShape: ...

    def device_lock(self) -> StorageLeaseContext: ...

    def resume_streaming_attempt(self, lease: StorageLeasePort) -> ResumeAttemptShape | None: ...

    def make_staging_writer(self, start: int, count: int) -> StagingWriterTargetPort: ...

    def notify_publication_failure(self) -> None: ...

    def publish_quarantined_prefix(
        self, source: Path, *, should_defer: Callable[[], bool]
    ) -> QuarantinePublicationShape: ...

    def retain_validated_attempt(self, attempt_id: str, attempt: StagedAttemptShape) -> None: ...

    def terminalize_prefix_attempt(self, attempt_id: str) -> None: ...

    def sweep_terminal_retired(self, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]: ...

    def sweep_terminal_quarantine(self, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]: ...

    def quarantined_attempts(self, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]: ...

    def mark_quarantine_unprocessable(self, source: Path, reason: str) -> None: ...

    def mark_quarantine_published(self, source: Path) -> None: ...


QuarantineErrorKind = Literal["unprocessable", "deferred"]


@runtime_checkable
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

    def make_clock_correction_sink(self, staging: StagingPort) -> ClockDurabilityPort: ...

    def publish_quarantined_prefix(
        self,
        source: Path,
        staging: StagingPort,
        should_defer: Callable[[], bool],
    ) -> QuarantinePublicationShape: ...

    def classify_quarantine_error(self, error: BaseException) -> QuarantineErrorKind | None: ...

    def is_writer_error(self, error: BaseException) -> bool: ...

    def is_writer_failed(self, error: BaseException) -> bool: ...

    def is_staging_error(self, error: BaseException) -> bool: ...

    def is_device_busy_error(self, error: BaseException) -> bool: ...

    def debug_event(self, event: str, **fields: object) -> None: ...

    def debug_exception(self, event: str, error: BaseException, **fields: object) -> None: ...
