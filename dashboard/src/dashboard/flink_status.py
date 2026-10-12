from typing import Any

import requests

DEFAULT_TIMEOUT_SECONDS = 2.0

JOBS_OVERVIEW_PATH = "/jobs/overview"
JOB_DETAIL_PATH = "/jobs/{job_id}"
JOB_CHECKPOINTS_PATH = "/jobs/{job_id}/checkpoints"

JOBS_FIELD = "jobs"
JOB_NAME_FIELD = "name"
JOB_ID_FIELD = "jid"
JOB_START_TIME_FIELD = "start-time"
JOB_STATE_FIELD = "state"
JOB_STATE_RUNNING = "RUNNING"
CHECKPOINTS_LATEST_FIELD = "latest"
CHECKPOINTS_COMPLETED_FIELD = "completed"
CHECKPOINT_ID_FIELD = "id"
CHECKPOINT_DURATION_FIELD = "end_to_end_duration"
CHECKPOINT_SIZE_FIELD = "state_size"


class FlinkStatus:
    """Snapshot of the Market Pulse Job's state as seen by the Flink REST
    API: job status plus its latest completed checkpoint. `.ok` is False
    whenever the jobmanager is unreachable or no matching job is running
    (e.g. mid-restart during a failure demo), so callers can render that
    instead of raising."""

    def __init__(
        self,
        ok: bool,
        job_status: str | None = None,
        checkpoint_id: int | None = None,
        checkpoint_duration_ms: int | None = None,
        checkpoint_size_bytes: int | None = None,
        error: str | None = None,
    ) -> None:
        self.ok = ok
        self.job_status = job_status
        self.checkpoint_id = checkpoint_id
        self.checkpoint_duration_ms = checkpoint_duration_ms
        self.checkpoint_size_bytes = checkpoint_size_bytes
        self.error = error


def _find_job_id(base_url: str, job_name: str, timeout_seconds: float) -> str | None:
    """The job named `job_name` to report on — `/jobs/overview` keeps every
    past submission (CANCELED, FAILED, ...), and a re-submitted job shows up
    as a new entry with the same name rather than replacing the old one. A
    RUNNING match always wins over a more recent but no-longer-live one;
    among non-RUNNING matches (e.g. right after a crash, before Flink
    reschedules it), the most recently started one is the relevant one."""
    response = requests.get(f"{base_url}{JOBS_OVERVIEW_PATH}", timeout=timeout_seconds)
    response.raise_for_status()
    matches = [
        job
        for job in response.json().get(JOBS_FIELD, [])
        if job.get(JOB_NAME_FIELD) == job_name
    ]
    if not matches:
        return None
    running = [job for job in matches if job.get(JOB_STATE_FIELD) == JOB_STATE_RUNNING]
    candidates = running or matches
    latest = max(candidates, key=lambda job: job.get(JOB_START_TIME_FIELD, 0))
    return latest.get(JOB_ID_FIELD)


def fetch_status(
    base_url: str, job_name: str, timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
) -> FlinkStatus:
    """Queries `{base_url}/jobs/overview` for the job named `job_name`, then
    its latest checkpoint from `/jobs/{jid}/checkpoints`. Network/HTTP
    failures and a missing job both come back as `FlinkStatus(ok=False, ...)`
    rather than raising, since this is a best-effort dashboard panel, not a
    control path."""
    try:
        job_id = _find_job_id(base_url, job_name, timeout_seconds)
        if job_id is None:
            return FlinkStatus(ok=False, error=f"No running job named '{job_name}'")

        detail_path = JOB_DETAIL_PATH.format(job_id=job_id)
        overview = requests.get(f"{base_url}{detail_path}", timeout=timeout_seconds)
        overview.raise_for_status()
        job_status = overview.json().get(JOB_STATE_FIELD)

        checkpoints_path = JOB_CHECKPOINTS_PATH.format(job_id=job_id)
        checkpoints = requests.get(f"{base_url}{checkpoints_path}", timeout=timeout_seconds)
        checkpoints.raise_for_status()
        latest: dict[str, Any] | None = (
            checkpoints.json().get(CHECKPOINTS_LATEST_FIELD, {}).get(CHECKPOINTS_COMPLETED_FIELD)
        )

        return FlinkStatus(
            ok=True,
            job_status=job_status,
            checkpoint_id=latest.get(CHECKPOINT_ID_FIELD) if latest else None,
            checkpoint_duration_ms=(
                latest.get(CHECKPOINT_DURATION_FIELD) if latest else None
            ),
            checkpoint_size_bytes=(
                latest.get(CHECKPOINT_SIZE_FIELD) if latest else None
            ),
        )
    except requests.RequestException as exc:
        return FlinkStatus(ok=False, error=str(exc))
