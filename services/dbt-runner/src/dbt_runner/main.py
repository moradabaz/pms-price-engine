import subprocess
import time

from common import configure_logging, get_logger

from dbt_runner.settings import DbtRunnerSettings


def run_dbt_command(args: list[str]) -> bool:
    """Runs one dbt CLI command as a subprocess — dbt-core/dbt-duckdb live in
    a separate venv baked into the image (Dockerfile), not this package's own
    dependencies (pyproject.toml). Returns whether it succeeded."""
    result = subprocess.run(["dbt", *args], capture_output=True, text=True)
    if result.returncode != 0:
        get_logger(__name__).warning(
            "dbt_command_failed",
            args=args,
            stdout=result.stdout[-2000:],
            stderr=result.stderr[-2000:],
        )
    return result.returncode == 0


def run_once(settings: DbtRunnerSettings) -> dict[str, bool]:
    dirs = [
        "--project-dir",
        settings.project_dir,
        "--profiles-dir",
        settings.profiles_dir,
    ]
    return {
        "seed": run_dbt_command(["seed", *dirs]),
        "run": run_dbt_command(["run", *dirs]),
        "test": run_dbt_command(["test", *dirs]),
    }


def run_forever(settings: DbtRunnerSettings) -> None:
    logger = get_logger(__name__)
    while True:
        started = time.monotonic()
        results = run_once(settings)
        logger.info("dbt_tick", **results)

        elapsed = time.monotonic() - started
        # Run, then sleep the *remainder* of the interval (spec 05 §10) — a
        # fixed sleep would let a slow tick stack on top of the next one.
        time.sleep(max(0.0, settings.run_interval_seconds - elapsed))


def main() -> None:
    settings = DbtRunnerSettings()
    configure_logging(settings.log_level)
    run_forever(settings)


if __name__ == "__main__":
    main()
