from common import configure_logging, get_logger
from pyflink.datastream import StreamExecutionEnvironment

from market_pulse_job.job import build_job
from market_pulse_job.settings import MarketPulseJobSettings


def main() -> None:
    settings = MarketPulseJobSettings()
    configure_logging(settings.log_level)
    logger = get_logger(__name__)

    env = StreamExecutionEnvironment.get_execution_environment()
    build_job(env, settings)

    logger.info("market_pulse_job_starting")
    env.execute("market-pulse-job")


if __name__ == "__main__":
    main()
