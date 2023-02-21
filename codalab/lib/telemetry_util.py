import logging
import os

import sentry_sdk

CODALAB_SENTRY_INGEST = os.getenv("CODALAB_SENTRY_INGEST_URL", None)
CODALAB_SENTRY_ENVIRONMENT = os.getenv("CODALAB_SENTRY_ENVIRONMENT", None)
logger = logging.getLogger(__name__)


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


def using_sentry():
    return CODALAB_SENTRY_INGEST is not None


def initialize_sentry():
    """
    Initialize the Sentry SDK if it hasn't already been initialized.

    Playing around with Sentry profiling
    """
    if sentry_sdk.Hub.current.client is None:
        # Only do profiling in dev and test environments.
        # And sample a higher percentage of transactions in dev environment.
        transaction_sample_rate = 0
        if os.getenv('CODALAB_SENTRY_TRANSACTION_RATE'):
            transaction_sample_rate = os.getenv('CODALAB_SENTRY_TRANSACTION_RATE')
        if CODALAB_SENTRY_ENVIRONMENT == 'prod':
            sentry_sdk.init(
                dsn=CODALAB_SENTRY_INGEST,
                environment=CODALAB_SENTRY_ENVIRONMENT,
                traces_sample_rate=transaction_sample_rate,
            )
        else:
            sentry_sdk.init(
                dsn=CODALAB_SENTRY_INGEST,
                environment=CODALAB_SENTRY_ENVIRONMENT,
                traces_sample_rate=0.05,
                _experiments={"profiles_sample_rate": transaction_sample_rate,},
            )
        print_sentry_warning()


def load_sentry_data(username=None, **kwargs):
    with sentry_sdk.configure_scope() as scope:
        if username:
            scope.user = {"username": username}
        for kwarg, value in kwargs.items():
            scope.set_tag(kwarg, value)


def capture_exception(exception=None):
    sentry_sdk.capture_exception(exception)


@run_once
def print_sentry_warning():
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    logger.info("@       ATTENTION: Reporting exceptions to the CodaLab team via Sentry.      @")
    logger.info("@  This will log personally-identifying data (e.g., username, instance URL). @")
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
