import logging
import os

import sentry_sdk

CODALAB_SENTRY_INGEST = os.getenv("CODALAB_SENTRY_INGEST_URL", None)
logger = logging.getLogger(__name__)


def run_if_sentry_ingest_provided(f):
    def wrapper(*args, **kwargs):
        if CODALAB_SENTRY_INGEST:
            return f(*args, **kwargs)

    return wrapper


def run_once(f):
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


@run_if_sentry_ingest_provided
def initialize_sentry():
    """
    Initialize the Sentry SDK if it hasn't already been initialized.
    """
    if sentry_sdk.Hub.current.client is None:
        sentry_sdk.init(CODALAB_SENTRY_INGEST)
        print_sentry_warning()


@run_if_sentry_ingest_provided
def load_sentry_data(username=None, **kwargs):
    with sentry_sdk.configure_scope() as scope:
        if username:
            scope.user = {"username": username}
        for kwarg, value in kwargs.items():
            scope.set_tag(kwarg, value)


@run_if_sentry_ingest_provided
def capture_exception(exception=None):
    sentry_sdk.capture_exception(exception)


@run_if_sentry_ingest_provided
@run_once
def print_sentry_warning():
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    logger.info("@       ATTENTION: Reporting exceptions to the CodaLab team via Sentry.      @")
    logger.info("@  This will log personally-identifying data (e.g., username, instance URL). @")
    logger.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
