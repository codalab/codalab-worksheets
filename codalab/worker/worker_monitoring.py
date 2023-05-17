import logging
import os
from typing import Dict, Optional

import sentry_sdk
from sentry_sdk.profiler import start_profiling  # type: ignore

from .worker_run_state import RunState

logger = logging.getLogger(__name__)

transaction_sample_rate = float(os.getenv('CODALAB_SENTRY_TRANSACTION_RATE') or 0)
profiles_sample_rate = float(os.getenv('CODALAB_SENTRY_PROFILES_RATE') or 0)
assert 0 <= transaction_sample_rate <= 1
assert 0 <= profiles_sample_rate <= 1
sentry_sdk.init(
    dsn=os.getenv('CODALAB_SENTRY_INGEST_URL'),
    environment=os.getenv('CODALAB_SENTRY_ENVIRONMENT'),
    traces_sample_rate=transaction_sample_rate,
    _experiments={"profiles_sample_rate": profiles_sample_rate,},  # type: ignore
)


class WorkerMonitoring(object):
    def __init__(self):
        # Sentry Hub is an object used to route events to Sentry
        # Each Hub can only have one open transaction at one time.
        # Bundles can be run concurrently, so each bundle needs its own Hub.
        # The global default Hub sentry_sdk.Hub.current is not used.
        # Each Hub in this dictionary should have exactly one open transaction
        # and one open span.
        self._bundle_uuid_to_hub: Dict[str, sentry_sdk.Hub] = {}
        self._bundle_uuid_to_profile = {}

    def notify_stage_transition(self, run_state: RunState, is_terminal: bool = False):
        bundle_uuid = run_state.bundle.uuid
        hub: Optional[sentry_sdk.Hub] = self._bundle_uuid_to_hub.get(bundle_uuid)
        if hub is None:
            # The hub does not exist for this bundle.
            # Create a hub and a transaction for the bundle.
            hub = sentry_sdk.Hub(sentry_sdk.Hub.current)
            self._bundle_uuid_to_hub[bundle_uuid] = hub
            hub.scope.set_context(
                "bundle",
                {
                    "path": run_state.bundle_path,
                    "command": run_state.bundle.command,
                    "uuid": run_state.bundle.uuid,
                },
            )
            hub.scope.set_user({"id": run_state.bundle.owner_id})
            # TODO: Do hub.scope.set_tag here.
            tx = hub.start_transaction(
                op="queue.task", name=f"worker-{run_state.bundle.command.split()[0]}"
            )
            self._bundle_uuid_to_profile[bundle_uuid] = start_profiling(tx, hub)  # type: ignore
            tx.__enter__()
            self._bundle_uuid_to_profile[bundle_uuid].__enter__()
        else:
            # The hub exists for this bundle and has a transaction for the bundle and
            # a child span for the previous stage.
            # Close the span for the previous stage.
            if hub.scope.span is None:
                logger.error(
                    f'hub.scope.span should not be None for bundle {bundle_uuid} on stage {run_state.stage}'
                )
                raise Exception(
                    f'hub.scope.span should not be None for bundle {bundle_uuid} on stage {run_state.stage}'
                )
                logger.debug(f'bundle has run state {vars(run_state)}')
            else:
                hub.scope.span.__exit__(None, None, None)
        # At this point, the hub has a transaction for the bundle, but no child span for the stage.
        # Open the span for the current stage.
        hub.start_span(op="queue.task.stage", description=run_state.stage).__enter__()

        if is_terminal:
            assert hub.scope.span is not None
            hub.scope.span.__exit__(None, None, None)
            # TODO: Do transaction.set_measurement here.
            self._bundle_uuid_to_profile[bundle_uuid].__exit__(None, None, None)
            hub.scope.transaction.__exit__(None, None, None)
            del self._bundle_uuid_to_hub[bundle_uuid]
            del self._bundle_uuid_to_profile[bundle_uuid]
