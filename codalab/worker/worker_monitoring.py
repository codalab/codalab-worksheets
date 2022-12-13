from typing import Dict, Optional

import sentry_sdk

from .worker_run_state import RunState


class WorkerMonitoring(object):
    def __init__(self):
        # Each Hub can only have one open transaction at one time.
        # Bundles can be run concurrently, so each bundle needs its own Hub.
        # The global default Hub sentry_sdk.Hub.current is not used.
        # Each Hub in this dictionary should have exactly one open transaction
        # and one open span.
        self._bundle_uuid_to_hub: Dict[str, sentry_sdk.Hub] = {}

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
                    'container_id': run_state.bundle.container_id,
                    'docker_image': run_state.bundle.docker_image,
                },
            )
            hub.scope.set_user({"id": run_state.bundle.owner_id})
            # TODO: Do hub.scope.set_tag here.
            hub.start_transaction(op="queue.task", name="worker.run").__enter__()
        else:
            # The hub exists for this bundle and has a transaction for the bundle and
            # a child span for the previous stage.
            # Close the span for the previous stage.
            assert hub.scope.span is not None
            hub.scope.span.__exit__(None, None, None)
        # At this point, the hub has a transaction for the bundle, but no child span for the stage.
        # Open the span for the current stage.
        hub.start_span(op="queue.task.stage", description=run_state.stage).__enter__()

        if is_terminal:
            assert hub.scope.span is not None
            hub.scope.span.__exit__(None, None, None)
            # TODO: Do transaction.set_measurement here.
            hub.scope.transaction.__exit__(None, None, None)
            del self._bundle_uuid_to_hub[bundle_uuid]
