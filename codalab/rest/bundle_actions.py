from bottle import local, post, request

from codalab.common import UsageError, precondition
from codalab.lib.bundle_action import BundleAction
from codalab.objects.permission import check_bundles_have_all_permission
from codalab.rest.schemas import BundleActionSchema
from codalab.server.authenticated_plugin import AuthenticatedPlugin
from codalab.worker.bundle_state import State


@post('/bundle-actions', apply=AuthenticatedPlugin())
def create_bundle_actions():
    """
    Sends the message to the worker to do the bundle action, and adds the
    action string to the bundle metadata.
    """
    actions = BundleActionSchema(strict=True, many=True).load(request.json).data

    check_bundles_have_all_permission(local.model, request.user, [a['uuid'] for a in actions])
    for action in actions:
        bundle = local.model.get_bundle(action['uuid'])
        if bundle.state not in [
            State.CREATED,
            State.UPLOADING,
            State.STAGED,
            State.PREPARING,
            State.RUNNING,
            State.FINALIZING,
        ]:
            raise UsageError(
                'Cannot execute this action on a bundle that is not in the following states:'
                'created, uploading, staged, running, preparing and finalizing.'
            )

        worker = local.model.get_bundle_worker(action['uuid'])
        new_actions = getattr(bundle.metadata, 'actions', []) + [BundleAction.as_string(action)]

        # The state updates of bundles in PREPARING, RUNNING, or FINALIZING state will be handled on the worker side.
        if worker:
            precondition(
                local.worker_model.send_json_message(worker['socket_id'], action, 60),
                'Unable to reach worker.',
            )
            local.model.update_bundle(bundle, {'metadata': {'actions': new_actions}})
        else:
            # The state updates of bundles in CREATED, UPLOADING, or STAGED state
            # will be handled on the rest-server side.
            local.model.update_bundle(
                bundle, {'state': State.KILLED, 'metadata': {'actions': new_actions}}
            )

    return BundleActionSchema(many=True).dump(actions).data
