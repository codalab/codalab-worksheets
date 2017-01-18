from codalab.lib import worksheet_util


def bundle_to_bundle_info(model, bundle):
    """
    Helper: Convert bundle to bundle_info.
    """
    # See tables.py
    result = {
        'uuid': bundle.uuid,
        'bundle_type': bundle.bundle_type,
        'owner_id': bundle.owner_id,
        'command': bundle.command,
        'data_hash': bundle.data_hash,
        'state': bundle.state,
        'is_anonymous': bundle.is_anonymous,
        'metadata': bundle.metadata.to_dict(),
        'dependencies': [dep.to_dict() for dep in bundle.dependencies],
    }
    if result['dependencies']:
        dep_names = model.get_bundle_names(
            [dep['parent_uuid'] for dep in result['dependencies']])
        for dep in result['dependencies']:
            dep['parent_name'] = dep_names.get(dep['parent_uuid'])

    # Shim in args
    result['args'] = worksheet_util.interpret_genpath(result, 'args')

    return result
