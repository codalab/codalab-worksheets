class State(object):
    """
    An enumeration of states that a bundle can be in.
    """
    # Waiting for contents to be uploaded
    UPLOADING = 'uploading'
    # Just created
    CREATED = 'created'
    # All the dependencies are met
    STAGED = 'staged'
    # Creating a make bundle.
    MAKING = 'making'
    # Waiting for the worker to start up.
    WAITING_FOR_WORKER_STARTUP = 'waiting_for_worker_startup'
    # Wait for the worker to start running the bundle.
    STARTING = 'starting'
    # Wait for worker to download dependencies and docker images
    PREPARING = 'preparing'
    # Actually running
    RUNNING = 'running'
    # Run finished and finalized server-side, tell worker to discard it
    FINALIZING = 'finalizing'
    # Done running and succeeded
    READY = 'ready'
    # Done running and failed
    FAILED = 'failed'
    # Killed by user
    KILLED = 'killed'
    # Assigned worker has gone offline
    WORKER_OFFLINE = 'worker_offline'

    OPTIONS = {CREATED, STAGED, MAKING, WAITING_FOR_WORKER_STARTUP, STARTING, RUNNING, READY, FAILED, PREPARING, FINALIZING}
    ACTIVE_STATES = {MAKING, WAITING_FOR_WORKER_STARTUP, STARTING, RUNNING, FINALIZING, PREPARING}
    FINAL_STATES = {READY, FAILED, KILLED}
