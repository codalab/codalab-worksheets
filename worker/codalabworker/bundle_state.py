class State(object):
    """
    An enumeration of states that a bundle can be in.
    """
    UPLOADING = 'uploading'  # Waiting for contents to be uploaded
    CREATED = 'created'   # Just created
    STAGED = 'staged'     # All the dependencies are met
    MAKING = 'making'  # Creating a make bundle.
    WAITING_FOR_WORKER_STARTUP = 'waiting_for_worker_startup'  # Waiting for the worker to start up.
    STARTING = 'starting'  # Wait for the worker to start running the bundle.
    PREPARING = 'preparing' # Wait for worker to download dependencies and docker images
    RUNNING = 'running'   # Actually running
    READY = 'ready'       # Done running and succeeded
    FAILED = 'failed'     # Done running and failed
    KILLED = 'killed'     # Killed by user
    WORKER_OFFLINE = 'worker_offline'  # Assigned worker has gone offline

    OPTIONS = {CREATED, STAGED, MAKING, WAITING_FOR_WORKER_STARTUP, STARTING, RUNNING, READY, FAILED, PREPARING}
    ACTIVE_STATES = {MAKING, WAITING_FOR_WORKER_STARTUP, STARTING, RUNNING, PREPARING}
    FINAL_STATES = {READY, FAILED, KILLED}



