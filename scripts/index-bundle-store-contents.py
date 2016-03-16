#!./venv/bin/python
"""
Indexes the contents of the bundle store. Used during the launch of the new
worker system.

TODO(klopyrev): Delete once it's launched.
"""
import sys
sys.path.append('.')

from codalab.common import State
from codalab.lib.codalab_manager import CodaLabManager
from codalab.model.tables import bundle as cl_bundle, bundle_contents_index as cl_bundle_contents_index
from sqlalchemy import distinct, select
from worker.file_util import index_contents


manager = CodaLabManager()
bundle_store = manager.bundle_store()
model = manager.model()
engine = model.engine

with engine.begin() as conn:
    bundles = conn.execute(
        select([cl_bundle.c.uuid])
        .where(cl_bundle.c.state.in_([State.READY, State.FAILED]))
    ).fetchall()
    indexed_bundles = conn.execute(
        select([distinct(cl_bundle_contents_index.c.bundle_uuid)])
    ).fetchall()

uuids_to_index = (set(bundle.uuid for bundle in bundles) -
                  set(bundle.bundle_uuid for bundle in indexed_bundles))

for uuid in uuids_to_index:
    print 'Indexing', uuid
    index = index_contents(bundle_store.get_bundle_location(uuid))
    model.update_bundle_contents_index(uuid, index)
