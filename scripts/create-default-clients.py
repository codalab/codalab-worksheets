#!./venv/bin/python
"""
Script that creates the default CodaLab OAuth2 clients.

 - codalab_cli_client for the Bundle CLI clients authenticating through the Password Grant
 - codalab_worker_client for workers authenticating through the Password Grant

TODO(skoo): Create row for the web client given a redirect url.
"""
import sys
sys.path.append('.')

from codalab.lib.codalab_manager import CodaLabManager
from codalab.objects.oauth2 import OAuth2Client

manager = CodaLabManager()
model = manager.model()

if not model.get_oauth2_client('codalab_cli_client'):
    model.save_oauth2_client(OAuth2Client(
        model,
        client_id='codalab_cli_client',
        secret=None,
        name='Codalab CLI',
        user_id=None,
        grant_type='password',
        response_type='token',
        scopes='default',
        redirect_uris='',
    ))

if not model.get_oauth2_client('codalab_worker_client'):
    model.save_oauth2_client(OAuth2Client(
        model,
        client_id='codalab_worker_client',
        secret=None,
        name='Codalab Worker',
        user_id=None,
        grant_type='password',
        response_type='token',
        scopes='default',
        redirect_uris='',
    ))
