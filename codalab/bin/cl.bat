
@set EDITOR=notepad
@set PYTHONPATH=%~dp0\..\..
@python %~dp0\codalab_client.py %~dp0\..\config\sqlite_client_config.json %*