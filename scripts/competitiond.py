#!./venv/bin/python
"""
Competition leaderboard evaluation daemon.

Does the following in a loop, every {refresh_period_seconds}:
    1. Find bundles tagged with {submission_tag} and filter them.
    2. Run the {predict} command with the submitted bundle to generate
       predictions on the test set.
    3. Tag the resulting test run bundle with {predict.tag}, untagging
       any previous test run bundles for the same submitter.
    4. Run {evaluate} command with the test run bundle.
    5. Tag the resulting evaluation bundle with {evaluate.tag}, untagging
       any previous evaluation bundles for the same submitter.

All bundles created by this daemon are added to {log_worksheet_uuid}.
Each user will be limited to {max_submissions_per_period} every
{quota_period_seconds}, and {max_submissions_total} ever.

Example config file:

{
    "max_submissions_per_period": 5,
    "max_submissions_total": 10000,
    "refresh_period_seconds": 10,
    "host": "https://worksheets.codalab.org",
    "username": "xxxxxxxxxxxx",
    "password": "xxxxxxxxxxxx",
    "submission_tag": "xxxx-submit",
    "log_worksheet_uuid": "0x2263f854a967abcabade0b6c88f51f29",
    "predict": {
        "mimic": [
            {
                "old": "0x4870af25abc94b0687a1927fcec66392",
                "new": "0xbcd57bee090b421c982906709c8c27e1"
            }
        ],
        "metadata": {
            "request_queue": ""
        },
        "tag": "xxxx-predict"
    },
    "evaluate": {
        "dependencies": [
            {
                "parent_uuid": "0x089063eb85b64b239b342405b5ebab57",
                "child_path": "evaluate.py"
            },
            {
                "parent_uuid": "0x5538cba32e524fad8b005cd19abb9f95",
                "child_path": "dev.json"
            },
            {
                "parent_uuid": "{predict}",
                "parent_path": "predictions.json",
                "child_path": "predictions.json"
            }
        ],
        "command": "python evaluate.py dev.json predictions.json",
        "tag": "xxxx-eval"
    },
    "score_specs": [
        {
            "name": "f1",
            "key": "/stdout:f1"
        },
        {
            "name": "exact_match",
            "key": "/stdout:exact_match"
        }
    ],
    "metadata": {
        "name": "Cool Competition Leaderboard"
    }
}

The following string substitutions will be made in the dependency specs:

    {predict} => UUID of the resulting test run bundle

"""
import argparse
import getpass
import json
import logging
import re
import signal
import sys
import time
import traceback
from collections import defaultdict

from marshmallow import Schema, fields, ValidationError

sys.path.append('.')
from codalab.bundles import RunBundle
from codalab.common import NotFoundError, State, PermissionError
from codalab.client.json_api_client import (
    JsonApiClient,
    JsonApiRelationship,
    JsonApiException,
)
from codalab.lib.bundle_util import mimic_bundles
from codalab.lib.metadata_util import fill_missing_metadata
from codalab.lib.print_util import pretty_json
from codalab.lib.spec_util import UUID_STR
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.rest.schemas import BundleDependencySchema, validate_uuid
from codalab.server.auth import RestOAuthHandler


logger = logging.getLogger(__name__)


class ThrottledJsonApiClientWithRetry(JsonApiClient):
    """
    JsonApiClient with a retry block around every request.
    """
    def __init__(self, *args, **kwargs):
        self.__num_retries = kwargs.pop('num_retries', 2)
        self.__wait_seconds = kwargs.pop('wait_seconds', 1)
        super(ThrottledJsonApiClientWithRetry, self).__init__(*args, **kwargs)

    def _make_request(self, *args, **kwargs):
        num_retries_left = self.__num_retries
        while True:
            try:
                return super(ThrottledJsonApiClientWithRetry, self)._make_request(*args, **kwargs)
            except JsonApiException:
                if num_retries_left > 0:
                    num_retries_left -= 1
                    logger.exception('Request failed, retrying in %s second(s)...', self.__wait_seconds)
                    time.sleep(self.__wait_seconds)
                    continue
                else:
                    raise


class RunConfigSchema(Schema):
    command = fields.String(required=True)
    dependencies = fields.List(fields.Nested(BundleDependencySchema), required=True)
    tag = fields.String(required=True)
    metadata = fields.Dict(missing=dict)


class MimicReplacementSchema(Schema):
    old = fields.String(validate=validate_uuid)
    new = fields.String(validate=validate_uuid)


class MimicConfigSchema(Schema):
    tag = fields.String(required=True)
    metadata = fields.Dict(missing=dict)
    mimic = fields.List(fields.Nested(MimicReplacementSchema), required=True)
    # How far up the ancestor tree do we look for replacements?
    depth = fields.Integer(missing=1)


class ScoreSpecSchema(Schema):
    name = fields.String(required=True)
    key = fields.String(required=True)


class ConfigSchema(Schema):
    max_submissions_per_period = fields.Integer(missing=1)
    max_submissions_total = fields.Integer(missing=1e10)
    refresh_period_seconds = fields.Integer(missing=60)
    max_leaderboard_size = fields.Integer(missing=10000)
    quota_period_seconds = fields.Integer(missing=24*60*60)  # default 1 day
    count_failed_submissions = fields.Boolean(missing=True)  # default count everything toward quota
    make_predictions_public = fields.Boolean(missing=False)  # default keep predictions private
    allow_orphans = fields.Boolean(missing=True)  # default allow leaderboard entries that no longer have corresponding submission bundles
    host = fields.Url(required=True)
    username = fields.String()
    password = fields.String()
    submission_tag = fields.String(required=True)
    log_worksheet_uuid = fields.String(validate=validate_uuid)
    predict = fields.Nested(MimicConfigSchema, required=True)
    evaluate = fields.Nested(RunConfigSchema, required=True)
    # Leaderboard sorted by the first key in this list
    score_specs = fields.List(fields.Nested(ScoreSpecSchema), required=True)
    # Gets passed directly to the output JSON
    metadata = fields.Dict(missing=dict)


class AuthHelper(object):
    REFRESH_BUFFER_SECONDS = 5 * 60

    def __init__(self, host, username, password):
        self.username = username
        self.password = password
        self.auth_handler = RestOAuthHandler(host)
        self.grant = None
        self.expires_at = None

    def get_access_token(self):
        if not self.grant or time.time() > self.expires_at - self.REFRESH_BUFFER_SECONDS:
            self.grant = self.auth_handler.generate_token('credentials',
                                                          self.username, self.password)
            if self.grant is None:
                raise PermissionError('Invalid username or password.')
            self.expires_at = time.time() + self.grant['expires_in']
        return self.grant['access_token']


PREDICT_RUN_PREFIX = 'predict-run-'


class Competition(object):
    """
    Internal data model:

    description of bundles hold leaderboard submission metadata serialized as JSON

    all prediction bundles are tagged with predict tag
    only the latest evaluation bundle for each submitter is tagged with evaluate tag

    the prediction bundles maintain the record of all submissions.
    """
    def __init__(self, config_path, output_path, leaderboard_only):
        self.config = self.load_config(config_path)
        self.output_path = output_path
        self.leaderboard_only = leaderboard_only
        auth = AuthHelper(
            self.config['host'],
            self.config.get('username') or raw_input('Username: '),
            self.config.get('password') or getpass.getpass('Password: '))

        # Remove credentials from config to prevent them from being copied
        # into the leaderboard file.
        self.config.pop('username', None)
        self.config.pop('password', None)

        self.client = ThrottledJsonApiClientWithRetry(self.config['host'], auth.get_access_token)
        self.should_stop = False

    @staticmethod
    def load_config(config_path):
        with open(config_path, 'r') as fp:
            config = json.load(fp)
        try:
            config = ConfigSchema(strict=True).load(config).data
        except ValidationError as e:
            print >> sys.stderr, 'Invalid config file:', e
            sys.exit(1)
        return config

    @staticmethod
    def get_competition_metadata(bundle):
        try:
            return json.loads(bundle['metadata']['description'])
        except ValueError:
            return None

    def clear_competition_metadata(self, bundle):
        bundle['metadata']['description'] = ''
        self.client.update('bundles', {
            'id': bundle['id'],
            'metadata': {'description': ''}
        })

    def ensure_log_worksheet_private(self):
        """
        Ensure that the leaderboard worksheet is private, so that all bundles
        created on it are automatically private.
        """
        if self.config['make_predictions_public']:
            return

        # Get public group info
        public = self.client.fetch('groups', 'public')

        # Set permissions
        self.client.create('worksheet-permissions', {
            'group': JsonApiRelationship('groups', public['id']),
            'worksheet': JsonApiRelationship('worksheets', self.config['log_worksheet_uuid']),
            'permission': 0,
        })

    def make_public_readable(self, bundle):
        # Get public group info
        public = self.client.fetch('groups', 'public')

        # Set permissions
        self.client.create('bundle-permissions', {
            'group': JsonApiRelationship('groups', public['id']),
            'bundle': JsonApiRelationship('bundles', bundle['id']),
            'permission': 1,
        })

    def untag(self, bundles, tag):
        self.client.update('bundles', [{
            'id': b['id'],
            'metadata': {'tags': [t for t in b['metadata']['tags'] if t != tag]}
        } for b in bundles])

    def collect_submissions(self):
        logger.debug("Collecting latest submissions")
        # Fetch all submissions
        all_submissions = self.client.fetch('bundles', params={
            'keywords': [
                'tags={submission_tag}'.format(**self.config),
                'created=.sort-',
                '.limit={max_leaderboard_size}'.format(**self.config),
            ]
        })

        # Drop all but the latest submission for each user.
        submissions = {}
        for bundle in reversed(all_submissions):
            owner_id = bundle['owner']['id']
            created = bundle['metadata']['created']
            if owner_id not in submissions or \
                    created > submissions[owner_id]['metadata']['created']:
                submissions[owner_id] = bundle

        # Fetch latest evaluation bundles
        last_tests = self.client.fetch('bundles', params={
            'keywords': [
                '.mine',  # don't allow others to forge evaluations
                'tags={evaluate[tag]}'.format(**self.config),
                '.limit={max_leaderboard_size}'.format(**self.config),
            ]
        })

        # Collect data in preparation for computing submission counts
        submission_times = defaultdict(list)  # map from submitter_user_id -> UNIX timestamps of submissions, sorted
        submission_ids = set()                # set of submission bundle uuids
        for eval_bundle in last_tests:
            submit_info = self.get_competition_metadata(eval_bundle)
            if submit_info is None:
                continue
            timestamp = eval_bundle['metadata']['created']
            submission_ids.add(submit_info['submit_id'])
            # Only count toward quota if not failed or configured to count failed submissions
            # if predict_bundle['state'] != State.FAILED or self.config['count_failed_submissions']:
            submission_times[submit_info['submitter_id']].append(timestamp)

        # Compute submission counts
        num_total_submissions = defaultdict(int)
        num_period_submissions = defaultdict(int)
        now = time.time()
        period_start = now - self.config['quota_period_seconds']
        for owner_id, timestamps in submission_times.items():
            # Count the total number of submissions
            num_total_submissions[owner_id] = len(timestamps)
            # Count the number of submissions in the past 24 hours
            num_period_submissions[owner_id] = sum(t > period_start for t in timestamps)

        # Drop submission if user has exceeded their quota
        for owner_id, bundle in submissions.items():
            # Drop submission if we already ran it before
            if bundle['id'] in submission_ids:
                logger.debug('Already mimicked last submission by '
                             '{owner[user_name]}.'.format(**bundle))
                del submissions[owner_id]
                continue

            if num_total_submissions[owner_id] >= self.config['max_submissions_total']:
                logger.debug(
                    "{owner[user_name]} exceeded quota "
                    "({used}/{allowed} total submissions)".format(
                        used=num_total_submissions[owner_id],
                        allowed=self.config['max_submissions_total'],
                        **bundle))
                del submissions[owner_id]
                continue

            if num_period_submissions[owner_id] >= self.config['max_submissions_per_period']:
                logger.debug(
                    "{owner[user_name]} exceeded quota "
                    "({used}/{allowed} submissions per day)".format(
                        used=num_period_submissions[owner_id],
                        allowed=self.config['max_submissions_per_period'],
                        **bundle))
                del submissions[owner_id]
                continue

        return submissions, num_total_submissions, num_period_submissions

    def run_prediction(self, submit_bundle):
        """
        Given a bundle tagged for submission, try to mimic the bundle with the
        evaluation data according to the prediction run specification.

        Returns the mimicked prediction bundle. (If the mimic created multiple
        bundles, then the one corresponding to the tagged submission bundle is
        returned.)

        Returns None if the submission does not meet requirements.
        """
        predict_bundle_name = PREDICT_RUN_PREFIX + submit_bundle['owner']['id']
        predict_config = self.config['predict']
        to_be_replaced = [spec['old'] for spec in predict_config['mimic']]
        replacements = [spec['new'] for spec in predict_config['mimic']]

        def find_mimicked(plan):
            for old_info, new_info in plan:
                if old_info['uuid'] == submit_bundle['uuid']:
                    return new_info
            return None

        metadata = {
            'tags': [predict_config['tag']],
        }
        metadata.update(predict_config['metadata'])
        mimic_args = {
            'client': self.client,
            'old_inputs': to_be_replaced,
            'old_output': submit_bundle['uuid'],
            'new_inputs': replacements,
            'new_output_name': predict_bundle_name,
            'worksheet_uuid': self.config['log_worksheet_uuid'],
            'depth': predict_config['depth'],
            'shadow': False,
            'metadata_update': metadata,
            'skip_prelude': True,
        }

        # Do dry run to check if the submission has the right dependencies.
        # If the submission bundle is not mimicked (i.e. not in the mimic plan),
        # that means that none of its ancestors are in the set of bundles that
        # we are trying to replace.
        if find_mimicked(mimic_bundles(dry_run=True, **mimic_args)) is None:
            logger.info(
                "Submission {uuid} by {owner[user_name]} is missing "
                "expected dependencies.".format(**submit_bundle))
            return None

        # Actually perform the mimic now
        predict_bundle = find_mimicked(mimic_bundles(dry_run=False, **mimic_args))
        assert predict_bundle is not None, "Unexpected error: couldn't find mimicked bundle in plan"
        return predict_bundle

    def run_evaluation(self, submit_bundle, predict_bundle):
        eval_bundle_name = '{owner[user_name]}-results'.format(**submit_bundle)

        # Untag any old evaluation run(s) for this submitter
        old_evaluations = self.client.fetch('bundles', params={
            'keywords': [
                '.mine',  # don't allow others to forge evaluations
                'tags={evaluate[tag]}'.format(**self.config),
                'name=' + eval_bundle_name,
                ]
        })
        if old_evaluations:
            self.untag(old_evaluations, self.config['evaluate']['tag'])

        # Create evaluation runs on the predictions with leaderboard tag
        # Build up metadata
        metadata = {
            'name': eval_bundle_name,
            'tags': [self.config['evaluate']['tag']],
            'description': json.dumps({
                'submit_id': submit_bundle['id'],
                'submitter_id': submit_bundle['owner']['id'],
                'predict_id': predict_bundle['id'],
            }),
        }
        metadata.update(self.config['evaluate']['metadata'])
        metadata = fill_missing_metadata(RunBundle, argparse.Namespace(), metadata)
        # Substitute in the prediction bundle UUID where required
        dependencies = []
        for dep_spec in self.config['evaluate']['dependencies']:
            dep = dep_spec.copy()
            dep['parent_uuid'] = dep['parent_uuid'].format(predict=predict_bundle['uuid'])
            dependencies.append(dep)
        # Create the bundle
        eval_bundle = self.client.create('bundles', {
            'bundle_type': 'run',
            'command': self.config['evaluate']['command'],
            'dependencies': dependencies,
            'metadata': metadata,
        }, params={'worksheet': self.config['log_worksheet_uuid']})
        self.make_public_readable(eval_bundle)
        return eval_bundle

    @staticmethod
    def _is_publicly_readable(bundle):
        for perm in bundle['group_permissions']:
            if perm['group_name'] == 'public':
                return perm['permission'] >= GROUP_OBJECT_PERMISSION_READ
        # No permissions on public group
        return False

    def _fetch_leaderboard(self):
        """
        Fetches the evaluation bundles tagged for the leaderboard, along with
        the corresponding submission bundles if they exist.

        :return: (eval_bundles, eval2submit) where eval_bundles is a list of the
                 evaluation bundles, and eval2submit is a dict mapping evaluation
                 bundle id to the original submission bundle. The id will not be
                 a key in eval2submit if a corresponding submission bundle does
                 not exist.
        """
        logger.debug('Fetching the leaderboard')
        # Fetch bundles on current leaderboard
        eval_bundles = self.client.fetch('bundles', params={
            'keywords': [
                '.mine',  # don't allow others to forge evaluations
                'tags={evaluate[tag]}'.format(**self.config),
                '.limit={max_leaderboard_size}'.format(**self.config),
            ]
        })
        eval_bundles = {b['id']: b for b in eval_bundles}

        # Build map from submission bundle id => eval bundle
        submit2eval = {}
        for bundle in eval_bundles.itervalues():
            submit_info = self.get_competition_metadata(bundle)
            # Eval bundles that are missing competition metadata are simply
            # skipped; code downstream must handle the case where eval2submit
            # does not contain an entry for a given eval bundle
            if submit_info is not None:
                submit2eval[submit_info['submit_id']] = bundle

        # Fetch the original submission bundles.
        # A NotFoundError will be thrown if a bundle no longer exists.
        # We will remove that submission from the leaderboard, and keep
        # trying until there are no more deleted bundles.
        logger.debug('Fetching corresponding original submission bundles')
        while True:
            if len(eval_bundles) == 0:
                submit_bundles = {}
                break
            try:
                uuids = submit2eval.keys()
                submit_bundles = []
                for start in range(0, len(uuids), 50):
                    end = start + 50
                    submit_bundles.extend(self.client.fetch('bundles', params={
                        'specs': uuids[start:end],
                        'worksheet': self.config['log_worksheet_uuid'],
                    }))
                break
            except NotFoundError as e:
                missing_submit_uuid = re.search(UUID_STR, e.message).group(0)
                eval_uuid = submit2eval[missing_submit_uuid]['id']

                # If a submission bundle (missing_uuid) has been deleted...
                if self.config['allow_orphans']:
                    # Just clear the competition metadata on the eval bundle,
                    # thus removing the reference to the original submit bundle
                    logger.info("Clearing reference to deleted submission %s", missing_submit_uuid)
                    self.clear_competition_metadata(eval_bundles[eval_uuid])
                    pass
                else:
                    # Untag and remove entry from the leaderboard entirely
                    logger.info("Removing submission %s", missing_submit_uuid)
                    self.untag([submit2eval[missing_submit_uuid]], self.config['evaluate']['tag'])
                    del eval_bundles[eval_uuid]

                # Drop from list of submit bundles and try fetching batch again
                del submit2eval[missing_submit_uuid]
                continue

        # Build map from eval bundle id => submission bundle
        eval2submit = {}
        for submit_bundle in submit_bundles:
            eval_bundle = submit2eval[submit_bundle['id']]
            eval2submit[eval_bundle['id']] = submit_bundle

        return eval_bundles, eval2submit

    def fetch_scores(self, eval_bundles):
        """
        Fetch scores from server.

        Returns dict with (bundle_id, score_spec_name) as the key and the score
        value as the value.
        """
        # Extract score specs
        scores = {}
        queries = []
        keys = []
        for bundle in eval_bundles.itervalues():
            if bundle['state'] == State.READY:
                for spec in self.config['score_specs']:
                    queries.append((bundle['id'], spec['key'], None))
                    keys.append((bundle['id'], spec['name']))
            else:
                # All scores are None if the bundle failed
                scores[bundle['id']] = {spec['name']: None for spec in self.config['score_specs']}

        # Actually fetch score values
        results = self.client.interpret_file_genpaths(queries)
        for (bundle_id, spec_name), value in zip(keys, results):
            if bundle_id not in scores:
                scores[bundle_id] = {}
            scores[bundle_id][spec_name] = value

        return scores

    def generate_leaderboard(self, num_total_submissions, num_period_submissions):
        eval_bundles, eval2submit = self._fetch_leaderboard()
        scores = self.fetch_scores(eval_bundles)

        # Build leaderboard table
        logger.debug('Fetching scores and building leaderboard table')
        leaderboard = []
        for bundle in eval_bundles.itervalues():
            if bundle['id'] in eval2submit:
                submit_bundle = eval2submit[bundle['id']]
                submission_info = {
                    # Can include any information we want from the submission
                    # within bounds of reason (since submitter may want to
                    # keep some of the metadata private).
                    'description': submit_bundle['metadata']['description'],
                    'public': self._is_publicly_readable(submit_bundle),
                    'user_name': submit_bundle['owner']['user_name'],
                    'num_total_submissions': num_total_submissions[submit_bundle['owner']['id']],
                    'num_period_submissions': num_period_submissions[submit_bundle['owner']['id']],
                    'created': submit_bundle['metadata']['created'],
                }
            else:
                # If there isn't a corresponding submit bundle, use some sane
                # defaults based on just the eval bundle.
                submission_info = {
                    'description': bundle['metadata']['description'],
                    'public': None,
                    'user_name': None,
                    'num_total_submissions': 0,
                    'num_period_submissions': 0,
                    'created': bundle['metadata']['created'],
                }

            leaderboard.append({
                'bundle': bundle,
                'scores': scores,
                'submission': submission_info,
            })

        # Sort by the scores, descending
        leaderboard.sort(
            key=lambda e: tuple(e['scores'][spec['name']] for spec in self.config['score_specs']),
            reverse=True
        )

        # Write table to JSON file along with other data
        output = {
            'leaderboard': leaderboard,
            'config': self.config,
            'updated': time.time(),
        }
        with open(self.output_path, 'w') as fp:
            fp.write(pretty_json(output))

        logger.debug('Wrote leaderboard at {.output_path}'.format(self))

    def run_once(self):
        submissions, num_total_submissions, num_period_submissions = self.collect_submissions()
        if not submissions:
            logger.debug('No new submissions.')

        if not self.leaderboard_only:
            for owner_id, submit_bundle in submissions.items():
                logger.info("Mimicking submission for "
                            "{owner[user_name]}".format(**submit_bundle))
                predict_bundle = self.run_prediction(submit_bundle)
                if predict_bundle is None:
                    logger.info("Aborting submission for "
                                "{owner[user_name]}".format(**submit_bundle))
                    continue
                self.run_evaluation(submit_bundle, predict_bundle)
                logger.info("Finished mimicking submission for "
                            "{owner[user_name]}".format(**submit_bundle))
                num_total_submissions[owner_id] += 1
                num_period_submissions[owner_id] += 1

        self.generate_leaderboard(num_total_submissions, num_period_submissions)

    def run(self):
        self.ensure_log_worksheet_private()
        logger.info('Starting competition daemon...')
        while not self.should_stop:
            try:
                self.run_once()
            except:
                traceback.print_exc()

            if self.should_stop:
                break

            time.sleep(self.config['refresh_period_seconds'])

    def stop(self):
        logger.info('Stopping competition daemon...')
        self.should_stop = True


def main():
    # Support all configs as command line arguments too
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('config_file',
                        help='JSON file containing configurations.')
    parser.add_argument('output_path',
                        help='path to write json file containing leaderboard.')
    parser.add_argument('-l', '--leaderboard-only', action='store_true',
                        help='Generate a new leaderboard but without creating any new runs.')
    parser.add_argument('-d', '--daemon', action='store_true',
                        help='Run as a daemon. (By default only runs once.)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Output verbose log messages.')
    args = parser.parse_args()
    logging.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s',
                        level=(logging.DEBUG if args.verbose else logging.INFO))
    comp = Competition(args.config_file, args.output_path, args.leaderboard_only)
    if args.daemon:
        # Catch interrupt signals so that eval loop doesn't get interrupted in the
        # middle of a series of actions and leave things in an inconsistent state.
        for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
            signal.signal(sig, lambda signup, frame: comp.stop())
        comp.run()
    else:
        logger.info('Running batch competition evaluation')
        comp.ensure_log_worksheet_private()
        comp.run_once()


if __name__ == '__main__':
    main()
