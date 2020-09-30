"""
Competition leaderboard evaluation daemon.

    1. Find bundles tagged with {submission_tag} and filter them.
    2. Run the {predict} command with the submitted bundle to generate
       predictions on the test set.
    3. Tag the resulting test run bundle with {predict.tag}, untagging
       any previous test run bundles for the same submitter.
    4. Run {evaluate} command with the test run bundle.
    5. Tag the resulting evaluation bundle with {evaluate.tag}, untagging
       any previous evaluation bundles for the same submitter.

If in daemon mode, performs the above steps in a loop every
{refresh_period_seconds} seconds. Otherwise, just runs through them once.

All bundles created by this daemon are added to {log_worksheet_uuid}.
Each user will be limited to {max_submissions_per_period} every
{quota_period_seconds}, and {max_submissions_total} ever.

The following string substitutions will be made in the dependency specs:

    {predict} => UUID of the resulting test run bundle

Config file keys:

"""
import argparse
import getpass
import json
import logging
import random
import re
import signal
import sys
import time
import traceback
from collections import defaultdict, namedtuple

from marshmallow import Schema, fields, ValidationError, missing
import yaml

from codalab.bundles import RunBundle
from codalab.common import NotFoundError, PermissionError
from codalab.client.json_api_client import JsonApiClient, JsonApiRelationship, JsonApiException
from codalab.lib.bundle_util import mimic_bundles
from codalab.lib.metadata_util import fill_missing_metadata
from codalab.lib.print_util import pretty_json
from codalab.lib.spec_util import UUID_STR
from codalab.model.tables import GROUP_OBJECT_PERMISSION_READ
from codalab.rest.schemas import BundleDependencySchema, validate_uuid
from codalab.server.auth import RestOAuthHandler
from codalab.worker.bundle_state import State

sys.path.append('.')
logger = logging.getLogger(__name__)


class JsonApiClientWithRetry(JsonApiClient):
    """
    JsonApiClient with a retry block around every request.
    """

    def __init__(self, *args, **kwargs):
        self.__num_retries = kwargs.pop('num_retries', 4)
        self.__wait_seconds = kwargs.pop('wait_seconds', 1)
        super(JsonApiClientWithRetry, self).__init__(*args, **kwargs)

    def _make_request(self, *args, **kwargs):
        num_retries_left = self.__num_retries
        wait_seconds = self.__wait_seconds
        while True:
            try:
                return super(JsonApiClientWithRetry, self)._make_request(*args, **kwargs)
            except JsonApiException:
                if num_retries_left > 0:
                    num_retries_left -= 1
                    logger.exception(
                        'Request failed, retrying in %s second(s)...', self.__wait_seconds
                    )
                    time.sleep(wait_seconds)
                    wait_seconds *= 5  # exponential backoff
                    wait_seconds += random.uniform(-1, 1)  # small jitter
                    continue
                else:
                    raise


class RunConfigSchema(Schema):
    command = fields.String(required=True, metadata='bash command')
    dependencies = fields.List(fields.Nested(BundleDependencySchema), required=True)
    tag = fields.String(
        missing='competition-evaluate', metadata='how to tag new evaluation bundles'
    )
    metadata = fields.Dict(missing={}, metadata='metadata keys for new evaluation bundles')


class MimicReplacementSchema(Schema):
    old = fields.String(
        validate=validate_uuid, required=True, metadata='uuid of bundle to swap out'
    )
    new = fields.String(validate=validate_uuid, required=True, metadata='uuid of bundle to swap in')


class MimicConfigSchema(Schema):
    tag = fields.String(missing='competition-predict', metadata='how to tag new prediction bundles')
    metadata = fields.Dict(missing={}, metadata='overwrite metadata keys in mimicked bundles')
    depth = fields.Integer(
        missing=10, metadata='how far up the dependency tree to look for replacements'
    )
    mimic = fields.List(fields.Nested(MimicReplacementSchema), required=True)


class ScoreSpecSchema(Schema):
    name = fields.String(required=True, metadata='name of the score (for convenience)')
    key = fields.String(
        required=True,
        metadata='target path of the score in the evaluate bundle (e.g. \"/results.json:f1_score\")',
    )


class ConfigSchema(Schema):
    max_submissions_per_period = fields.Integer(
        missing=1, metadata='number of submissions allowed per user per quota period'
    )
    max_submissions_total = fields.Integer(
        missing=10000, metadata='number of submissions allowed per user for eternity'
    )
    refresh_period_seconds = fields.Integer(
        missing=60,
        metadata='(for daemon mode) number of seconds to wait before checking for new submissions again',
    )
    max_leaderboard_size = fields.Integer(
        missing=10000, metadata='maximum number of bundles you expect to have on the log worksheet'
    )
    quota_period_seconds = fields.Integer(
        missing=24 * 60 * 60, metadata='window size for the user submission quotas in seconds'
    )
    count_failed_submissions = fields.Boolean(
        missing=True, metadata='whether to count failed evaluations toward submission quotas'
    )
    make_predictions_public = fields.Boolean(
        missing=False, metadata='whether to make newly-created prediction bundles publicly readable'
    )
    allow_orphans = fields.Boolean(
        missing=True,
        metadata='whether to keep leaderboard entries that no longer have corresponding submission bundles',
    )
    allow_multiple_models = fields.Boolean(
        missing=False, metadata='whether to distinguish multiple models per user by bundle name'
    )
    host = fields.String(
        missing='https://worksheets.codalab.org',
        metadata='address of the CodaLab instance to connect to',
    )
    username = fields.String(metadata='username for CodaLab account to use')
    password = fields.String(metadata='password for CodaLab account to use')
    submission_tag = fields.String(required=True, metadata='tag for searching for submissions')
    log_worksheet_uuid = fields.String(
        validate=validate_uuid, metadata='UUID of worksheet to create new bundles in'
    )
    predict = fields.Nested(MimicConfigSchema, required=True)
    evaluate = fields.Nested(RunConfigSchema, required=True)
    # Leaderboard sorted by the first key in this list
    score_specs = fields.List(fields.Nested(ScoreSpecSchema), required=True)
    # Gets passed directly to the output JSON
    metadata = fields.Dict(
        missing={}, metadata='additional metadata to include in the leaderboard file'
    )


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
            self.grant = self.auth_handler.generate_token(
                'credentials', self.username, self.password
            )
            if self.grant is None:
                raise PermissionError('Invalid username or password.')
            self.expires_at = time.time() + self.grant['expires_in']
        return self.grant['access_token']


SubmissionKey = namedtuple('SubmissionKey', 'owner_id bundle_name')


class Competition(object):
    """
    Internal data model:

    description of bundles hold leaderboard submission metadata serialized as JSON

    all prediction bundles are tagged with predict tag
    only the latest evaluation bundle for each submitter is tagged with evaluate tag

    the prediction bundles maintain the record of all submissions.
    """

    def __init__(self, config_path, output_path, leaderboard_only):
        self.config = self._load_config(config_path)
        self.output_path = output_path
        self.leaderboard_only = leaderboard_only
        auth = AuthHelper(
            self.config['host'],
            self.config.get('username') or input('Username: '),
            self.config.get('password') or getpass.getpass('Password: '),
        )

        # Remove credentials from config to prevent them from being copied
        # into the leaderboard file.
        self.config.pop('username', None)
        self.config.pop('password', None)

        self.client = JsonApiClientWithRetry(self.config['host'], auth.get_access_token)
        self.should_stop = False

    @staticmethod
    def _load_config(config_path):
        with open(config_path, 'r') as fp:
            config = yaml.safe_load(fp)
        try:
            config = ConfigSchema(strict=True).load(config).data
        except ValidationError as e:
            print('Invalid config file:', e, file=sys.stderr)
            sys.exit(1)
        return config

    @staticmethod
    def _get_competition_metadata(bundle):
        """
        Load competition-specific metadata from a bundle dict.
        Returns metadata dict, or None if no metadata found.
        """
        try:
            return json.loads(bundle['metadata']['description'])
        except ValueError:
            return None

    def _clear_competition_metadata(self, bundle):
        """
        Clears competition-specific metadata from a bundle on the server.
        """
        bundle['metadata']['description'] = ''
        self.client.update('bundles', {'id': bundle['id'], 'metadata': {'description': ''}})

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
        self.client.create(
            'worksheet-permissions',
            {
                'group': JsonApiRelationship('groups', public['id']),
                'worksheet': JsonApiRelationship('worksheets', self.config['log_worksheet_uuid']),
                'permission': 0,
            },
        )

    def _make_public_readable(self, bundle):
        """
        Make the given bundle readable to the public.
        """
        # Get public group info
        public = self.client.fetch('groups', 'public')

        # Set permissions
        self.client.create(
            'bundle-permissions',
            {
                'group': JsonApiRelationship('groups', public['id']),
                'bundle': JsonApiRelationship('bundles', bundle['id']),
                'permission': 1,
            },
        )

    def _untag(self, bundles, tag):
        """
        Remove the given `tag` from each of the bundles in `bundles`.
        """
        self.client.update(
            'bundles',
            [
                {
                    'id': b['id'],
                    'metadata': {'tags': [t for t in b['metadata']['tags'] if t != tag]},
                }
                for b in bundles
            ],
        )

    def _fetch_latest_submissions(self):
        # Fetch all submissions
        all_submissions = self.client.fetch(
            'bundles',
            params={
                'keywords': [
                    'tags={submission_tag}'.format(**self.config),
                    'created=.sort-',
                    '.limit={max_leaderboard_size}'.format(**self.config),
                ],
                'include': ['owner'],
            },
        )

        # Drop all but the latest submission for each user
        # (or for each model, as distinguished by the bundle name)
        submissions = {}
        for bundle in reversed(all_submissions):
            owner_id = bundle['owner']['id']
            created = bundle['metadata']['created']

            # If multiple models are allowed for each user, subsect by submission bundle name as well
            if self.config['allow_multiple_models']:
                key = SubmissionKey(owner_id, bundle['metadata']['name'])
            else:
                key = SubmissionKey(owner_id, None)

            if key not in submissions or created > submissions[key]['metadata']['created']:
                submissions[key] = bundle
        return submissions

    def _fetch_submission_history(self):
        # Fetch latest evaluation bundles
        last_tests = self.client.fetch(
            'bundles',
            params={
                'keywords': [
                    '.mine',  # don't allow others to forge evaluations
                    'tags={evaluate[tag]}'.format(**self.config),
                    '.limit={max_leaderboard_size}'.format(**self.config),
                ]
            },
        )

        # Collect data in preparation for computing submission counts
        submission_times = defaultdict(
            list
        )  # map from submitter_user_id -> UNIX timestamps of submissions, sorted
        previous_submission_ids = set()  # set of submission bundle uuids
        for eval_bundle in last_tests:
            submit_info = self._get_competition_metadata(eval_bundle)
            if submit_info is None:
                continue
            timestamp = eval_bundle['metadata']['created']
            previous_submission_ids.add(submit_info['submit_id'])
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

        return previous_submission_ids, num_total_submissions, num_period_submissions

    def _filter_submissions(
        self, submissions, previous_submission_ids, num_total_submissions, num_period_submissions
    ):
        # Drop submission if user has exceeded their quota
        for key, bundle in list(submissions.items()):
            # Drop submission if we already ran it before
            if bundle['id'] in previous_submission_ids:
                logger.debug(
                    'Already mimicked last submission by ' '{owner[user_name]}.'.format(**bundle)
                )
                del submissions[key]
                continue

            if num_total_submissions[key.owner_id] >= self.config['max_submissions_total']:
                logger.debug(
                    "{owner[user_name]} exceeded quota "
                    "({used}/{allowed} total submissions)".format(
                        used=num_total_submissions[key.owner_id],
                        allowed=self.config['max_submissions_total'],
                        **bundle
                    )
                )
                del submissions[key]
                continue

            if num_period_submissions[key.owner_id] >= self.config['max_submissions_per_period']:
                logger.debug(
                    "{owner[user_name]} exceeded quota "
                    "({used}/{allowed} submissions per day)".format(
                        used=num_period_submissions[key.owner_id],
                        allowed=self.config['max_submissions_per_period'],
                        **bundle
                    )
                )
                del submissions[key]
                continue
        return submissions

    def collect_submissions(self):
        """
        Collect all valid submissions, along with the latest quota counts.
        """
        logger.debug("Collecting latest submissions")
        submissions = self._fetch_latest_submissions()
        (
            previous_submission_ids,
            num_total_submissions,
            num_period_submissions,
        ) = self._fetch_submission_history()
        submissions = self._filter_submissions(
            submissions, previous_submission_ids, num_total_submissions, num_period_submissions
        )
        return list(submissions.values()), num_total_submissions, num_period_submissions

    def run_prediction(self, submit_bundle):
        """
        Given a bundle tagged for submission, try to mimic the bundle with the
        evaluation data according to the prediction run specification.

        Returns the mimicked prediction bundle. (If the mimic created multiple
        bundles, then the one corresponding to the tagged submission bundle is
        returned.)

        Returns None if the submission does not meet requirements.
        """
        predict_bundle_name = '{owner[user_name]}-{metadata[name]}-predict'.format(**submit_bundle)
        predict_config = self.config['predict']
        to_be_replaced = [spec['old'] for spec in predict_config['mimic']]
        replacements = [spec['new'] for spec in predict_config['mimic']]

        def find_mimicked(plan):
            for old_info, new_info in plan:
                if old_info['uuid'] == submit_bundle['uuid']:
                    return new_info
            return None

        metadata = {'tags': [predict_config['tag']]}
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
            'metadata_override': metadata,
            'skip_prelude': True,
        }

        # Do dry run to check if the submission has the right dependencies.
        # If the submission bundle is not mimicked (i.e. not in the mimic plan),
        # that means that none of its ancestors are in the set of bundles that
        # we are trying to replace.
        if find_mimicked(mimic_bundles(dry_run=True, **mimic_args)) is None:
            logger.info(
                "Submission {uuid} by {owner[user_name]} is missing "
                "expected dependencies.".format(**submit_bundle)
            )
            return None

        # Actually perform the mimic now
        predict_bundle = find_mimicked(mimic_bundles(dry_run=False, **mimic_args))
        assert predict_bundle is not None, "Unexpected error: couldn't find mimicked bundle in plan"
        return predict_bundle

    def run_evaluation(self, submit_bundle, predict_bundle):
        eval_bundle_name = '{owner[user_name]}-{metadata[name]}-results'.format(**submit_bundle)

        # Untag any old evaluation run(s) for this submitter
        old_evaluations = self.client.fetch(
            'bundles',
            params={
                'keywords': [
                    '.mine',  # don't allow others to forge evaluations
                    'tags={evaluate[tag]}'.format(**self.config),
                    'name=' + eval_bundle_name,
                ]
            },
        )
        if old_evaluations:
            self._untag(old_evaluations, self.config['evaluate']['tag'])

        # Create evaluation runs on the predictions with leaderboard tag
        # Build up metadata
        metadata = {
            'name': eval_bundle_name,
            'tags': [self.config['evaluate']['tag']],
            'description': json.dumps(
                {
                    'submit_id': submit_bundle['id'],
                    'submitter_id': submit_bundle['owner']['id'],
                    'predict_id': predict_bundle['id'],
                }
            ),
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
        eval_bundle = self.client.create(
            'bundles',
            {
                'bundle_type': 'run',
                'command': self.config['evaluate']['command'],
                'dependencies': dependencies,
                'metadata': metadata,
            },
            params={'worksheet': self.config['log_worksheet_uuid']},
        )
        self._make_public_readable(eval_bundle)
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

        :return: (eval_bundles, eval2submit) where eval_bundles is a dict mapping
                 evaluation bundle ids to the evaluation bundles themselves, and
                 eval2submit is a dict mapping evaluation bundle id to the
                 original submission bundle. The id will not be a key in
                 eval2submit if a corresponding submission bundle does not exist.
        """
        logger.debug('Fetching the leaderboard')
        # Fetch bundles on current leaderboard
        eval_bundles = self.client.fetch(
            'bundles',
            params={
                'keywords': [
                    '.mine',  # don't allow others to forge evaluations
                    'tags={evaluate[tag]}'.format(**self.config),
                    '.limit={max_leaderboard_size}'.format(**self.config),
                ]
            },
        )
        eval_bundles = {b['id']: b for b in eval_bundles}

        # Build map from submission bundle id => eval bundle
        submit2eval = {}
        for eval_id, eval_bundle in eval_bundles.items():
            meta = self._get_competition_metadata(eval_bundle)
            # Eval bundles that are missing competition metadata are simply
            # skipped; code downstream must handle the case where eval2submit
            # does not contain an entry for a given eval bundle
            if meta is not None:
                # Allow manual hiding
                if meta.get('hide', False):
                    del eval_bundles[eval_id]
                else:
                    submit2eval[meta['submit_id']] = eval_bundle

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
                uuids = list(submit2eval.keys())
                submit_bundles = []
                for start in range(0, len(uuids), 50):
                    end = start + 50
                    submit_bundles.extend(
                        self.client.fetch(
                            'bundles',
                            params={
                                'specs': uuids[start:end],
                                'worksheet': self.config['log_worksheet_uuid'],
                                'include': ['owner', 'group_permissions'],
                            },
                        )
                    )
                break
            except NotFoundError as e:
                missing_submit_uuid = re.search(UUID_STR, str(e)).group(0)
                eval_uuid = submit2eval[missing_submit_uuid]['id']

                # If a submission bundle (missing_uuid) has been deleted...
                if self.config['allow_orphans']:
                    # Just clear the competition metadata on the eval bundle,
                    # thus removing the reference to the original submit bundle
                    logger.info("Clearing reference to deleted submission %s", missing_submit_uuid)
                    self._clear_competition_metadata(eval_bundles[eval_uuid])
                    pass
                else:
                    # Untag and remove entry from the leaderboard entirely
                    logger.info("Removing submission %s", missing_submit_uuid)
                    self._untag([submit2eval[missing_submit_uuid]], self.config['evaluate']['tag'])
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

    def _fetch_scores(self, eval_bundles):
        """
        Fetch scores from server.

        Returns dict with (bundle_id, score_spec_name) as the key and the score
        value as the value.
        """
        # Extract score specs
        scores = {}
        queries = []
        keys = []
        for bundle in eval_bundles.values():
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
        scores = self._fetch_scores(eval_bundles)

        # Build leaderboard table
        logger.debug('Fetching scores and building leaderboard table')
        leaderboard = []
        for eval_bundle in eval_bundles.values():
            meta = self._get_competition_metadata(eval_bundle)
            if eval_bundle['id'] in eval2submit:
                submit_bundle = eval2submit[eval_bundle['id']]
                submission_info = {
                    # Can include any information we want from the submission
                    # within bounds of reason (since submitter may want to
                    # keep some of the metadata private).
                    'description': meta.get('description', None)
                    or submit_bundle['metadata']['description'],  # Allow description override
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
                    'description': eval_bundle['metadata']['description'],
                    'public': None,
                    'user_name': None,
                    'num_total_submissions': 0,
                    'num_period_submissions': 0,
                    'created': eval_bundle['metadata']['created'],
                }
            leaderboard.append(
                {
                    'bundle': eval_bundle,
                    'scores': scores[eval_bundle['id']],
                    'submission': submission_info,
                }
            )

        # Sort by the scores, descending
        leaderboard.sort(
            key=lambda e: [
                (e['scores'][spec['name']] is not None, e['scores'][spec['name']])
                for spec in self.config['score_specs']
            ],
            reverse=True,
        )

        # Write table to JSON file along with other data
        output = {'leaderboard': leaderboard, 'config': self.config, 'updated': time.time()}
        with open(self.output_path, 'w') as fp:
            fp.write(pretty_json(output))

        logger.debug('Wrote leaderboard at {.output_path}'.format(self))

    def run_once(self):
        submissions, num_total_submissions, num_period_submissions = self.collect_submissions()
        if not submissions:
            logger.debug('No new submissions.')

        if not self.leaderboard_only:
            for submit_bundle in submissions:
                logger.info(
                    "Mimicking submission for " "{owner[user_name]}".format(**submit_bundle)
                )
                predict_bundle = self.run_prediction(submit_bundle)
                if predict_bundle is None:
                    logger.info(
                        "Aborting submission for " "{owner[user_name]}".format(**submit_bundle)
                    )
                    continue
                self.run_evaluation(submit_bundle, predict_bundle)
                logger.info(
                    "Finished mimicking submission for "
                    "{owner[user_name]}".format(**submit_bundle)
                )

                # Update local counts for the leaderboard
                owner_id = submit_bundle['owner']['id']
                num_total_submissions[owner_id] += 1
                num_period_submissions[owner_id] += 1

        self.generate_leaderboard(num_total_submissions, num_period_submissions)

    def run(self):
        self.ensure_log_worksheet_private()
        logger.info('Starting competition daemon...')
        while not self.should_stop:
            try:
                self.run_once()
            except Exception:
                traceback.print_exc()

            if self.should_stop:
                break

            time.sleep(self.config['refresh_period_seconds'])

    def stop(self):
        logger.info('Stopping competition daemon...')
        self.should_stop = True


def generate_description():
    def display_schema(schema, doc, indent, first_indent=None):
        saved_indent = indent
        if first_indent is not None:
            indent = first_indent
        for field_name, field in schema._declared_fields.items():
            field_help = field.metadata.get('metadata', '')
            field_class = field.__class__
            if field_class is fields.Nested:
                doc += indent + '%s:\n' % field_name
                doc = display_schema(field.nested, doc, (indent + '  '))
            elif field_class is fields.List:
                doc += indent + '%s:\n' % field_name
                doc = display_schema(
                    field.container.nested, doc, (indent + '    '), first_indent=(indent + '  - ')
                )
                doc += indent + '  - ...\n'
            else:
                field_type = field.__class__.__name__.lower()
                if field.missing is missing and field.required:
                    doc += indent + '%s: %s, %s [required]\n' % (field_name, field_type, field_help)
                elif field.missing is missing and not field.required:
                    doc += indent + '%s: %s, %s\n' % (field_name, field_type, field_help)
                else:
                    doc += indent + '%s: %s, %s [default: %s]\n' % (
                        field_name,
                        field_type,
                        field_help,
                        json.dumps(field.missing).strip(),
                    )
            indent = saved_indent
        return doc

    return display_schema(ConfigSchema, __doc__, ' ' * 4)


def main():
    # Support all configs as command line arguments too
    parser = argparse.ArgumentParser(
        description=generate_description(), formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('config_file', help='YAML/JSON file containing configurations.')
    parser.add_argument('output_path', help='path to write JSON file containing leaderboard.')
    parser.add_argument(
        '-l',
        '--leaderboard-only',
        action='store_true',
        help='Generate a new leaderboard but without creating any new runs.',
    )
    parser.add_argument(
        '-d', '--daemon', action='store_true', help='Run as a daemon. (By default only runs once.)'
    )
    parser.add_argument('-v', '--verbose', action='store_true', help='Output verbose log messages.')
    args = parser.parse_args()
    logging.basicConfig(
        format='[%(levelname)s] %(asctime)s: %(message)s',
        level=(logging.DEBUG if args.verbose else logging.INFO),
    )
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
