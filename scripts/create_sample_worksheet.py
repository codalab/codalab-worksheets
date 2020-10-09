import argparse
import os
import random
import re
import string
import time

from scripts.test_util import cleanup, run_command

"""
Script to create small and large sample worksheets in any instance to stress test the front end. The purpose of
the small worksheet is to test all features CodaLab offers on the front end. The large worksheet is a much bigger
version of the small worksheet and its purpose is to push the limit and stress test the frontend rendering capabilities.
"""


class SampleWorksheet:
    TAG = 'codalab-sample-worksheet'

    _WORKSHEET_FILE_PATH = '/tmp/sample-worksheet-temp.txt'
    _TEX_AND_MATH = (
        'The loss minimization framework is to cast learning as an optimization problem. We are estimating (fitting or '
        'learning) $\mathbf w$ using $ \mathcal{D}_\\text{train}$. A loss function $ \\text{Loss}(x, y, \mathbf w) $ '
        'quantifies how unhappy you would be if you used $\mathbf w$ to make a prediction on $x$ when the correct '
        'output is $y$. This is the object we want to minimize. Below is minimizing training loss across all training '
        'examples. Note that $ \\text{TrainLoss}(\mathbf w) $ is just the average of loss for all training examples.\n'
        '\n$$ \\text{TrainLoss}(\mathbf w) = \\frac{1}{| \mathcal{D}_{\\text{train}} |} \sum_{(x,y) \in \mathcal{D}'
        '_{\\text{train}}}\\text{Loss}(x, y, \mathbf w) \\ \n\min_{\mathbf w \in \mathbb{R}^d} \\text{TrainLoss}(\mathbf w)$$'
    )

    _NAME_REGEX = '[\s\S]{0,100}'
    _TEXT_REGEX = '[\s\S]{0,1000}'

    _FULL_UUID_REGEX = '0x[a-z0-9]{32}'
    _PARTIAL_UUID_REGEX = '0x[a-z0-9]{6}'
    """The list of bundle states is populated from :ref:`codalab.worker.bundle_state`."""
    _BUNDLE_STATE_REGEX = (
        '(uploading|created|staged|making|starting|preparing|running'
        '|finalizing|ready|failed|killed|worker_offline)'
    )
    _PERMISSION_REGEX = '(read|all|none)'

    _SIZE_REGEX = '[0-9.]{0,10}[tkgmb]{0,2}'
    _IMAGE_REGEX = '\[Image\]'
    _GRAPH_REGEX = '\[Graph\]'
    _SCHEMA_REGEX = '\[SchemaBlock\]'

    def __init__(self, cl, large=False, preview_mode=False):
        # For simplicity, reference a set number of entities for each section of the small and large worksheet.
        if large:
            self._description = 'large'
            self._entities_count = 100
        else:
            self._description = 'small'
            self._entities_count = 3
        self._cl = cl
        self._preview_mode = preview_mode
        self._worksheet_name = 'cl_{}_worksheet'.format(self._description)
        self._content = []

        # For testing, _expected_line holds the expected regex pattern for each line of the worksheet
        self._expected_lines = []

    def create(self):
        # Skip creating a sample worksheet if one already exists
        worksheet_uuids = run_command(
            [self._cl, 'wsearch', self._worksheet_name, '--uuid-only']
        ).split('\n')
        if re.match(SampleWorksheet._FULL_UUID_REGEX, worksheet_uuids[0]):
            print(
                'There is already an existing {} with UUID {}. Skipping creating a sample worksheet...'.format(
                    self._worksheet_name, worksheet_uuids[0]
                )
            )
            return

        print('Creating a {} worksheet...'.format(self._description))
        self._create_dependencies()
        self._add_introduction()
        self._add_worksheet_references()
        self._add_bundle_references()
        self._add_schemas()
        self._add_display_modes()
        self._add_search()
        self._add_invalid_directives()
        self._add_rendering_logic()
        self._create_sample_worksheet()
        print('Done.')

    def test_print(self):
        self._wait_for_bundles_to_finish()
        print('\n\nValidating output of cl print {}...'.format(self._worksheet_name))
        output_lines = run_command([self._cl, 'print', self._worksheet_name]).split('\n')
        has_error = False
        for i in range(len(self._expected_lines)):
            line = str(i + 1).zfill(5)
            if re.match(self._expected_lines[i], output_lines[i]):
                print('\x1b[1;34m{}| {}\x1b[0m'.format(line, output_lines[i]))
            else:
                has_error = True
                # Output mismatch message in red
                print(
                    '\033[91m{}| {} EXPECTED: {} \033[0m'.format(
                        line, output_lines[i], self._expected_lines[i]
                    )
                )

        assert not has_error
        print('Finished validating content of the sample worksheet...')
        print('Success.')

    def _create_dependencies(self):
        if self._preview_mode:
            # When in preview mode, search for existing bundles and worksheets instead of creating new ones
            random_worksheets = run_command(
                [self._cl, 'wsearch', '.limit=%d' % self._entities_count, '--uuid-only']
            ).split('\n')
            self._valid_worksheets = random_worksheets
            self._private_worksheets = random_worksheets
            random_bundles = run_command(
                [
                    self._cl,
                    'search',
                    'state=ready',
                    '.limit=%d' % self._entities_count,
                    '--uuid-only',
                ]
            ).split('\n')
            self._valid_bundles = random_bundles
            self._private_bundles = random_bundles
            return

        self._valid_worksheets = []
        self._private_worksheets = []
        self._valid_bundles = []
        self._private_bundles = []
        for _ in range(self._entities_count):
            # Create valid worksheets with a bundle each for the sample worksheet to reference
            id = self._random_id()
            name = 'valid_worksheet_%s' % id
            title = 'Other Worksheet %s' % id
            self._valid_worksheets.append(self._create_tagged_worksheet(name, title))
            uuid = run_command(
                [
                    self._cl,
                    'run',
                    '--request-memory',
                    '10m',
                    '--request-docker-image',
                    'python:3.6.10-slim-buster',
                    'echo codalab rules!',
                    '--tags=%s' % SampleWorksheet.TAG,
                ]
            )
            self._valid_bundles.append(uuid)
            # Create a valid private worksheet and a bundle each
            name = 'valid_private_worksheet_%s' % id
            title = 'Other Private Worksheet %s' % id
            uuid = self._create_tagged_worksheet(name, title)
            run_command([self._cl, 'wperm', uuid, 'public', 'none'])
            self._private_worksheets.append(uuid)
            uuid = run_command(
                [
                    self._cl,
                    'run',
                    '--request-memory',
                    '10m',
                    '--request-docker-image',
                    'python:3.6.10-slim-buster',
                    'echo private run',
                    '--tags=%s' % SampleWorksheet.TAG,
                ]
            )
            run_command([self._cl, 'perm', uuid, 'public', 'none'])
            self._private_bundles.append(uuid)

    def _wait_for_bundles_to_finish(self):
        if self._valid_bundles:
            for bundle in self._valid_bundles:
                run_command([self._cl, 'wait', bundle])
                print('Bundle {} is finished.'.format(bundle))

    def _create_sample_worksheet(self):
        # Write out the contents to a temporary file
        with open(SampleWorksheet._WORKSHEET_FILE_PATH, 'w') as file:
            file.write('\n'.join(self._content))

        # Create the main worksheet used for stress testing the frontend
        title = '{} Worksheet'.format(self._description[0].upper() + self._description[1:])
        self._create_tagged_worksheet(self._worksheet_name, title)

        # Replace the content of the current worksheet with the temporary file's content. Delete the temp file after.
        run_command([self._cl, 'wedit', '--file=' + SampleWorksheet._WORKSHEET_FILE_PATH])
        os.remove(SampleWorksheet._WORKSHEET_FILE_PATH)
        print('Deleted worksheet file at {}.'.format(SampleWorksheet._WORKSHEET_FILE_PATH))

    def _add_introduction(self):
        self._expected_lines.append(
            f'### Worksheet: https?:\/\/.*{SampleWorksheet._FULL_UUID_REGEX}.*cl_small_worksheet.*'
        )
        self._expected_lines.append('### Title: (Small|Large) Worksheet')
        self._expected_lines.append(f'### Tags: {SampleWorksheet.TAG}')
        self._expected_lines.append(f'### Owner: {SampleWorksheet._NAME_REGEX}')
        self._expected_lines.append(
            f'### Permissions: public\({SampleWorksheet._PARTIAL_UUID_REGEX}\):{SampleWorksheet._PERMISSION_REGEX}'
        )

        self._add_header('Introduction')
        self._add_line('This is the **{}** sample worksheet.'.format(self._description), True)

    def _create_tagged_worksheet(self, name, title):
        uuid = run_command([self._cl, 'new', name])
        run_command([self._cl, 'work', name])
        run_command([self._cl, 'wedit', '--tag=%s' % SampleWorksheet.TAG, '--title=%s' % title])
        return uuid

    def _add_worksheet_references(self):
        self._add_header('Worksheet References')
        self._add_subheader('Valid Worksheet References')
        self._add_worksheets(self._valid_worksheets)
        self._add_subheader('Private Worksheet References')
        self._add_worksheets(self._private_worksheets)

    def _add_bundle_references(self):
        self._add_header('Bundle References')
        self._add_subheader('Valid Bundle References')
        self._add_bundles(self._valid_bundles)
        self._add_default_table_pattern(len(self._valid_bundles))
        self._add_subheader('Private Bundle References')
        self._add_bundles(self._private_bundles)
        self._add_default_table_pattern(len(self._private_bundles))

    def _add_schemas(self):
        self._add_header('Schemas')
        self._add_subheader('Valid Schema')
        self._add_line('% schema valid_schema')
        self._add_line('% add uuid uuid "[0:8]"')
        self._add_line('% add name')
        self._add_line('% add summary')
        self._add_line('% add metadata')
        self._add_line('% add permission')
        self._add_line('% add group_permissions')
        self._add_line('% display table valid_schema')
        self._add_blank_line_pattern()
        self._expected_lines.append(SampleWorksheet._SCHEMA_REGEX)
        self._add_bundles(self._valid_bundles)
        self._add_table_pattern(
            ['uuid', 'name', 'summary', 'metadata', 'permission', 'group_permissions'],
            len(self._valid_bundles),
        )

        self._add_description('Attempting to reference private bundles with a valid schema')
        self._add_line('% display table valid_schema')
        self._add_bundles(self._private_bundles)
        self._add_table_pattern(
            ['uuid', 'name', 'summary', 'metadata', 'permission', 'group_permissions'],
            len(self._private_bundles),
        )

        self._add_subheader('Post-Processor Schema')
        self._add_line('% schema post_processor_schema')
        self._add_line('% add "duration time" time duration')
        self._add_line('% add updated last_updated date')
        self._add_line('% add size data_size size')
        self._add_line('% add uuid uuid "[0:8]"')
        self._add_line('% display table post_processor_schema')
        self._add_blank_line_pattern()
        self._expected_lines.append(SampleWorksheet._SCHEMA_REGEX)
        self._add_bundles(self._valid_bundles)
        self._add_table_pattern(
            ['duration', 'time', 'updated', 'size', 'uuid'], len(self._valid_bundles)
        )

        self._add_subheader('Combine Schemas')
        self._add_line('% schema combined_schema')
        self._add_line('% addschema valid_schema')
        self._add_line('% addschema post_processor_schema')
        self._add_blank_line_pattern()
        self._expected_lines.append(SampleWorksheet._SCHEMA_REGEX)
        self._add_line('% display table combined_schema')
        self._add_bundles(self._valid_bundles)
        self._add_table_pattern(
            [
                'uuid',
                'name',
                'summary',
                'metadata',
                'permission',
                'group_permissions',
                'duration',
                'time',
                'updated',
                'size',
                'uuid',
            ],
            len(self._valid_bundles),
        )

        self._add_subheader('Invalid Schemas')
        self._add_description('Attempting to add a field before referencing a schema')
        self._add_line('% add name')
        self._add_blank_line_pattern()
        self._expected_lines.append(
            'Error in source line [\d]+: `add` must be preceded by `schema` directive'
        )

        self._add_description('Attempting to add a non-existing schema')
        self._add_line('% schema invalid_schema')
        self._add_line('% addschema nonexistent_schema')
        self._add_blank_line_pattern()
        self._expected_lines.append('Unexpected error while parsing line [\d]+')

        self._add_description('Attempting to create a schema with invalid functions')
        self._add_line('% schema invalid_functions_schema')
        self._add_line('% add time time duration2')
        self._add_line('% add updated last_updated date2')
        self._add_line('% add size data_size size2')
        self._add_blank_line_pattern()
        self._expected_lines.append(SampleWorksheet._SCHEMA_REGEX)
        self._add_line('% display table invalid_functions_schema')
        self._add_bundles(self._valid_bundles)
        self._add_table_pattern(['time', 'updated', 'size'], 0)
        for _ in range(len(self._valid_bundles)):
            self._expected_lines.append(
                '\s\s<invalid function: duration2>\s\s<invalid function: date2>\s\s<invalid function: size2>'
            )

    def _add_display_modes(self):
        self._add_header('Display Modes')
        self._add_subheader('Table')
        self._add_line('% display table valid_schema')
        self._add_bundles(self._valid_bundles)
        self._add_table_pattern(
            ['uuid', 'name', 'summary', 'metadata', 'permission', 'group_permissions'],
            len(self._valid_bundles),
        )

        self._add_subheader('Image')
        for uuid in self._search_bundles('.png'):
            self._add_line('% display image / width=500')
            self._add_bundle(uuid)
            self._add_blank_line_pattern()
            self._expected_lines.append(SampleWorksheet._IMAGE_REGEX)

        self._add_subheader('Record')
        self._add_line('% display record valid_schema')
        self._add_bundles(self._valid_bundles)
        self._add_records_pattern(
            ['uuid', 'name', 'summary', 'metadata', 'permission', 'group_permissions'],
            len(self._valid_bundles),
        )

        self._add_subheader('HTML')
        for uuid in self._search_bundles('.html'):
            self._add_line('% display html /')
            self._add_bundle(uuid)
            self._add_blank_line_pattern()
            self._expected_lines.append(SampleWorksheet._TEXT_REGEX)

        self._add_subheader('Graph')
        for uuid in self._search_bundles('.tsv'):
            self._add_line('% display graph /')
            self._add_bundle(uuid)
            self._add_blank_line_pattern()
            self._expected_lines.append(SampleWorksheet._GRAPH_REGEX)

    def _add_search(self):
        self._add_header('Search')
        self._add_subheader('Bundle Search')
        self._add_line('% search echo run .limit={}'.format(self._entities_count))
        self._add_default_table_pattern(self._entities_count)

        self._add_subheader('Partial UUID Matching')
        self._add_line('% search 0x .limit={}'.format(self._entities_count))
        self._add_default_table_pattern(self._entities_count)

        self._add_subheader('Worksheet Search')
        self._add_line('% wsearch worksheet .limit={}'.format(self._entities_count))
        self._add_worksheets_pattern(self._entities_count)

        self._add_subheader('More Examples')
        self._add_description('Search for total disk usage')
        self._add_line('Total Disk Usage:')
        self._add_line('% search size=.sum .format=size')
        self._expected_lines.extend(['Total Disk Usage:', SampleWorksheet._SIZE_REGEX])

        self._add_description('Search for my bundles')
        self._add_line('% search .mine .limit={}'.format(self._entities_count))
        self._add_default_table_pattern(self._entities_count)

        self._add_description('Search for the largest bundles')
        self._add_line('% search size=.sort- .limit={}'.format(self._entities_count))
        self._add_default_table_pattern(self._entities_count)

        self._add_description('Search for recently ready runs')
        self._add_line('% search state=ready .limit={} id=.sort-'.format(self._entities_count))
        self._add_default_table_pattern(self._entities_count)

        self._add_description('Search for worksheets (tag: {})'.format(SampleWorksheet.TAG))
        self._add_line(
            '% wsearch tag={} id=.sort- .limit={}'.format(SampleWorksheet.TAG, self._entities_count)
        )
        self._add_worksheets_pattern(self._entities_count)

        self._add_description('Search for recently created bundles')
        self._add_line('% schema recently_created_schema')
        self._add_line('% add name')
        self._add_line('% add owner owner_name')
        self._add_line('% add created created date')
        self._add_line('% display table recently_created_schema')
        self._add_line('% search .mine .limit={}'.format(self._entities_count))
        self._add_blank_line_pattern()
        self._expected_lines.append(SampleWorksheet._SCHEMA_REGEX)
        self._add_table_pattern(['name', 'owner', 'created'], self._entities_count)

    def _add_invalid_directives(self):
        self._add_header('Invalid Directives')
        self._add_line('% hi')
        self._add_blank_line_pattern()
        self._expected_lines.append('Error in source line [\d]+: unknown directive `hi`')
        self._add_line('% hello')
        self._add_blank_line_pattern()
        self._expected_lines.append('Error in source line [\d]+: unknown directive `hello`')

    def _add_rendering_logic(self):
        self._add_header('Rendering')
        self._add_subheader('Markdown')
        self._add_line('Emphasis, aka italics, with *asterisks* or _underscores_.', True)
        self._add_line('Strong emphasis, aka bold, with **asterisks** or __underscores__.', True)
        self._add_line('Combined emphasis with **asterisks and _underscores_**.', True)
        self._add_line('Strikethrough uses two tildes. ~~Scratch this.~~', True)

        self._add_description('Below is an ordered list')
        self._add_line('1. First item', True)
        self._add_line('2. Second item', True)

        self._add_description('Below is an unordered list')
        self._add_line('* Unordered list can use asterisks', True)
        self._add_line('- Or minuses', True)
        self._add_line('+ Or pluses', True)

        self._add_description('Below is a table')
        self._add_line('| Tables        | Are           | Cool  |', True)
        self._add_line('| ------------- |:-------------:| -----:|', True)
        self._add_line('| col 3 is      | right-aligned | 1600 |', True)
        self._add_line('| col 2 is      | centered      |   12 |', True)
        self._add_line('| zebra stripes | are neat      |    1 |', True)

        self._add_subheader('Unicode Characters')
        self._add_line('En-Dash &ndash; &#150;', True)
        self._add_line('Em-Dash &mdash; &#151;', True)
        self._add_line('Minus Symbol &minus; &#8722;', True)

        self._add_subheader('Code Block')
        self._add_line('~~~ Python', True)
        self._add_line('def main():', True)
        self._add_line('\t# This is some Python code', True)
        self._add_line('\tprint("Hello")', True)
        self._add_line('~~~', True)

        self._add_subheader('Some Latex and Math')
        self._add_line('Source: [CS221](http://cs221.stanford.edu/)', True)
        for _ in range(self._entities_count):
            self._add_line(SampleWorksheet._TEX_AND_MATH)
            self._expected_lines.append(SampleWorksheet._TEXT_REGEX)

    # Helpers
    def _add_header(self, title):
        self._add_line('\n## %s' % title)
        self._add_blank_line_pattern()
        self._expected_lines.append('## %s' % re.escape(title))

    def _add_subheader(self, title):
        self._add_line('\n#### %s' % title)
        self._add_blank_line_pattern()
        self._expected_lines.append('#### %s' % re.escape(title))

    def _add_description(self, description):
        self._add_line('\n##### %s' % description)
        self._add_blank_line_pattern()
        self._expected_lines.append('##### %s' % re.escape(description))

    def _add_worksheets(self, worksheets):
        self._add_blank_line_pattern()
        for uuid in worksheets:
            self._add_line('{{%s}}' % uuid)
            self._expected_lines.append(f'\[Worksheet .*{SampleWorksheet._FULL_UUID_REGEX}.*\]')

    def _add_bundles(self, bundles):
        for uuid in bundles:
            self._add_bundle(uuid)

    def _add_bundle(self, uuid):
        self._add_line('{%s}' % uuid)

    def _add_line(self, line, add_pattern=False):
        # Add the line to the worksheet. If add_pattern is true, the regex form of the line will also be added
        # to the list of expected patterns for testing purposes.
        self._content.append(line)
        if add_pattern:
            self._expected_lines.append(re.escape(line))

    def _search_bundles(self, query):
        if self._preview_mode:
            # When in preview mode, just return the cached UUIDs of valid bundles instead of performing a new search
            return self._valid_bundles

        bundles = run_command(
            [
                self._cl,
                'search',
                query,
                '.limit=%d' % self._entities_count,
                'created=.sort-',
                '--uuid-only',
            ]
        )
        if not bundles:
            return []
        return bundles.split('\n')

    def _add_table_pattern(self, headers, row_count):
        def add_row_pattern(values):
            self._expected_lines.append('\s\s%s' % '\s*'.join(values))

        self._add_blank_line_pattern()
        add_row_pattern(headers)
        self._add_dash_pattern()
        row_patterns = [self._get_pattern(header) for header in headers]
        for _ in range(row_count):
            add_row_pattern(row_patterns)

    def _add_default_table_pattern(self, row_count):
        self._add_table_pattern(
            ['uuid\[0:8\]', 'name', 'summary\[0:1024\]', 'data_size', 'state', 'description'],
            row_count,
        )

    def _add_records_pattern(self, headers, record_count):
        def format_record_entity(header):
            return '\s\s{}:\s*{}'.format(header, self._get_pattern(header))

        for _ in range(record_count):
            self._add_blank_line_pattern()
            self._add_dash_pattern()
            self._expected_lines.extend([format_record_entity(header) for header in headers])

    def _add_worksheets_pattern(self, worksheet_count):
        self._add_blank_line_pattern()
        for _ in range(worksheet_count):
            self._expected_lines.append(f'\[Worksheet .*{SampleWorksheet._FULL_UUID_REGEX}.*\]')

    def _add_dash_pattern(self):
        self._expected_lines.append('\s\s[-]*')

    def _add_blank_line_pattern(self):
        self._expected_lines.append('')

    def _get_pattern(self, header_type):
        if 'uuid' in header_type:
            return SampleWorksheet._PARTIAL_UUID_REGEX
        elif header_type == 'permission':
            return SampleWorksheet._PERMISSION_REGEX
        elif header_type == 'name':
            return SampleWorksheet._NAME_REGEX
        elif header_type == 'state':
            return SampleWorksheet._BUNDLE_STATE_REGEX
        else:
            return SampleWorksheet._TEXT_REGEX

    def _random_id(self):
        return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))


def main():
    if args.cleanup:
        cleanup(cl, SampleWorksheet.TAG)
        return
    print(args)

    ws = SampleWorksheet(cl, args.large, args.preview)
    start_time = time.time()
    ws.create()
    if args.test_print:
        ws.test_print()
    duration_seconds = time.time() - start_time
    print("--- Completion Time: {} minutes---".format(duration_seconds / 60))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Creates a sample worksheet in the specified CodaLab instance.'
    )
    parser.add_argument(
        '--cl-executable',
        type=str,
        help='Path to Codalab CLI executable (defaults to "cl")',
        default='cl',
    )
    parser.add_argument(
        '--preview',
        action='store_true',
        help='Whether to reference existing bundles and worksheets instead of creating new ones (defaults to false)',
    )
    parser.add_argument(
        '--large',
        action='store_true',
        help='Whether to create a large worksheet (defaults to false)',
    )
    parser.add_argument(
        '--cleanup',
        action='store_true',
        help='Whether to clean up bundles and worksheets created from previous sample worksheets (defaults to false)',
    )
    parser.add_argument(
        '--test-print',
        action='store_true',
        help='Whether to test the content of sample worksheet '
        'after it is created by running cl print (defaults to false)',
    )

    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main()
