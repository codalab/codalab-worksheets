import argparse
import os
import random
import string
import sys
import time

sys.path.append('..')
from test_util import cleanup, run_command

"""
Script to create small and large sample worksheets in any instance to stress test the front end. The purpose of 
the small worksheet is to test all features CodaLab offers on the front end. The large worksheet is a much bigger 
version of the small worksheet and its purpose is to push the limit and stress test the frontend rendering capabilities.
"""


class SampleWorksheet:
    TAG = 'codalab-sample-worksheet'

    _FILE_NAME = 'sample-worksheet-temp.txt'
    _TEX_AND_MATH = (
        'The loss minimization framework is to cast learning as an optimization problem. We are estimating (fitting or '
        'learning) $\mathbf w$ using $ \mathcal{D}_\\text{train}$. A loss function $ \\text{Loss}(x, y, \mathbf w) $ '
        'quantifies how unhappy you would be if you used $\mathbf w$ to make a prediction on $x$ when the correct '
        'output is $y$. This is the object we want to minimize. Below is minimizing training loss across all training '
        'examples. Note that $ \\text{TrainLoss}(\mathbf w) $ is just the average of loss for all training examples.\n'
        '\n$$ \\text{TrainLoss}(\mathbf w) = \\frac{1}{| \mathcal{D}_{\\text{train}} |} \sum_{(x,y) \in \mathcal{D}'
        '_{\\text{train}}}\\text{Loss}(x, y, \mathbf w) \\ \n\min_{\mathbf w \in \mathbb{R}^d} \\text{TrainLoss}(\mathbf w)$$'
    )

    def __init__(self, cl, args):
        # For simplicity, reference a set number of entities for each section of the small and large worksheet.
        if args.large:
            self._description = 'large'
            self._entities_count = 100
        else:
            self._description = 'small'
            self._entities_count = 3
        self._cl = cl
        self._preview_mode = args.preview
        self._worksheet_name = 'cl_{}_worksheet'.format(self._description)
        self._content = []

    def create(self):
        print('Creating a {} worksheet...'.format(self._description))
        self._create_dependencies()
        self._add_introduction()
        self._add_worksheet_references()
        self._add_bundle_references()
        self._add_schemas()
        self._add_display_modes()
        self._add_search()
        self._add_rendering_logic()
        self._add_invalid_directives()
        self._create_sample_worksheet()
        print('Done.')

    def _create_dependencies(self):
        if self._preview_mode:
            # When in preview mode, search for existing bundles and worksheets instead of creating new ones
            random_worksheets = self._run(
                [self._cl, 'wsearch', '.limit=%d' % self._entities_count, '--uuid-only']
            ).split('\n')
            self._valid_worksheets = random_worksheets
            self._private_worksheets = random_worksheets
            random_bundles = self._run(
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
            self._valid_bundles.append(
                self._run(
                    [self._cl, 'run', 'echo codalab rules!', '--tags=%s' % SampleWorksheet.TAG]
                )
            )
            # Create a valid private worksheet and a bundle each
            name = 'valid_private_worksheet_%s' % id
            title = 'Other Private Worksheet %s' % id
            uuid = self._create_tagged_worksheet(name, title)
            self._run([self._cl, 'wperm', uuid, 'public', 'none'])
            self._private_worksheets.append(uuid)
            uuid = self._run(
                [self._cl, 'run', 'echo private run', '--tags=%s' % SampleWorksheet.TAG]
            )
            self._run([self._cl, 'perm', uuid, 'public', 'none'])
            self._private_bundles.append(uuid)

    def _create_sample_worksheet(self):
        # Write out the contents to a temporary file
        with open(SampleWorksheet._FILE_NAME, 'w') as file:
            file.write('\n'.join(self._content))

        # Create the main worksheet used for stress testing the frontend
        title = '{} Worksheet'.format(self._description[0].upper() + self._description[1:])
        self._create_tagged_worksheet(self._worksheet_name, title)

        # Replace the content of the current worksheet with the temporary file's content. Delete the temp file after.
        self._run([self._cl, 'wedit', '--file=' + SampleWorksheet._FILE_NAME])
        os.remove(SampleWorksheet._FILE_NAME)
        print('Deleted file {}.'.format(SampleWorksheet._FILE_NAME))

    def _add_introduction(self):
        self._add_header('Introduction')
        self._add_line('This is the **{}** sample worksheet.'.format(self._description))

    def _create_tagged_worksheet(self, name, title):
        uuid = self._run([self._cl, 'new', name])
        self._run([self._cl, 'work', name])
        self._run([self._cl, 'wedit', '--tag=%s' % SampleWorksheet.TAG, '--title=%s' % title])
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
        self._add_subheader('Private Bundle References')
        self._add_bundles(self._private_bundles)

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
        self._add_bundles(self._valid_bundles)

        self._add_description('Attempting to reference private bundles with a valid schema')
        self._add_line('% display table valid_schema')
        self._add_bundles(self._private_bundles)

        self._add_subheader('Post-Processor Schema')
        self._add_line('% schema post_processor_schema')
        self._add_line('% add "duration time" time duration')
        self._add_line('% add updated last_updated date')
        self._add_line('% add size data_size size')
        self._add_line('% add uuid uuid "[0:8]"')
        self._add_line('% display table post_processor_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('Combine Schemas')
        self._add_line('% schema combined_schema')
        self._add_line('% addschema valid_schema')
        self._add_line('% addschema post_processor_schema')
        self._add_line('% display table combined_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('Invalid Schemas')
        self._add_description('Attempting to add a field before referencing a schema')
        self._add_line('% add name')
        self._add_description('Attempting to add a non-existing schema')
        self._add_line('% schema invalid_schema')
        self._add_line('% addschema nonexistent_schema')
        self._add_description('Attempting to create a schema with invalid functions')
        self._add_line('% schema invalid_functions_schema')
        self._add_line('% add time time duration2')
        self._add_line('% add updated last_updated date2')
        self._add_line('% add size data_size size2')
        self._add_line('% display table invalid_functions_schema')
        self._add_bundles(self._valid_bundles)

    def _add_display_modes(self):
        self._add_header('Display Modes')
        self._add_subheader('Table')
        self._add_line('% display table valid_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('Image')
        for uuid in self._search_bundles('.png'):
            self._add_line('% display image / width=500')
            self._add_bundle(uuid)

        self._add_subheader('Record')
        self._add_line('% display record valid_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('HTML')
        for uuid in self._search_bundles('.html'):
            self._add_line('% display html /')
            self._add_bundle(uuid)

        self._add_subheader('Graph')
        for uuid in self._search_bundles('.tsv'):
            self._add_line('% display graph /')
            self._add_bundle(uuid)

    def _add_search(self):
        self._add_header('Search')
        self._add_subheader('Bundle Search')
        self._add_line('% search python run .limit={}'.format(self._entities_count))

        self._add_subheader('Partial UUID Matching')
        self._add_line('% search 0x .limit={}'.format(self._entities_count))

        self._add_subheader('Worksheet Search')
        self._add_line('% wsearch test .limit={}'.format(self._entities_count))

        self._add_subheader('More Examples')
        self._add_description('Search for total disk usage')
        self._add_line('Total Disk Usage:')
        self._add_line('% search size=.sum .format=size')
        self._add_description('Search for my bundles')
        self._add_line('% search .mine .limit={}'.format(self._entities_count))
        self._add_description('Search for the largest bundles')
        self._add_line('% search size=.sort- .limit={}'.format(self._entities_count))
        self._add_description('Search for recently failed runs')
        self._add_line('% search state=failed .limit={} id=.sort-'.format(self._entities_count))
        self._add_description('Search for datasets (worksheets with tag "data")')
        self._add_line('% wsearch tag=data id=.sort- .limit={}'.format(self._entities_count))

        self._add_description('Search for recently created bundles')
        self._add_line('% schema recently_created_schema')
        self._add_line('% add name')
        self._add_line('% add owner owner_name')
        self._add_line('% add created created date')
        self._add_line('% display table created')
        self._add_line('% search created=.sort- .limit={}'.format(self._entities_count))

    def _add_invalid_directives(self):
        self._add_header('Invalid Directives')
        self._add_line('% hi')
        self._add_line('% hello')

    def _add_rendering_logic(self):
        self._add_header('Rendering')
        self._add_subheader('Markdown')
        self._add_line('\nEmphasis, aka italics, with *asterisks* or _underscores_.')
        self._add_line('\nStrong emphasis, aka bold, with **asterisks** or __underscores__.')
        self._add_line('\nCombined emphasis with **asterisks and _underscores_**.')
        self._add_line('\nStrikethrough uses two tildes. ~~Scratch this.~~')

        self._add_description('Below is an ordered list')
        self._add_line('1. First item')
        self._add_line('2. Second item')
        self._add_description('Below is an unordered list')
        self._add_line('* Unordered list can use asterisks')
        self._add_line('- Or minuses')
        self._add_line('+ Or pluses')

        self._add_description('Below is a table')
        self._add_line('| Tables        | Are           | Cool  |')
        self._add_line('| ------------- |:-------------:| -----:|')
        self._add_line('| col 3 is      | right-aligned | 1600 |')
        self._add_line('| col 2 is      | centered      |   12 |')
        self._add_line('| zebra stripes | are neat      |    1 |')

        self._add_subheader('Unicode Characters')
        self._add_line('\nEn-Dash &ndash; &#150;')
        self._add_line('\nEm-Dash &mdash; &#151;')
        self._add_line('\nMinus Symbol &minus; &#8722;')

        self._add_subheader('Code Block')
        self._add_line('~~~ Python')
        self._add_line('def main():')
        self._add_line('\t# This is some Python code')
        self._add_line('\tprint("Hello")')
        self._add_line('~~~')

        self._add_subheader('Some Latex and Math')
        for _ in range(self._entities_count):
            self._add_line(SampleWorksheet._TEX_AND_MATH)
        self._add_line('\nSource: [CS221](http://cs221.stanford.edu/)')

    # Helpers
    def _add_header(self, title):
        self._add_line('\n## %s' % title)

    def _add_subheader(self, title):
        self._add_line('\n#### %s' % title)

    def _add_description(self, description):
        self._add_line('\n##### %s' % description)

    def _add_worksheets(self, worksheets):
        for uuid in worksheets:
            self._add_line('{{%s}}' % uuid)

    def _add_bundles(self, bundles):
        for uuid in bundles:
            self._add_bundle(uuid)

    def _add_bundle(self, uuid):
        self._add_line('{%s}' % uuid)

    def _add_line(self, line):
        self._content.append(line)

    def _search_bundles(self, query):
        if self._preview_mode:
            # When in preview mode, just return the cached UUIDs of valid bundles instead of performing a new search
            return self._valid_bundles

        return self._run(
            [
                self._cl,
                'search',
                query,
                '.limit=%d' % self._entities_count,
                'created=.sort-',
                '--uuid-only',
            ]
        ).split('\n')

    def _run(self, args):
        return run_command(args, force_subprocess=True)

    def _random_id(self):
        return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(32))


def main():
    if args.cleanup:
        cleanup(cl, SampleWorksheet.TAG)
        return
    print(args)
    ws = SampleWorksheet(cl, args)
    start_time = time.time()
    ws.create()
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

    # Parse args and run this script
    args = parser.parse_args()
    cl = args.cl_executable
    main()
