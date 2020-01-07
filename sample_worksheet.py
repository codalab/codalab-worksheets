import argparse
import random
import string

from stress_test import cleanup
from test_cli import run_command

"""
Script to create tiny and giant worksheets in any instance.
"""


class SampleWorksheet:
    TAG = 'codalab-sample-worksheet'
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
        # For simplicity, reference a set number of entities for each section of the tiny and giant worksheet.
        if args.giant:
            self._description = 'giant'
            self._entities_count = 100
        else:
            self._description = 'tiny'
            self._entities_count = 3
        self._cl = cl
        self._valid_worksheets = []
        self._private_worksheets = []
        self._valid_bundles = []
        self._private_bundles = []

    def create(self):
        print('Creating a {} worksheet...'.format(self._description))
        self._create_dependencies()
        self._create_sample_worksheet()
        self._add_worksheet_references()
        self._add_bundle_references()
        self._add_schemas()
        self._add_display_modes()
        self._add_search()
        self._add_rendering_logic()
        self._add_invalid_directives()
        print('Done. Outputting the contents of the worksheet...\n\n')
        self._run([self._cl, 'print'])

    def _create_dependencies(self):
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
        # Create the tiny or giant worksheet and cache its uuid
        name = 'cl_{}_worksheet'.format(self._description)
        title = '{} Worksheet'.format(self._description[0].upper() + self._description[1:])
        self._create_tagged_worksheet(name, title)

        # Append worksheet introduction
        self._add_header('Introduction')
        self._run(
            [
                self._cl,
                'add',
                'text',
                'This is the **{}** sample worksheet.'.format(self._description),
            ]
        )

    def _create_tagged_worksheet(self, name, title):
        uuid = self._run([self._cl, 'new', name])
        self._run([self._cl, 'work', name])
        self._run([self._cl, 'wedit', '--tag=%s' % SampleWorksheet.TAG, '--title=%s' % title])
        return uuid

    def _add_worksheet_references(self):
        self._add_header('Worksheet References')
        self._add_subheader('Valid Worksheet References')
        for uuid in self._valid_worksheets:
            self._run([self._cl, 'add', 'worksheet', uuid])
        self._add_subheader('Private Worksheet References')
        for uuid in self._private_worksheets:
            self._run([self._cl, 'add', 'worksheet', uuid])

    def _add_bundle_references(self):
        self._add_header('Bundle References')
        self._add_subheader('Valid Bundle References')
        self._add_bundles(self._valid_bundles)
        self._add_subheader('Private Bundle References')
        self._add_bundles(self._private_bundles)

    def _add_schemas(self):
        self._add_header('Schemas')
        self._add_subheader('Valid Schema')
        self._add_text('% schema valid_schema')
        self._add_text('% add uuid uuid "[0:8]"')
        self._add_text('% add name')
        self._add_text('% add summary')
        self._add_text('% add metadata')
        self._add_text('% add permission')
        self._add_text('% add group_permissions')
        self._add_text('% display table valid_schema')
        self._add_bundles(self._valid_bundles)

        self._add_description('Attempting to reference private bundles with a valid schema')
        self._add_text('% display table valid_schema')
        self._add_bundles(self._private_bundles)

        self._add_subheader('Post-Processor Schema')
        self._add_text('% schema post_processor_schema')
        self._add_text('% add "duration time" time duration')
        self._add_text('% add updated last_updated date')
        self._add_text('% add size data_size size')
        self._add_text('% add uuid uuid "[0:8]"')
        self._add_text('% display table post_processor_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('Combine Schemas')
        self._add_text('% schema combined_schema')
        self._add_text('% addschema valid_schema')
        self._add_text('% addschema post_processor_schema')
        self._add_text('% display table combined_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('Invalid Schemas')
        self._add_description('Attempting to add a field before referencing a schema')
        self._add_text('% add name')
        self._add_description('Attempting to add a non-existing schema')
        self._add_text('% schema invalid_schema')
        self._add_text('% addschema nonexistent_schema')
        self._add_description('Attempting to create a schema with invalid functions')
        self._add_text('% schema invalid_functions_schema')
        self._add_text('% add time time duration2')
        self._add_text('% add updated last_updated date2')
        self._add_text('% add size data_size size2')
        self._add_text('% display table invalid_functions_schema')
        self._add_bundles(self._valid_bundles)

    def _add_display_modes(self):
        self._add_header('Display Modes')
        self._add_subheader('Table')
        self._add_text('% display table valid_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('Image')
        for uuid in self._search_bundles('.png'):
            self._add_text('% display image / width=500')
            self._add_bundle(uuid)

        self._add_subheader('Record')
        self._add_text('% display record valid_schema')
        self._add_bundles(self._valid_bundles)

        self._add_subheader('HTML')
        for uuid in self._search_bundles('.html'):
            self._add_text('% display html /')
            self._add_bundle(uuid)

        self._add_subheader('Graph')
        for uuid in self._search_bundles('.tsv'):
            self._add_text('% display graph /')
            self._add_bundle(uuid)

    def _add_search(self):
        self._add_header('Search')
        self._add_subheader('Bundle Search')
        self._add_text('% search python run .limit={}'.format(self._entities_count))

        self._add_subheader('Partial UUID Matching')
        self._add_text('% search 0x .limit={}'.format(self._entities_count))

        self._add_subheader('Worksheet Search')
        self._add_text('% wsearch test .limit={}'.format(self._entities_count))

        self._add_subheader('More Examples')
        self._add_description('Search for total disk usage')
        self._add_text('Total Disk Usage:')
        self._add_text('% search size=.sum .format=size')
        self._add_description('Search for my bundles')
        self._add_text('% search .mine .limit={}'.format(self._entities_count))
        self._add_description('Search for the largest bundles')
        self._add_text('% search size=.sort- .limit={}'.format(self._entities_count))
        self._add_description('Search for recently failed runs')
        self._add_text('% search state=failed .limit={} id=.sort-'.format(self._entities_count))
        self._add_description('Search for datasets (worksheets with tag "data")')
        self._add_text('% wsearch tag=data id=.sort- .limit={}'.format(self._entities_count))

        self._add_description('Search for recently created bundles')
        self._add_text('% schema recently_created_schema')
        self._add_text('% add name')
        self._add_text('% add owner owner_name')
        self._add_text('% add created created date')
        self._add_text('% display table created')
        self._add_text('% search created=.sort- .limit={}'.format(self._entities_count))

    def _add_invalid_directives(self):
        self._add_header('Invalid Directives')
        self._add_text('% hi')
        self._add_text('% hello')

    def _add_rendering_logic(self):
        self._add_header('Rendering')
        self._add_subheader('Markdown')
        self._add_text('\nEmphasis, aka italics, with *asterisks* or _underscores_.')
        self._add_text('\nStrong emphasis, aka bold, with **asterisks** or __underscores__.')
        self._add_text('\nCombined emphasis with **asterisks and _underscores_**.')
        self._add_text('\nStrikethrough uses two tildes. ~~Scratch this.~~')

        self._add_description('Below is an ordered list')
        self._add_text('1. First item')
        self._add_text('2. Second item')
        self._add_description('Below is an unordered list')
        self._add_text('* Unordered list can use asterisks')
        self._add_text('- Or minuses')
        self._add_text('+ Or pluses')

        self._add_description('Below is a table')
        self._add_text('| Tables        | Are           | Cool  |')
        self._add_text('| ------------- |:-------------:| -----:|')
        self._add_text('| col 3 is      | right-aligned | 1600 |')
        self._add_text('| col 2 is      | centered      |   12 |')
        self._add_text('| zebra stripes | are neat      |    1 |')

        self._add_subheader('Unicode Characters')
        self._add_text('\nEn-Dash &ndash; &#150;')
        self._add_text('\nEm-Dash &mdash; &#151;')
        self._add_text('\nMinus Symbol &minus; &#8722;')

        self._add_subheader('Code Block')
        self._add_text('~~~ Python')
        self._add_text('def main():')
        self._add_text('\t# This is some Python code')
        self._add_text('\tprint("Hello")')
        self._add_text('~~~')

        self._add_subheader('Some Latex and Math')
        for _ in range(self._entities_count):
            self._add_text(SampleWorksheet._TEX_AND_MATH)
        self._add_text('\nSource: [CS221](http://cs221.stanford.edu/)')

    # Helpers
    def _add_header(self, title):
        self._add_text('\n## %s' % title)

    def _add_subheader(self, title):
        self._add_text('\n#### %s' % title)

    def _add_description(self, description):
        self._add_text('\n##### %s' % description)

    def _add_text(self, text):
        self._run([self._cl, 'add', 'text', text])

    def _add_bundles(self, bundles):
        for bundle in bundles:
            self._add_bundle(bundle)

    def _add_bundle(self, bundle):
        self._run([self._cl, 'add', 'bundle', bundle])

    def _search_bundles(self, query):
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
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase) for _ in range(24)
        )


def main():
    if args.cleanup:
        cleanup(cl, SampleWorksheet.TAG)
        return
    ws = SampleWorksheet(cl, args)
    ws.create()


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
        '--giant',
        action='store_true',
        help='Whether to create a giant worksheet (defaults to false)',
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
