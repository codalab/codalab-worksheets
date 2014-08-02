'''
worksheet_util contains the following public functions:
- request_lines: pops up an editor to allow for full-text editing of a worksheet.
- parse_worksheet_form: takes those lines and generates a set of items (triples)
- interpret_items: takes those triples and returns a structure that interprets all the directives in the worksheet item.

A worksheet contains a list of items, where each item includes
- bundle_uuid (only used if type == bundle)
- value (used for text and directive)
- type: one of the following:
  * markup: just plain plain text (markdown)
  * bundle: represents a bundle
  * directive: special instructions for determining formatting
This is the representation in the DB.
In the code, we have full items of the form (bundle_info, value_obj, type).

The value of a directive can be parsed into a sequence of tokens, where the first token determines the type.
Types of directives:
% title <title text>
%
% schema <schema name>
% <other schema name>
% <field> <genpath> <post-processing>
% endschema
%
%% this is a comment
% display hidden
% display inline <genpath (e.g., stats/errorRate)>
% display contents <genpath (e.g., stats/things)>
% display image <genpath (e.g., graph.png)>
% display html <genpath (e.g., test.html)>
% display record <schema name>
% display table <schema name>
%
% search <keywords>

A genpath (generalized path) is either:
- a bundle field (e.g., command)
- a metadata field (e.g., name)
- a normal path, but can descend into a YAML file (e.g., /stats:train/errorRate)

There are two representations of worksheet items:
- (bundle_uuid, value, type) [inserted into the database]
- (bundle_info, value_obj, type) [used in the code]
'''
import os
import re
import subprocess
import sys
import tempfile
import yaml

from codalab.common import UsageError
from codalab.lib import path_util, canonicalize, formatting

# Types of worksheet items
TYPE_MARKUP = 'markup'
TYPE_BUNDLE = 'bundle'
TYPE_DIRECTIVE = 'directive'

BUNDLE_REGEX = re.compile('^(\[(.*)\])?\s*\{(.*)\}$')
DIRECTIVE_CHAR = '%'
DIRECTIVE_REGEX = re.compile(r'^' + DIRECTIVE_CHAR + '\s*(.*)$')

# Tokens are serialized as a space-separated list, where we use " to quote.
# "first token" "\"second token\"" third

def quote(token):
    if ' ' in token or '"' in token:
        return '"' + token.replace('"', '\\"') + '"'
    return token
def tokens_to_string(tokens):
    return ' '.join(quote(token) for token in tokens)

def string_to_tokens(s):
    #print s
    tokens = []
    i = 0
    while i < len(s):
        if s[i] == '"':
            while True:
                try:
                    j = s.index('"', i+1)
                except:
                    raise UsageError('Unclosed quote: %s' % s)
                if s[j-1] != '\\': break
            tokens.append(s[i+1:j].replace('\\"', '"'))
            j += 1 # Skip over the last quote
        else:
            try:
                j = s.index(' ', i+1)
            except:
                j = len(s)
            tokens.append(s[i:j])
        i = j
        while i < len(s) and s[i] == ' ': i += 1
    return tokens

def convert_item_to_db(item):
    (bundle_info, value_obj, type) = item
    bundle_uuid = bundle_info['uuid'] if bundle_info else None
    value = tokens_to_string(value_obj) if type == TYPE_DIRECTIVE else value_obj
    if not value: value = ''  # TODO: change the database schema so that None's are allowed
    return (bundle_uuid, value, type)

def get_worksheet_lines(worksheet_info):
    '''
    Generator that returns pretty-printed lines of text for the given worksheet.
    '''
    header = '''
// Editing for worksheet %s.  The coments (//) are simply instructions
// to you and not part of the actual worksheet.  You can enter:
// - Arbitrary Markdown (see http://daringfireball.net/projects/markdown/syntax)
// - References to bundles: {<bundle_spec>}
// - Directives (%% title|schema|add|display)
//   * title "Place title here"
//   * schema <schema name>
//   * add <descriptor> | add <key name> <value source>
//   * display inline|contents|image|html <value source>
//   * display record|table <schema name>
// For example, you can define a schema for a table and then set the display mode to using that schema:
// %% schema s1
// %% add name
// %% add command
// %% add time
// %% display table s1
// %% {run1}
// %% {run2}
    '''.strip() % (worksheet_info['name'],)
    lines = header.split('\n')

    for (bundle_info, value_obj, type) in worksheet_info['items']:
        if type == TYPE_MARKUP:
            lines.append(value_obj)
        elif type == TYPE_BUNDLE:
            metadata = bundle_info['metadata']
            description = bundle_info['bundle_type']
            description += ' ' + metadata['name']
            deps = interpret_genpath(bundle_info, 'dependencies')
            if deps: description += ' -- ' + deps
            command = bundle_info.get('command')
            if command: description += ' : ' + command
            lines.append('[%s]{%s}' % (description, bundle_info['uuid']))
        elif type == TYPE_DIRECTIVE:
            value = tokens_to_string(value_obj)
            value = DIRECTIVE_CHAR + ('' if len(value) == 0 or value.startswith(DIRECTIVE_CHAR) else ' ') + value
            lines.append(value)
    return lines

def request_lines(worksheet_info, client):
    '''
    Input: worksheet_info, client (which is used to get bundle_infos)
    Popup an editor, populated with the current worksheet contents.
    Return a list of new items (bundle_uuid, value, type) that the user typed into the editor.
    '''
    # Construct a form template with the current value of the worksheet.
    template_lines = get_worksheet_lines(worksheet_info)
    template = os.linesep.join(template_lines) + os.linesep

    # Show the form to the user in their editor of choice.
    editor = os.environ.get('EDITOR', 'notepad' if sys.platform == 'win32' else 'vim')
    tempfile_name = ''
    with tempfile.NamedTemporaryFile(suffix='.md', delete=False) as form:
        form.write(template)
        form.flush()
        tempfile_name = form.name
    if os.path.isfile(tempfile_name):
        subprocess.call([editor, tempfile_name])
        with open(tempfile_name, 'rb') as form:
            lines = form.readlines()
        path_util.remove(tempfile_name)
    else:
        lines = template_lines

    # Process the result
    form_result = [line.rstrip() for line in lines]
    if form_result == template_lines:
        raise UsageError('No change made; aborting')
    return form_result

def parse_worksheet_form(form_result, client, worksheet_uuid):
    '''
    Input: form_result is a list of lines.
    Return (list of (bundle_uuid, value, type) triples, commands to execute)
    '''
    # The user can specify '!<command> ^', which perform actions on the previous bundle.
    bundle_uuids = []
    commands = []
    def parse(line):
        m = BUNDLE_REGEX.match(line)
        if m:
            try:
                bundle_uuid = client.get_bundle_uuid(worksheet_uuid, m.group(3))
                bundle_info = client.get_bundle_info(bundle_uuid)
                bundle_uuids.append(bundle_uuid)
                return (bundle_info, None, TYPE_BUNDLE)
            except UsageError, e:
                return (None, line + ': ' + e.message, TYPE_MARKUP)

        m = DIRECTIVE_REGEX.match(line)
        if m:
            return (None, string_to_tokens(m.group(1)), TYPE_DIRECTIVE)

        return (None, line, TYPE_MARKUP)
        
    result = []
    for line in form_result:
        if line.startswith('//'):  # Comments
            pass
        elif line.startswith('!'):  # Run commands
            command = line[1:].strip().split()
            # Replace ^ with the reference to the last bundle.
            command = [(bundle_uuids[-1] if arg == '^' else arg) for arg in command]
            commands.append(command)
        else:
            result.append(parse(line))

    return (result, commands)

def interpret_genpath(bundle_info, genpath):
    '''
    This function is called in the first server call to a BundleClient to
    quickly interpret the genpaths that only require looking bundle_info
    (cheap).
    '''
    if genpath == 'dependencies':
        return ','.join([dep['parent_name'] for dep in bundle_info[genpath]])
    elif genpath.startswith('dependencies/'):
        # Look up the particular dependency
        _, name = genpath.split('/', 1)
        for dep in bundle_info['dependencies']:
            if dep['child_path'] == name:
                return dep['parent_name']
        return 'n/a'

    # If genpath is referring to a file, then just returns instructions for
    # fetching that file rather than actually doing it.
    if genpath.startswith('/'):
        return (bundle_info['uuid'], genpath[1:])

    # Either bundle info or metadata
    value = bundle_info.get(genpath, None)
    if not value: value = bundle_info['metadata'].get(genpath, None)
    return value or ''

def canonicalize_schema_item(args):
    if len(args) == 1:
        return (os.path.basename(args[0]), args[0], lambda x : x)
    elif len(args) == 2:
        return (args[0], args[1], lambda x : x)
    elif len(args) == 3:
        return (args[0], args[1], args[2])
    else:
        raise UsageError('Invalid number of arguments: %s' % value_obj)

def canonicalize_schema_items(items):
    return [canonicalize_schema_item(item) for item in items]

def apply_func(func, arg):
    try:
        return func(arg)
    except:
        # Can't apply the function, so just return the arg.
        return arg 

def get_default_schemas():
    created = ['created', 'created', formatting.time_str]
    data_size = ['data_size', 'data_size', formatting.size_str]
    schemas = {}

    schemas['default'] = canonicalize_schema_items([['name'], ['bundle_type'], created, data_size, ['state']])

    schemas['program'] = canonicalize_schema_items([['name'], created, data_size])
    schemas['dataset'] = canonicalize_schema_items([['name'], created, data_size])

    schemas['make'] = canonicalize_schema_items([['name'], created, ['dependencies'], ['state']])
    schemas['run'] = canonicalize_schema_items([['name'], created, ['dependencies'], ['command'], ['state']])
    return schemas

def interpret_items(schemas, items):
    '''
    schemas: initial mapping from name to list of schema items (columns of a table)
    items: list of worksheet items to interpret
    Return a list of items, where each item is either:
    - ('markup'|'inline'|'contents', string)
    - ('record'|'table', (col1, col2), [{col1:value1, col2:value2}, ...])
    - ('image'|'html', genpath)
    - ('search', [keyword, ...])
    '''
    result = {}

    # Set default schema
    current_schema = None

    current_display = ('table', 'default')
    new_items = []
    bundle_infos = []
    def flush():
        '''
        Gathered a group of bundles (in a table), which we can group together.
        '''
        if len(bundle_infos) == 0: return
        # Print out the curent bundles somehow
        mode = current_display[0] 
        args = current_display[1:]
        if mode == 'hidden':
            pass
        elif mode == 'inline' or mode == 'contents':
            for bundle_info in bundle_infos:
                new_items.append((mode, interpret_genpath(bundle_info, args[0])))
        elif mode == 'image':
            new_items.append((mode, args[0]))
        elif mode == 'html':
            new_items.append((mode, args[0]))
        elif mode == 'record':
            # display record schema =>
            # key1: value1
            # key2: value2
            # ...
            schema = schemas[args[0] if len(args) > 0 else 'default']
            for bundle_info in bundle_infos:
                header = ('key', 'value')
                rows = []
                for (name, genpath, post) in schema:
                    rows.append({'key': name + ':', 'value': apply_func(post, interpret_genpath(bundle_info, genpath))})
                new_items.append((mode, (header, rows)))
        elif mode == 'table':
            # display table schema =>
            # key1       key2
            # b1_value1  b1_value2
            # b2_value1  b2_value2
            schema = schemas[args[0] if len(args) > 0 else 'default']
            header = tuple(name for (name, genpath, post) in schema)
            rows = []
            for bundle_info in bundle_infos:
                rows.append({name : apply_func(post, interpret_genpath(bundle_info, genpath)) for (name, genpath, post) in schema})
            new_items.append((mode, (header, rows)))
        else:
            raise UsageError('Unknown display mode: %s' % mode)
        bundle_infos[:] = []  # Clear

    for (bundle_info, value_obj, type) in items:
        if type == TYPE_BUNDLE:
            bundle_infos.append(bundle_info)
        elif type == TYPE_MARKUP:
            flush()
            new_items.append((TYPE_MARKUP, value_obj))
            pass
        elif type == TYPE_DIRECTIVE:
            flush()
            if len(value_obj) == 0: continue
            command = value_obj[0]
            if command == 'title':
                result['title'] = value_obj[1]
            elif command == 'schema':
                name = value_obj[1]
                schemas[name] = current_schema = []
            elif command == 'add':
                # genpath | name genpath | name genpath 
                schema_item = canonicalize_schema_item(value_obj[1:])
                current_schema.append(schema_item)
            elif command == 'display':
                current_display = value_obj[1:]
            elif command == 'search':
                keywords = value_obj[1:]
                mode = command
                data = {'keywords': keywords, 'display': current_display, 'schemas': schemas}
                new_items.append((mode, data))
            elif command == '%' or command == '':  # Comment
                pass
            else:
                raise UsageError('Unknown command: %s' % command)
    flush()
    result['items'] = new_items
    #print result
    return result

def lookup_targets(client, value):
    '''
    This is called upon second requests to the server to fetch information out of the
    files.
    '''
    # TODO: currently, this is really inefficient since we are possibly reading
    # the same file multiple times.  Make this more efficient!
    if isinstance(value, tuple):
        bundle_uuid, subpath = value
        if ':' in subpath:
            subpath, key = subpath.split(':')
            contents = client.head_target((bundle_uuid, subpath), 50)
            if contents == None: return ''
            info = yaml.load('\n'.join(contents))
            for k in key.split('/'):
                info = info.get(k, None)
                if k == None: return ''
            return info
        else:
            if subpath == '.': subpath = ''
            contents = client.head_target((bundle_uuid, subpath), 1)
            if contents == None: return ''
            return contents[0].strip()
    return value

def interpret_search(client, worksheet_uuid, data):
    '''
    Input: specification of a search query.
    Output: worksheet items based on the result of issuing the search query.
    '''
    # First, item determines the display
    items = [(None, ['display'] + data['display'], TYPE_DIRECTIVE)]

    # Next come the actual bundles
    bundle_uuids = client.search_bundle_uuids(worksheet_uuid, data['keywords'], 100, False)
    for bundle_uuid in bundle_uuids:
        items.append((client.get_bundle_info(bundle_uuid), None, TYPE_BUNDLE) )

    # Finally, interpret the items
    return interpret_items(data['schemas'], items)
