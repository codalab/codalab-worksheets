'''
worksheet_util contains the following public functions:
- interpret_items: returns a structure that interprets all the directives in the worksheet item.
- request_new_items: pops up an editor to allow for full-text editing of a worksheet.

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
% display inline <genpath (e.g., output/stats/errorRate)>
% display contents <genpath (e.g., output/stats/things)>
% display image <genpath (e.g., output/graph.png)>
% display html <genpath (e.g., output/test.html)>
% display record <schema name>
% display table <schema name>

A genpath (generalized path) is either:
- a bundle field (e.g., command)
- a metadata field (e.g., metadata/name)
- a normal path, but can descend into a YAML file (e.g., output/stats/errorRate)

There are two representations of items:
- (bundle_uuid, 
'''
import os
import re
import subprocess
import sys
import tempfile

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
    for (bundle_info, value_obj, type) in worksheet_info['items']:
        if type == TYPE_MARKUP:
            yield value_obj
        elif type == TYPE_BUNDLE:
            metadata = bundle_info['metadata']
            description = bundle_info['bundle_type']
            description += ' ' + metadata['name']
            deps = interpret_genpath(bundle_info, 'dependencies')
            if deps: description += ' <- ' + deps
            command = bundle_info.get('command')
            if command: description += ' : ' + command
            yield '[%s]{%s}' % (description, bundle_info['uuid'])
        elif type == TYPE_DIRECTIVE:
            value = tokens_to_string(value_obj)
            value = DIRECTIVE_CHAR + ('' if len(value) == 0 or value.startswith(DIRECTIVE_CHAR) else ' ') + value
            yield value

def request_new_items(worksheet_info, client):
    '''
    Input: worksheet_info, client (which is used to get bundle_infos)
    Popup an editor, populated with the current worksheet contents.
    Return a list of new items (bundle_uuid, value, type) that the user typed into the editor.
    '''
    header = '''
// Editing for worksheet %s.  The coments (//) are simply instructions
// to you and not part of the actual worksheet.  You can enter:
// - arbitrary Markdown text
// - References to bundles: {<bundle_spec>}
// - Directives (%% title|schema|add|display)
    '''.strip() % (worksheet_info['name'],)

    # Construct a form template with the current value of the worksheet.
    template_lines = header.split('\n')
    template_lines.extend(get_worksheet_lines(worksheet_info))
    template = os.linesep.join(template_lines) + os.linesep

    # Show the form to the user in their editor of choice.
    editor = os.environ.get('EDITOR', 'notepad' if sys.platform == 'win32' else 'vim')
    tempfile_name = ''
    with tempfile.NamedTemporaryFile(suffix='.c', delete=False) as form:
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
    return parse_worksheet_form(form_result, client, worksheet_info['uuid'])

def parse_worksheet_form(form_result, client, worksheet_uuid):
    '''
    Input: form_result is a list of lines.
    Return a list of (bundle_uuid, value, type) tuples.
    '''
    def parse(line):
        m = BUNDLE_REGEX.match(line)
        if m:
            try:
                bundle_uuid = client.get_bundle_uuid(worksheet_uuid, m.group(3))
                bundle_info = client.get_bundle_info(bundle_uuid)
                return (bundle_info, None, TYPE_BUNDLE)
            except UsageError, e:
                return (None, line + ': ' + e.message, TYPE_MARKUP)

        m = DIRECTIVE_REGEX.match(line)
        if m:
            return (None, string_to_tokens(m.group(1)), TYPE_DIRECTIVE)

        return (None, line, TYPE_MARKUP)
        
    result = []
    for line in form_result:
        if line.startswith('//'): continue  # Skip comments
        result.append(parse(line))
    return result

def interpret_genpath(bundle_info, genpath):
    # TODO: unify the two genpaths?
    if genpath == 'dependencies' or genpath == 'hard_dependencies':
        return ','.join([dep['parent_name'] for dep in bundle_info[genpath]])

    # Only return the pair if genpath might be referring to a file.
    if genpath == 'stdout' or genpath == 'stderr' or genpath.startswith('output'):
        return (bundle_info['uuid'], genpath)

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

def apply_func(func, arg):
    try:
        return func(arg)
    except:
        # Can't apply the function, so just return the arg.
        return arg 

def interpret_items(items):
    '''
    Return a list of items, where each item is either:
    - ('markup'|'inline'|'contents', string)
    - ('record'|'table', (col1, col2), [{col1:value1, col2:value2}, ...])
    - ('image'|'html', genpath)
    '''
    result = {}
    schemas = {}
    schemas['default'] = current_schema = [
        canonicalize_schema_item(x)
        for x in [['name'], ['bundle_type'], ['dependencies'], ['command'], ['data_size', 'data_size', formatting.size_str], ['state']]
    ]
    current_display = ('table', 'default')
    new_items = []
    bundle_infos = []
    def flush():
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
            new_items.append(('image', args[0]))
        elif mode == 'html':
            new_items.append(('html', args[0]))
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
            elif command == '%' or command == '':  # Comment
                pass
            else:
                raise UsageError('Unknown command: %s' % command)
    flush()
    result['items'] = new_items
    #print result
    return result
