'''
worksheet_util contains the following public functions:
- request_lines: pops up an editor to allow for full-text editing of a worksheet.
- parse_worksheet_form: takes those lines and generates a set of items (triples)
- interpret_items: takes those triples and returns a structure that interprets all the directives in the worksheet item.

A worksheet contains a list of (worksheet) items, where each item includes
- bundle_uuid (only used if type == bundle)
- value (used for text and directive)
- type: one of the following:
  * markup: just plain plain text (markdown)
  * bundle: represents a bundle
  * directive: special instructions for determining formatting
This is the representation in the DB.
In the code, we have full items of the form (bundle_info, value_obj, type).
In other words, there are two representations of worksheet items:
- (bundle_uuid, value, type) [inserted into the database]
- (bundle_info, value_obj, type) [used in the code]

The value of a directive can be parsed into a sequence of tokens, where the
first token determines the type.  Types of directives:
% title <title text>
%
% schema <schema name>
% addschema <schema name>
% add <field name> <genpath (e.g., /stats:errorRate)> <post-processing>
%
%% this is a comment
% display hidden
% display inline <genpath (e.g., /stats:errorRate)>
% display contents <genpath (e.g., /out.txt)>
% display image <genpath (e.g., /graph.png)>
% display html <genpath (e.g., /test.html)>
% display record <schema name>
% display table <schema name>
%
% search <keywords>

A genpath (generalized path) is either:
- a bundle field (e.g., 'command')
- a metadata field (e.g., 'name')
- a path (starts with '/'), but can descend into a YAML file (e.g., /stats:train/errorRate)
'''
import os
import re
import subprocess
import sys
import tempfile
import yaml

from codalab.common import UsageError
from codalab.lib import path_util, canonicalize, formatting, editor_util, spec_util

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
    '''
    Input (string): a b 'c d' e
    Output (array): ["a", "b", "c d", "e"]
    Both single and double quotes are supported.
    '''
    #print s
    tokens = []
    i = 0
    while i < len(s):
        # Every time we enter the loop, we're at the beginning of a token.
        if s[i] == '"' or s[i] == '\'':
            while True:
                try:
                    j = s.index(s[i], i+1)
                except:
                    raise UsageError('Unclosed quote: %s' % s)
                if s[j-1] != '\\': break
            tokens.append(s[i+1:j].replace('\\'+s[i], s[i]))
            j += 1 # Skip over the last quote
        else:
            try:
                j = s.index(' ', i+1)
            except:
                j = len(s)
            tokens.append(s[i:j])
        i = j
        # Skip over spaces
        while i < len(s) and s[i] == ' ': i += 1
    return tokens

############################################################

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
//   * add <descriptor> | add <key name> <value source> [post processing]
//   * display hidden
//   * display inline|contents|image|html <value source>
//   * display record|table <schema name>
// For example, you can define a schema for a table and then set the display mode to using that schema:
// %% schema s1
// %% add name
// %% add /stats:errorRate %%.3f
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

    lines = editor_util.open_and_edit(suffix='.md', template=template)
    if not lines:
        lines = template_lines
    # Process the result
    form_result = [line.rstrip() for line in lines]
    if form_result == template_lines:
        raise UsageError('No change made; aborting')
    return form_result


def get_bundle_uuid(client, worksheet_uuid, bundle_spec):
    '''
    Return the bundle_uuid corresponding to bundle_spec.
    Important difference from client.get_bundle_uuid: if bundle_spec is already
    a uuid, then just return it directly.  This avoids an extra call to the
    client.
    '''
    if spec_util.UUID_REGEX.match(bundle_spec):
        bundle_uuid = bundle_spec
    else:  # Already uuid, don't need to look up specification
        bundle_uuid = client.get_bundle_uuid(worksheet_uuid, bundle_spec)
    return bundle_uuid

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
                bundle_uuid = get_bundle_uuid(client, worksheet_uuid, m.group(3))
                bundle_info = {'uuid': bundle_uuid}  # info doesn't need anything other than uuid
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
            command = string_to_tokens(line[1:].strip())
            # Replace ^ with the reference to the last bundle.
            command = [(bundle_uuids[-1] if arg == '^' else arg) for arg in command]
            commands.append(command)
        else:
            result.append(parse(line))

    return (result, commands)

def interpret_genpath(bundle_info, genpath):
    '''
    This function is called in the first server call to a BundleClient to
    quickly interpret the genpaths (generalized path) that only require looking
    bundle_info (e.g., 'time', 'command').  The interpretation of generalized
    paths that require reading files is done by interpret_file_genpath.
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
        return (bundle_info['uuid'], genpath)

    # Either bundle info or metadata
    value = bundle_info.get(genpath, None)
    if not value: value = bundle_info['metadata'].get(genpath, None)
    return value or ''

def interpret_file_genpath(client, target_cache, bundle_uuid, genpath):
    '''
    |client|: used to read files
    |cache| is a mapping from target (bundle_uuid, subpath) to the info map,
    which is to be read/written to avoid reading/parsing the same file many
    times.
    |genpath| specifies the subpath and various fields (e.g., for
    /stats:train/errorRate, subpath = 'stats', key = 'train/errorRate').
    Return the string value.
    '''
    MAX_LINES = 1000  # Maximum number of lines we need to read from a file.

    # Load the file
    if not genpath.startswith('/'):
        raise UsageError('Not file genpath: %s' % genpath)
    genpath = genpath[1:]
    if ':' in genpath:  # Looking for a particular key in the file
        subpath, key = genpath.split(':')
    else:
        subpath, key = genpath, None
    target = (bundle_uuid, subpath)
    if target not in target_cache:
        #print 'LOAD', target
        contents = client.head_target(target, MAX_LINES)
        if contents != None:
            if all('\t' in x for x in contents):
                # Tab-separated file (key\tvalue\nkey\tvalue...)
                info = {}
                for x in contents:
                    kv = x.strip().split("\t", 1)
                    if len(kv) == 2: info[kv[0]] = kv[1]
            else:
                # YAML file
                info = yaml.load('\n'.join(contents))
        else:
            info = None
        target_cache[target] = info

    # Traverse the info object.
    info = target_cache.get(target, None)
    if key and info:
        for k in key.split('/'):
            info = info.get(k, None) if isinstance(info, dict) else None
            if info == None: break
    return info

def canonicalize_schema_item(args):
    '''
    Users who type in schema items can specify a partial argument list.
    Return the canonicalize version (a triple).
    '''
    if len(args) == 1:  # genpath
        return (os.path.basename(args[0]).split(":")[-1], args[0], None)
    elif len(args) == 2:  # name genpath
        return (args[0], args[1], None)
    elif len(args) == 3:  # name genpath post-processing
        return (args[0], args[1], args[2])
    else:
        raise UsageError('Invalid number of arguments: %s' % (args,))

def canonicalize_schema_items(items):
    return [canonicalize_schema_item(item) for item in items]

def apply_func(func, arg):
    '''
    Apply post-processing function |func| to |arg|.
    |func| is a string representing a list of functions (which are to be
    applied to |arg| in succession).  Each function is either:
    - 'time', 'size' for special formatting
    - '%...' for sprintf style formatting
    - s/... for regular expression substitution
    - [a:b] for taking substrings
    '''
    FUNC_DELIM = ' | '
    if isinstance(arg, tuple):
        # tuples are (bundle_uuid, genpath) which have not been fleshed out
        return arg + (func,)
    try:
        if func == None: return arg
        # String encoding of a function: size s/a/b
        for f in func.split(FUNC_DELIM):
            if f == 'date':
                arg = formatting.date_str(arg)
            elif f == 'duration':
                arg = formatting.duration_str(float(arg)) if arg else ''
            elif f == 'size':
                arg = formatting.size_str(arg)
            elif f.startswith('%'):
                arg = (f % float(arg)) if arg else ''
            elif f.startswith('s/'):  # regular expression
                _, s, t = f.split("/")
                arg = re.sub(s, t, arg)
            elif f.startswith('['):  # substring
                m = re.match('\[(.*):(.*)\]', f)
                if m:
                    start = int(m.group(1) or 0)
                    end = int(m.group(2) or -1)
                    arg = arg[start:end]
                else:
                    return '<invalid function: %s>' % f
            else:
                return '<invalid function: %s>' % f
        return arg
    except:
        # Can't apply the function, so just return the arg.
        return arg

def get_default_schemas():
    created = ['created', 'created', 'date']
    data_size = ['data_size', 'data_size', 'size']
    schemas = {}

    schemas['default'] = canonicalize_schema_items([['name'], ['bundle_type'], created, data_size, ['state']])

    schemas['program'] = canonicalize_schema_items([['name'], created, data_size])
    schemas['dataset'] = canonicalize_schema_items([['name'], created, data_size])

    schemas['make'] = canonicalize_schema_items([['name'], created, ['dependencies'], ['state']])
    schemas['run'] = canonicalize_schema_items([['name'], created, ['dependencies'], ['command'], ['state'], ['time']])
    return schemas

def interpret_items(schemas, items):
    '''
    schemas: initial mapping from name to list of schema items (columns of a table)
    items: list of worksheet items (triples) to interpret
    Return a list of items, where each item is either:
    - ('markup'|'inline'|'contents', string)
    - ('record'|'table', (col1, ..., coln), [{col1:value1, ... coln:value2}, ...]),
      where value is either a string (already rendered) or a (bundle_uuid, genpath, post) tuple
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
                interpreted = interpret_genpath(bundle_info, args[0])
                if not interpreted:
                    raise UsageError('Invalid argument to display %s: %s in the context of bundle %s (file required)' % (mode, args[0], bundle_info['uuid']))
                new_items.append({
                    'mode': mode,
                    'interpreted':interpreted,
                    'bundle_info': bundle_info
                })
        elif mode == 'image':
            bundle_info = bundle_infos if len(bundle_infos) else None
            new_items.append({
                'mode': mode,
                'interpreted': args[0],
                'bundle_info': bundle_info
            })
        elif mode == 'html':
            bundle_info = bundle_infos if len(bundle_infos) else None
            new_items.append({
                'mode': mode,
                'interpreted': args[0],
                'bundle_info': bundle_info
            })
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
                    rows.append({
                        'key': name + ':',
                        'value': apply_func(post, interpret_genpath(bundle_info, genpath))
                    })
                new_items.append({
                    'mode':mode,
                    'interpreted':(header, rows),
                    'bundle_info': bundle_info
                })
        elif mode == 'table':
            # display table schema =>
            # key1       key2
            # b1_value1  b1_value2
            # b2_value1  b2_value2
            schema = schemas[args[0] if len(args) > 0 else 'default']
            header = tuple(name for (name, genpath, post) in schema)
            rows = []
            for bundle_info in bundle_infos:
                row = {}
                rows.append({name : apply_func(post, interpret_genpath(bundle_info, genpath)) for (name, genpath, post) in schema})
            new_items.append({
                    "mode":mode,
                    'interpreted': (header, rows),
                    'bundle_info': bundle_info
                })
        else:
            raise UsageError('Unknown display mode: %s' % mode)
        bundle_infos[:] = []  # Clear


    for (bundle_info, value_obj, item_type) in items:
        if item_type == TYPE_BUNDLE:
            bundle_infos.append(bundle_info)
        elif item_type == TYPE_MARKUP:
            flush()
            new_items.append({
                    'mode': TYPE_MARKUP,
                    'interpreted': value_obj,
                    'bundle_info': bundle_info
                })
            pass
        elif item_type == TYPE_DIRECTIVE:
            flush()
            if len(value_obj) == 0: continue
            command = value_obj[0]
            if command == 'title':
                result['title'] = value_obj[1]
            elif command == 'schema':
                name = value_obj[1]
                schemas[name] = current_schema = []
            elif command == 'addschema':
                name = value_obj[1]
                current_schema += schemas[name]
            elif command == 'add':
                schema_item = canonicalize_schema_item(value_obj[1:])
                current_schema.append(schema_item)
            elif command == 'display':
                current_display = value_obj[1:]
            elif command == 'search':
                keywords = value_obj[1:]
                mode = command
                data = {'keywords': keywords, 'display': current_display, 'schemas': schemas}
                new_items.append({
                        'mode': TYPE_DIRECTIVE,
                        'interpreted': data,
                        'bundle_info': bundle_info
                    })
            elif command == '%' or command == '':  # Comment
                pass
            else:
                raise UsageError('Unknown command: %s' % command)
    flush()
    result['items'] = new_items
    return result

def interpret_search(client, worksheet_uuid, data):
    '''
    Input: specification of a search query.
    Output: worksheet items based on the result of issuing the search query.
    '''
    # First item determines the display
    items = [(None, ['display'] + data['display'], TYPE_DIRECTIVE)]

    # Next come the actual bundles
    bundle_uuids = client.search_bundle_uuids(worksheet_uuid, data['keywords'], 100, False)
    for bundle_uuid in bundle_uuids:
        items.append((client.get_bundle_info(bundle_uuid), None, TYPE_BUNDLE))

    # Finally, interpret the items
    return interpret_items(data['schemas'], items)
