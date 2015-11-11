'''
worksheet_util contains the following public functions:
- request_lines: pops up an editor to allow for full-text editing of a worksheet.
- parse_worksheet_form: takes those lines and generates a set of items (triples)
- interpret_items: takes those triples and returns a structure that interprets all the directives in the worksheet item.

A worksheet contains a list of (worksheet) items, where each item includes
- bundle_uuid (only used if type == bundle)
- subworkheet_uuid (only used if type == worksheet)
- value (used for text and directive)
- type (one of the following)
  * markup: just plain plain text (markdown)
  * directive: special instructions for determining formatting
  * bundle: represents a bundle
  * worksheet: represents a worksheet
This is the representation in the DB.
In the code, we have full items of the form (bundle_info, subworkheet_info, value_obj, type).
In other words, there are two representations of worksheet items:
- (bundle_uuid, subworksheet_uuid, value, type) [inserted into the database]
- (bundle_info, subworksheet_info, value_obj, type) [used in the code]

A genpath (generalized path) is either:
- a bundle field (e.g., 'command')
- a metadata field (e.g., 'name')
- a path (starts with '/'), but can descend into a YAML file (e.g., /stats:train/errorRate)

See get_worksheet_lines for documentation on the specification of the directives.
'''
import copy
import os
import re
import types
import yaml
import json
from itertools import izip

from codalab.common import UsageError
from codalab.lib import path_util, canonicalize, formatting, editor_util, spec_util
from codalab.objects.permission import permission_str, group_permissions_str

# Types of worksheet items
TYPE_MARKUP = 'markup'
TYPE_DIRECTIVE = 'directive'
TYPE_BUNDLE = 'bundle'
TYPE_WORKSHEET = 'worksheet'

def markup_item(x): return (None, None, x, TYPE_MARKUP)
def directive_item(x): return (None, None, x, TYPE_DIRECTIVE)
def bundle_item(x): return (x, None, '', TYPE_BUNDLE)  # TODO: replace '' with None when tables.py schema is updated
def subworksheet_item(x): return (None, x, '', TYPE_WORKSHEET)  # TODO: replace '' with None when tables.py schema is updated


BUNDLE_REGEX = re.compile('^(\[(.*)\])?\s*\{([^{]*)\}$')
SUBWORKSHEET_REGEX = re.compile('^(\[(.*)\])?\s*\{\{(.*)\}\}$')
def bundle_line(description, uuid): return '[%s]{%s}' % (description, uuid)
def worksheet_line(description, uuid): return '[%s]{{%s}}' % (description, uuid)

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
    tokens = []
    i = 0
    while i < len(s):
        # Every time we enter the loop, we're at the beginning of a token.
        if s[i] == '"' or s[i] == '\'':
            j = i
            while True:
                try:
                    j = s.index(s[i], j+1)
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

def get_worksheet_info_edit_command(raw_command_map):
    '''
    Return a cli-command for editing worksheet-info. Return None if raw_command_map contents are invalid.
    Input:
        raw_command: a map containing the info to edit, new_value and the action to perform
    '''
    if not raw_command_map.get('k') or not raw_command_map.get('v') or not raw_command_map.get('action') == 'worksheet-edit':
        return None
    return 'wedit -{k[0]} "{v}"'.format(**raw_command_map)

def convert_item_to_db(item):
    (bundle_info, subworksheet_info, value_obj, item_type) = item
    return (
        bundle_info['uuid'] if bundle_info else None,
        subworksheet_info['uuid'] if subworksheet_info else None,
        # TODO: change tables.py so that None's are allowed
        (tokens_to_string(value_obj) if item_type == TYPE_DIRECTIVE else value_obj) or '',
        item_type,
    )

def get_worksheet_lines(worksheet_info):
    '''
    Generator that returns pretty-printed lines of text for the given worksheet.
    '''
    lines = []
    for (bundle_info, subworksheet_info, value_obj, item_type) in worksheet_info['items']:
        if item_type == TYPE_MARKUP:
            lines.append(value_obj)
        elif item_type == TYPE_DIRECTIVE:
            print value_obj
            if value_obj[0] == DIRECTIVE_CHAR:
                # A comment directive
                lines.append('//' + ' '.join(value_obj[1:]))
            else:
                # A normal directive
                value = tokens_to_string(value_obj)
                value = DIRECTIVE_CHAR + ('' if len(value) == 0 or value.startswith(DIRECTIVE_CHAR) else ' ') + value
                lines.append(value)
        elif item_type == TYPE_BUNDLE:
            if 'metadata' not in bundle_info:
                # This happens when we add bundles by uuid and don't actually make sure they exist
                #lines.append('ERROR: non-existent bundle %s' % bundle_info['uuid'])
                description = formatting.contents_str(None)
            else:
                metadata = bundle_info['metadata']
                description = bundle_info['bundle_type']
                description += ' ' + metadata['name']
                deps = interpret_genpath(bundle_info, 'dependencies')
                if deps: description += ' -- ' + deps
                command = bundle_info.get('command')
                if command: description += ' : ' + command
            lines.append(bundle_line(description, bundle_info['uuid']))
        elif item_type == TYPE_WORKSHEET:
            lines.append(worksheet_line('worksheet ' + formatting.contents_str(subworksheet_info.get('name')), subworksheet_info['uuid']))
        else:
            raise RuntimeError('Invalid worksheet item type: %s' % type)
    return lines

def get_formatted_metadata(cls, metadata, raw=False):
    '''
    Input:
        cls: bundle subclass (e.g. DatasetBundle, RuunBundle, ProgramBundle)
        metadata: bundle metadata
        raw: boolean value indicating if the raw value needs to be returned
    Return a list of tuples containing the key and formatted value of metadata.
    '''
    result = []
    for spec in cls.METADATA_SPECS:
        key = spec.key
        if not raw:
            if key not in metadata: continue
            if metadata[key] == '' or metadata[key] == []: continue
            value = apply_func(spec.formatting, metadata.get(key))
            if isinstance(value, list): value = ' | '.join(value)
        else:
            value = metadata.get(key)
        result.append((key, value))
    return result

def get_editable_metadata_fields(cls, metadata):
    '''
    Input:
        cls: bundle subclass (e.g. DatasetBundle, RuunBundle, ProgramBundle)
        metadata: bundle metadata
    Return a list of metadata fields that are editable by the owner.
    '''
    result = []
    for spec in cls.METADATA_SPECS:
        key = spec.key
        if not spec.generated:
            result.append(key)
    return result

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

def get_bundle_uuids(client, worksheet_uuid, bundle_specs):
    '''
    Return the bundle_uuids corresponding to bundle_specs.
    Important difference from client.get_bundle_uuids: if all bundle_specs are already
    uuids, then just return them directly.  This avoids an extra call to the client.
    '''
    bundle_uuids = {}
    unresolved = []
    for spec in bundle_specs:
        spec = spec.strip()
        if spec_util.UUID_REGEX.match(spec):
            bundle_uuids[spec] = spec
        else:
            unresolved.append(spec)

    # Resolve uuids with a batch call to the client and update dict
    bundle_uuids.update(zip(unresolved, client.get_bundle_uuids(worksheet_uuid, unresolved)))

    # Return uuids for the bundle_specs in the original order provided
    return [bundle_uuids[spec] for spec in bundle_specs]

def get_bundle_uuid(client, worksheet_uuid, bundle_spec):
    '''
    Return the bundle_uuid corresponding to a single bundle_spec.
    If bundle_spec is already a uuid, then just return it directly.
    This avoids an extra call to the client.
    '''
    return get_bundle_uuids(client, worksheet_uuid, [bundle_spec])[0]

def get_worksheet_uuid(client, base_worksheet_uuid, worksheet_spec):
    '''
    Same thing as get_bundle_uuid, but for worksheets.
    '''
    worksheet_spec = worksheet_spec.strip()
    if spec_util.UUID_REGEX.match(worksheet_spec):
        worksheet_uuid = worksheet_spec  # Already uuid, don't need to look up specification
    else:
        worksheet_uuid = client.get_worksheet_uuid(base_worksheet_uuid, worksheet_spec)
    return worksheet_uuid

def parse_worksheet_form(form_result, client, worksheet_uuid):
    '''
    Input: form_result is a list of lines.
    Return (list of (bundle_info, subworksheet_info, value, type) tuples, commands to execute)
    '''
    def get_line_type(line):
        if line.startswith('!'):  # Run commands
            return 'command'
        elif line.startswith('//'):
            return 'comment'
        elif BUNDLE_REGEX.match(line) is not None:
            return TYPE_BUNDLE
        elif SUBWORKSHEET_REGEX.match(line) is not None:
            return TYPE_WORKSHEET
        elif DIRECTIVE_REGEX.match(line) is not None:
            return TYPE_DIRECTIVE
        else:
            return TYPE_MARKUP

    line_types = [get_line_type(line) for line in form_result]

    # Extract bundle specs and resolve uuids in one batch
    # bundle_specs = (line_indices, bundle_specs)
    bundle_specs = zip(*[(i, BUNDLE_REGEX.match(line).group(3))
                    for i, line in enumerate(form_result)
                    if line_types[i] == TYPE_BUNDLE])
    # bundle_uuids = {line_i: bundle_uuid, ...}
    bundle_uuids = dict(zip(bundle_specs[0], get_bundle_uuids(client, worksheet_uuid, bundle_specs[1])))

    commands = []
    items = []
    for line_i, (line_type, line) in enumerate(izip(line_types, form_result)):
        if line_type == 'command':
            command = string_to_tokens(line[1:].strip())
            # The user can specify '!<command> ^', which perform actions on the previous bundle.
            # Replace ^ with the reference to the last bundle.
            command = [(bundle_uuids[-1][1] if arg == '^' else arg) for arg in command]
            commands.append(command)
        elif line_type == 'comment':
            comment = line[2:]
            items.append(directive_item([DIRECTIVE_CHAR, comment]))
        elif line_type == TYPE_BUNDLE:
            bundle_info = {'uuid': bundle_uuids[line_i]}  # info doesn't need anything other than uuid
            items.append(bundle_item(bundle_info))
        elif line_type == TYPE_WORKSHEET:
            subworksheet_spec = SUBWORKSHEET_REGEX.match(line).group(3)
            try:
                subworksheet_uuid = get_worksheet_uuid(client, worksheet_uuid, subworksheet_spec)
                subworksheet_info = {'uuid': subworksheet_uuid}  # info doesn't need anything other than uuid
                items.append(subworksheet_item(subworksheet_info))
            except UsageError, e:
                items.append(markup_item(e.message + ': ' + line))
        elif line_type == TYPE_DIRECTIVE:
            directive = DIRECTIVE_REGEX.match(line).group(1)
            items.append(directive_item(string_to_tokens(directive)))
        elif line_type == TYPE_MARKUP:
            items.append(markup_item(line))
        else:
            raise RuntimeError("Invalid line type: this should not happen.")

    return items, commands

def is_file_genpath(genpath):
    # Return whether the genpath is a file (e.g., '/stdout') or not (e.g., 'command')
    return genpath.startswith('/')

def interpret_genpath(bundle_info, genpath):
    '''
    This function is called in the first server call to a BundleClient to
    quickly interpret the genpaths (generalized path) that only require looking
    bundle_info (e.g., 'time', 'command').  The interpretation of generalized
    paths that require reading files is done by interpret_file_genpath.
    '''
    # If genpath is referring to a file, then just returns instructions for
    # fetching that file rather than actually doing it.
    if is_file_genpath(genpath):
        return (bundle_info['uuid'], genpath)

    # Render dependencies
    deps = bundle_info.get('dependencies', [])
    anonymous = len(deps) == 1 and deps[0]['child_path'] == ''
    def render_dep(dep, show_key=True, show_uuid=False):
        if show_key and not anonymous:
            if show_uuid or dep['child_path'] != dep['parent_name']:
                a = dep['child_path'] + ':'
            else:
                a = ':'
        else:
            a = ''
        b = dep['parent_uuid'] if show_uuid else (dep['parent_name'] or '')
        c = '/' + dep['parent_path'] if dep['parent_path'] else ''
        return a + b + c

    # Special genpaths (dependencies, args)
    if genpath == 'dependencies':
        return ','.join([render_dep(dep) for dep in deps])
    elif genpath.startswith('dependencies/'):
        # Look up the particular dependency
        _, name = genpath.split('/', 1)
        for dep in deps:
            if dep['child_path'] == name:
                return render_dep(dep, show_key=False)
        return 'n/a'
    elif genpath == 'args':
        # Arguments that we would pass to 'cl'
        args = []
        bundle_type = bundle_info.get('bundle_type')
        if bundle_type not in ('make', 'run'): return None
        args += [bundle_type]
        for dep in deps:
            args.append(render_dep(dep, show_uuid=True))
        if bundle_info['command']:
            args.append(quote(bundle_info['command']))
        return ' '.join(args)
    elif genpath == 'host_worksheets':
        if 'host_worksheets' in bundle_info:
            return ' '.join('%s(%s)' % (info['name'], info['uuid']) for info in bundle_info['host_worksheets'])
    elif genpath == 'permission':
        if 'permission' in bundle_info:
            return permission_str(bundle_info['permission'])
    elif genpath == 'group_permissions':
        if 'group_permissions' in bundle_info:
            return group_permissions_str(bundle_info['group_permissions'])

    # Bundle field?
    value = bundle_info.get(genpath)
    if value != None: return value

    # Metadata field?
    value = bundle_info.get('metadata', {}).get(genpath)
    if value != None: return value

    return None

def interpret_file_genpath(client, target_cache, bundle_uuid, genpath, post):
    '''
    |client|: used to read files
    |cache| is a mapping from target (bundle_uuid, subpath) to the info map,
    which is to be read/written to avoid reading/parsing the same file many
    times.
    |genpath| specifies the subpath and various fields (e.g., for
    /stats:train/errorRate, subpath = 'stats', key = 'train/errorRate').
    |post| function to apply to the resulting value.
    Return the string value.
    '''
    MAX_LINES = 1000  # Maximum number of lines we need to read from a file.

    # Load the file
    if not is_file_genpath(genpath):
        raise UsageError('Not file genpath: %s' % genpath)
    genpath = genpath[1:]
    if ':' in genpath:  # Looking for a particular key in the file
        subpath, key = genpath.split(':')
    else:
        subpath, key = genpath, None

    # Just a link
    if post == 'link':
        # TODO: need to synchronize with frontend
        return '/%s' % os.path.join('api', 'bundles', 'filecontent', bundle_uuid, subpath)

    target = (bundle_uuid, subpath)
    if target not in target_cache:
        #print 'LOAD', target
        contents = client.head_target(target, MAX_LINES)
        # Try to interpret the structure of the file by looking inside it.
        if contents != None:
            import base64
            contents = map(base64.b64decode, contents)
            if all('\t' in x for x in contents):
                # Tab-separated file (key\tvalue\nkey\tvalue...)
                info = {}
                for x in contents:
                    kv = x.strip().split("\t", 1)
                    if len(kv) == 2: info[kv[0]] = kv[1]
            elif contents[0][0] == '{':
                # JSON file (hack)
                info = json.loads(''.join(contents))
            else:
                try:
                    # YAML file
                    info = yaml.load(''.join(contents))
                except:
                    # Plain text file
                    info = ''.join(contents)
        else:
            info = None
        target_cache[target] = info

    # Traverse the info object.
    info = target_cache.get(target, None)
    if key != None and info != None:
        for k in key.split('/'):
            if isinstance(info, dict):
                info = info.get(k, None)
            elif isinstance(info, list):
                try:
                    info = info[int(k)]
                except:
                    info = None
            else:
                info = None
            if info == None: break
    return apply_func(post, info)

def format_metadata(metadata):
    '''
    Format worksheet item metadata based on field type specified in the schema.
    '''
    if metadata:
        unformatted_fields = [(name, func) for (_, name, func) in get_default_schemas()['default'] if func]
        for (name, func) in unformatted_fields:
            if metadata.get(name):
                metadata[name] = apply_func(func, metadata[name])

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
    - 'duration', 'date', 'size' for special formatting
    - '%...' for sprintf-style formatting
    - s/.../... for regular expression substitution
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
                arg = formatting.date_str(float(arg)) if arg != None else None
            elif f == 'duration':
                arg = formatting.duration_str(float(arg)) if arg != None else None
            elif f == 'size':
                arg = formatting.size_str(float(arg)) if arg != None else None
            elif f.startswith('%'):
                arg = (f % float(arg)) if arg != None else None
            elif f.startswith('s/'):  # regular expression: s/<old string>/<new string>
                esc_slash = '_ESC_SLASH_'  # Assume this doesn't occur in s
                # Preserve escaped characters: \/
                tokens = f.replace('\\/', esc_slash).split('/')
                if len(tokens) != 3:
                    return '<invalid regex: %s>' % f
                s = tokens[1].replace(esc_slash, '/')
                t = tokens[2].replace(esc_slash, '/')
                arg = re.sub(s, t, arg)
            elif f.startswith('['):  # substring
                m = re.match('\[(.*):(.*)\]', f)
                if m:
                    start = int(m.group(1) or 0)
                    end = int(m.group(2) or len(arg))
                    arg = arg[start:end]
                else:
                    return '<invalid function: %s>' % f
            elif f.startswith('add '):
                # 'add k v' checks if arg is a dictionary and updates it with arg[k] = v
                if isinstance(arg, dict):
                    k, v = f.split(' ')[1:]
                    arg[k] = v
                else:
                    return 'arg (%s) not a dictionary' % type(arg)
            elif f.startswith('key '):
                # 'key k' converts arg into a dictionary where arg[k] = arg
                arg = {f.split(' ')[1]: arg}
            else:
                return '<invalid function: %s>' % f
        return arg
    except:
        # Applying the function failed, so just return the arg.
        return arg

def get_default_schemas():
    uuid = ['uuid', 'uuid', '[0:8]']
    created = ['created', 'created', 'date']
    data_size = ['data_size', 'data_size', 'size']
    time = ['time', 'time', 'duration']
    description = ['description']
    schemas = {}

    schemas['default'] = canonicalize_schema_items([uuid, ['name'], description, ['bundle_type'], created, ['dependencies'], ['command'], data_size, ['state']])

    schemas['program'] = canonicalize_schema_items([uuid, ['name'], description, created, data_size])
    schemas['dataset'] = canonicalize_schema_items([uuid, ['name'], description, created, data_size])

    schemas['make'] = canonicalize_schema_items([uuid, ['name'], description, created, ['dependencies'], ['state']])
    schemas['run'] = canonicalize_schema_items([uuid, ['name'], description, created, ['dependencies'], ['command'], ['state'], time])
    return schemas

def interpret_items(schemas, items):
    '''
    schemas: initial mapping from name to list of schema items (columns of a table)
    items: list of worksheet items (triples) to interpret
    Return a list of interpreted items, where each item is either:
    - ('markup'|'contents'|'image'|'html', rendered string | (bundle_uuid, genpath, properties))
    - ('record'|'table', (col1, ..., coln), [{col1:value1, ... coln:value2}, ...]),
      where value is either a rendered string or a (bundle_uuid, genpath, post) tuple
    - ('search', [keyword, ...])
    '''
    result = {}

    # Set default schema
    current_schema = None

    default_display = ('table', 'default')
    current_display_ref = [default_display]
    new_items = []
    bundle_infos = []
    def get_schema(args):  # args is a list of schema names
        args = args if len(args) > 0 else ['default']
        schema = []
        for arg in args:
            schema += schemas[arg]
        return schema
    def is_missing(info): return 'metadata' not in info
    def flush():
        '''
        Gathered a group of bundles (in a table), which we can group together.
        '''
        if len(bundle_infos) == 0:
            return
        # Print out the curent bundles somehow
        mode = current_display_ref[0][0]
        args = current_display_ref[0][1:]
        properties = {}
        if mode == 'hidden':
            pass
        elif mode == 'contents' or mode == 'image' or mode == 'html':
            for bundle_info in bundle_infos:
                if is_missing(bundle_info):
                    continue

                # Result: either a string (rendered) or (bundle_uuid, genpath, properties) triple
                interpreted = interpret_genpath(bundle_info, args[0])

                # Properties - e.g., height, width, maxlines (optional)
                if len(args) > 1:
                    properties = dict(item.split('=') for item in args[1].split(','))

                if isinstance(interpreted, tuple):  # Not rendered yet
                    bundle_uuid, genpath = interpreted
                    if not is_file_genpath(genpath):
                        raise UsageError('Expected a file genpath, but got %s' % genpath)
                    else:
                        # interpreted is a target: strip off the leading /
                        interpreted = (bundle_uuid, genpath[1:])
                new_items.append({
                    'mode': mode,
                    'interpreted': interpreted,
                    'properties': properties,
                    'bundle_info': copy.deepcopy(bundle_info)
                })
        elif mode == 'record':
            # display record schema =>
            # key1: value1
            # key2: value2
            # ...
            schema = get_schema(args)
            for bundle_info in bundle_infos:
                header = ('key', 'value')
                rows = []
                for (name, genpath, post) in schema:
                    rows.append({
                        'key': name + ':',
                        'value': apply_func(post, interpret_genpath(bundle_info, genpath))
                    })
                new_items.append({
                    'mode': mode,
                    'interpreted': (header, rows),
                    'properties': properties,
                    'bundle_info': copy.deepcopy(bundle_info)
                })
        elif mode == 'table':
            # display table schema =>
            # key1       key2
            # b1_value1  b1_value2
            # b2_value1  b2_value2
            schema = get_schema(args)
            header = tuple(name for (name, genpath, post) in schema)
            rows = []
            for bundle_info in bundle_infos:
                if 'metadata' not in bundle_info:
                    continue
                rows.append({name: apply_func(post, interpret_genpath(bundle_info, genpath)) for (name, genpath, post) in schema})
            new_items.append({
                    'mode': mode,
                    'interpreted': (header, rows),
                    'properties': properties,
                    'bundle_info': copy.deepcopy(bundle_infos)
                })
        else:
            raise UsageError('Unknown display mode: %s' % mode)
        bundle_infos[:] = []  # Clear

        # Reset display to minimize the long distance dependencies of directives
        if item_type != TYPE_BUNDLE:
            current_display_ref[0] = default_display
        # Reset schema to minimize long distance dependencies of directives
        if item_type != TYPE_DIRECTIVE:
            current_schema = None

    def get_command(value_obj):  # For directives only
        return value_obj[0] if len(value_obj) > 0 else None
    for item in items:
        (bundle_info, subworksheet_info, value_obj, item_type) = item
        properties = {}

        if item_type == TYPE_BUNDLE:
            bundle_infos.append(bundle_info)
        elif item_type == TYPE_WORKSHEET:
            flush()
            new_items.append({
                'mode': TYPE_WORKSHEET,
                'interpreted': subworksheet_info,  # TODO: convert into something more useful?
                'properties': {},
                'subworksheet_info': subworksheet_info,
            })
        elif item_type == TYPE_MARKUP:
            flush()
            new_items.append({
                'mode': TYPE_MARKUP,
                'interpreted': value_obj,
                'properties': {},
            })
        elif item_type == TYPE_DIRECTIVE:
            flush()
            command = get_command(value_obj)
            if command == '%' or command == '' or command == None:  # Comment
                pass
            elif command == 'schema':
                name = value_obj[1]
                schemas[name] = current_schema = []
            elif command == 'addschema':
                if current_schema == None:
                    raise UsageError("%s called, but no current schema (must call 'schema <schema-name>' first)" % value_obj)
                name = value_obj[1]
                current_schema += schemas[name]
            elif command == 'add':
                if current_schema == None:
                    raise UsageError("%s called, but no current schema (must call 'schema <schema-name>' first)" % value_obj)
                schema_item = canonicalize_schema_item(value_obj[1:])
                current_schema.append(schema_item)
            elif command == 'display':
                current_display_ref[0] = value_obj[1:]
            elif command == 'search':
                keywords = value_obj[1:]
                mode = command
                data = {'keywords': keywords, 'display': current_display_ref[0], 'schemas': schemas}
                new_items.append({
                    'mode': mode,
                    'interpreted': data,
                    'properties': {},
                })
            else:
                new_items.append({
                    'mode': TYPE_MARKUP,
                    'interpreted': 'ERROR: unknown directive **%% %s**' % ' '.join(value_obj),
                    'properties': {},
                })
                #raise UsageError('Unknown directive command in %s' % value_obj)
        else:
            raise RuntimeError('Unknown worksheet item type: %s' % item_type)

    flush()
    result['items'] = new_items

    return result

def interpret_genpath_table_contents(client, contents):
    '''
    contents represents a table, but some of the elements might not be interpreted.
    Interpret them by calling the client.
    '''
    # if called after an RPC call tuples may become lists
    need_gen_types = (types.TupleType, types.ListType)

    # Request information
    requests = []
    for r, row in enumerate(contents):
        for key, value in row.items():
            # value can be either a string (already rendered) or a (bundle_uuid, genpath, post) triple
            if isinstance(value, need_gen_types):
                requests.append(value)
    responses = client.interpret_file_genpaths(requests)

    # Put it in a table
    new_contents = []
    ri = 0
    for r, row in enumerate(contents):
        new_row = {}
        for key, value in row.items():
            if isinstance(value, need_gen_types):
                value = responses[ri]
                ri += 1
            new_row[key] = value
        new_contents.append(new_row)
    return new_contents

def interpret_search(client, worksheet_uuid, data):
    '''
    Input: specification of a search query.
    Output: worksheet items based on the result of issuing the search query.
    '''
    # First item determines the display
    items = [directive_item(('display',) + tuple(data['display']))]

    # Next come the actual bundles
    bundle_uuids = client.search_bundle_uuids(worksheet_uuid, data['keywords'])
    if not isinstance(bundle_uuids, list):  # Single number, just print it out...
        return interpret_items(data['schemas'], [markup_item(str(bundle_uuids))])

    bundle_infos = client.get_bundle_infos(bundle_uuids)
    for bundle_uuid in bundle_uuids:
        items.append(bundle_item(bundle_infos[bundle_uuid]))

    # Finally, interpret the items
    return interpret_items(data['schemas'], items)
