"""
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
"""
import copy
import os
import re
import sys
from itertools import izip


from codalab.common import PermissionError, UsageError
from codalab.lib import canonicalize, editor_util, formatting
from codalab.objects.permission import group_permissions_str, permission_str


# Note: this is part of the client's session, not server side.
CURRENT_WORKSHEET = '.'

# Types of worksheet items
TYPE_MARKUP = 'markup'
TYPE_DIRECTIVE = 'directive'
TYPE_BUNDLE = 'bundle'
TYPE_WORKSHEET = 'worksheet'

WORKSHEET_ITEM_TYPES = (TYPE_MARKUP, TYPE_DIRECTIVE, TYPE_BUNDLE, TYPE_WORKSHEET)

BUNDLE_REGEX = re.compile('^(\[(.*)\])?\s*\{([^{]*)\}$')
SUBWORKSHEET_REGEX = re.compile('^(\[(.*)\])?\s*\{\{(.*)\}\}$')

DIRECTIVE_CHAR = '%'
DIRECTIVE_REGEX = re.compile(r'^' + DIRECTIVE_CHAR + '\s*(.*)$')


def markup_item(x):
    return (None, None, x, TYPE_MARKUP)


def directive_item(x):
    return (None, None, x, TYPE_DIRECTIVE)


def bundle_item(x):
    return (x, None, '', TYPE_BUNDLE)  # TODO: replace '' with None when tables.py schema is updated


def subworksheet_item(x):
    return (None, x, '', TYPE_WORKSHEET)  # TODO: replace '' with None when tables.py schema is updated


def bundle_line(description, uuid):
    return '[%s]{%s}' % (description, uuid)


def worksheet_line(description, uuid):
    return '[%s]{{%s}}' % (description, uuid)


############################################################


def get_worksheet_info_edit_command(raw_command_map):
    """
    Return a cli-command for editing worksheet-info. Return None if raw_command_map contents are invalid.
    Input:
        raw_command: a map containing the info to edit, new_value and the action to perform
    """
    key = raw_command_map.get('k')
    value = raw_command_map.get('v')
    action = raw_command_map.get('action')
    if key is None or not key or value is None or not action == 'worksheet-edit':
        return None
    return 'wedit -{k[0]} "{v}"'.format(**raw_command_map)


def convert_item_to_db(item):
    (bundle_info, subworksheet_info, value_obj, item_type) = item
    return (
        bundle_info['uuid'] if bundle_info else None,
        subworksheet_info['uuid'] if subworksheet_info else None,
        # TODO: change tables.py so that None's are allowed
        (formatting.tokens_to_string(value_obj) if item_type == TYPE_DIRECTIVE else value_obj) or '',
        item_type,
    )


def get_worksheet_lines(worksheet_info):
    """
    Generator that returns pretty-printed lines of text for the given worksheet.
    """
    lines = []
    for item in worksheet_info['items']:
        (bundle_info, subworksheet_info, value_obj, item_type) = item

        if item_type == TYPE_MARKUP:
            lines.append(value_obj)
        elif item_type == TYPE_DIRECTIVE:
            if value_obj[0] == DIRECTIVE_CHAR:
                # A comment directive
                lines.append('//' + ' '.join(value_obj[1:]))
            else:
                # A normal directive
                value = formatting.tokens_to_string(value_obj)
                value = DIRECTIVE_CHAR + ('' if len(value) == 0 or value.startswith(DIRECTIVE_CHAR) else ' ') + value
                lines.append(value)
        elif item_type == TYPE_BUNDLE:
            if 'metadata' not in bundle_info:
                # This happens when we add bundles by uuid and don't actually make sure they exist
                # lines.append('ERROR: non-existent bundle %s' % bundle_info['uuid'])
                description = formatting.contents_str(None)
            else:
                metadata = bundle_info['metadata']
                # raise Exception(metadata)
                description = bundle_info['bundle_type']
                description += ' ' + metadata['name']
                deps = interpret_genpath(bundle_info, 'dependencies')
                if deps: description += ' -- ' + deps
                command = bundle_info.get('command')
                if command: description += ' : ' + command
            lines.append(bundle_line(description, bundle_info['uuid']))
        elif item_type == TYPE_WORKSHEET:
            lines.append(worksheet_line('worksheet ' + formatting.contents_str(subworksheet_info.get('name')),
                                        subworksheet_info['uuid']))
        else:
            raise RuntimeError('Invalid worksheet item type: %s' % type)
    return lines


def get_formatted_metadata(cls, metadata, raw=False):
    """
    Input:
        cls: bundle subclass (e.g. DatasetBundle, RuunBundle, ProgramBundle)
        metadata: bundle metadata
        raw: boolean value indicating if the raw value needs to be returned
    Return a list of tuples containing the key and formatted value of metadata.
    """
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


def get_editable_metadata_fields(cls):
    """
    Input:
        cls: bundle subclass (e.g. DatasetBundle, RuunBundle, ProgramBundle)
        metadata: bundle metadata
    Return a list of metadata fields that are editable by the owner.
    """
    result = []
    for spec in cls.METADATA_SPECS:
        key = spec.key
        if not spec.generated:
            result.append(key)
    return result


def get_metadata_types(cls):
    """
    Return map from key -> type for the metadata fields in the given bundle class.
    e.g.
       'request_time' -> 'basestring'
       'time' -> 'duration'
       'tags' -> 'list'

    Possible types: 'int', 'float', 'list', 'bool', 'duration',
                    'size', 'date', 'basestring'

    Special types like 'duration' are only indicated when client-side
    formatting/serialization is necessary.
    """
    return {
        spec.key: (not issubclass(spec.type, basestring) and spec.formatting) or spec.type.__name__
        for spec in cls.METADATA_SPECS
    }


def request_lines(worksheet_info):
    """
    Input: worksheet_info
    Popup an editor, populated with the current worksheet contents.
    Return a list of new items (bundle_uuid, value, type) that the user typed into the editor.
    """
    # Construct a form template with the current value of the worksheet.
    template_lines = get_worksheet_lines(worksheet_info)
    template = ''.join([line + os.linesep for line in template_lines])

    lines = editor_util.open_and_edit(suffix='.md', template=template)
    # Process the result
    form_result = [line.rstrip() for line in lines]
    if form_result == template_lines:
        raise UsageError('No change made; aborting')
    return form_result


def parse_worksheet_form(form_result, model, user, worksheet_uuid):
    """
    Input: form_result is a list of lines.
    Return (list of (bundle_info, subworksheet_info, value, type) tuples, commands to execute)
    """
    def get_line_type(line):
        if line.startswith('//'):
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
    bundle_lines = [
        (i, BUNDLE_REGEX.match(line).group(3))
        for i, line in enumerate(form_result)
        if line_types[i] == TYPE_BUNDLE
        ]
    # bundle_specs = (line_indices, bundle_specs)
    bundle_specs = zip(*bundle_lines) if len(bundle_lines) > 0 else [(), ()]
    # bundle_uuids = {line_i: bundle_uuid, ...}
    bundle_uuids = dict(zip(bundle_specs[0], canonicalize.get_bundle_uuids(model, user, worksheet_uuid, bundle_specs[1])))

    items = []
    for line_i, (line_type, line) in enumerate(izip(line_types, form_result)):
        if line_type == 'comment':
            comment = line[2:]
            items.append(directive_item([DIRECTIVE_CHAR, comment]))
        elif line_type == TYPE_BUNDLE:
            bundle_info = {'uuid': bundle_uuids[line_i]}  # info doesn't need anything other than uuid
            items.append(bundle_item(bundle_info))
        elif line_type == TYPE_WORKSHEET:
            subworksheet_spec = SUBWORKSHEET_REGEX.match(line).group(3)
            try:
                subworksheet_uuid = canonicalize.get_worksheet_uuid(model, user, worksheet_uuid, subworksheet_spec)
                subworksheet_info = {'uuid': subworksheet_uuid}  # info doesn't need anything other than uuid
                items.append(subworksheet_item(subworksheet_info))
            except UsageError, e:
                items.append(markup_item(e.message + ': ' + line))
        elif line_type == TYPE_DIRECTIVE:
            directive = DIRECTIVE_REGEX.match(line).group(1)
            items.append(directive_item(formatting.string_to_tokens(directive)))
        elif line_type == TYPE_MARKUP:
            items.append(markup_item(line))
        else:
            raise RuntimeError("Invalid line type %s: this should not happen." % line_type)

    return items


def is_file_genpath(genpath):
    # Return whether the genpath is a file (e.g., '/stdout') or not (e.g., 'command')
    return genpath.startswith('/')


def interpret_genpath(bundle_info, genpath):
    """
    Quickly interpret the genpaths (generalized path) that only require looking
    bundle_info (e.g., 'time', 'command').  The interpretation of generalized
    paths that require reading files is done by interpret_file_genpath.
    """
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
        return formatting.verbose_contents_str(None)
    elif genpath == 'args':
        # Arguments that we would pass to 'cl'
        args = []
        bundle_type = bundle_info.get('bundle_type')
        if bundle_type not in ('make', 'run'): return None
        args += [bundle_type]
        # Dependencies
        for dep in deps:
            args.append(render_dep(dep, show_uuid=True))
        # Command
        if bundle_info['command']:
            args.append(formatting.quote(bundle_info['command']))
        # Add request arguments from metadata
        metadata = bundle_info['metadata']
        for key, value in metadata.items():
            if key.startswith('request_') and value:
                key = key.replace('_', '-')
                if isinstance(value, bool):
                    args.append('--' + key)
                else:
                    args.extend(['--' + key, formatting.quote(str(value))])
        return ' '.join(args)
    elif genpath == 'summary':
        def friendly_render_dep(dep):
            key = dep['child_path'] or dep['parent_name']
            friendly_parent_name = formatting.verbose_contents_str(dep['parent_name'])
            value = key + '{' + (friendly_parent_name + ':' if key != dep['parent_name'] else '') + \
                dep['parent_uuid'][0:4] + '}'
            return key, value
        # Nice easy-to-ready description of how this bundle got created.
        bundle_type = bundle_info.get('bundle_type')
        if bundle_type in ('dataset', 'program'):
            return '[uploaded]'
        if bundle_type == 'make':
            args = []
            for dep in deps:
                args.append(friendly_render_dep(dep)[1])
            return '= ' + ' '.join(args)
        elif bundle_type == 'run':
            command = bundle_info['command']
            for dep in deps:
                key, value = friendly_render_dep(dep)
                # Replace full-word occurrences of key in the command with an indicator of the dependency.
                # Of course, a string match in the command isn't necessary a semantic reference to the dependency,
                # and there are some dependencies which are not explicit in the command.
                # But this can be seen as a best-effort attempt.
                command = re.sub(r'\b%s\b' % key, value, command)
            return '! ' + command
    elif genpath == 'host_worksheets':
        if 'host_worksheets' in bundle_info:
            return ' '.join('%s(%s)' % (info['name'], info['uuid']) for info in bundle_info['host_worksheets'])
    elif genpath == 'permission':
        if 'permission' in bundle_info:
            return permission_str(bundle_info['permission'])
    elif genpath == 'group_permissions':
        if 'group_permissions' in bundle_info:
            # FIXME(sckoo): we will be passing the old permissions format into this
            # which has been updated to accommodate the new formatting
            return group_permissions_str(bundle_info['group_permissions'])

    # Bundle field?
    value = bundle_info.get(genpath)
    if value is not None: return value

    # Metadata field?
    value = bundle_info.get('metadata', {}).get(genpath)
    if value is not None: return value

    return None


def format_metadata(metadata):
    """
    Format worksheet item metadata based on field type specified in the schema.
    """
    if metadata:
        unformatted_fields = [(name, func) for (_, name, func) in get_default_schemas()['default'] if func]
        for (name, func) in unformatted_fields:
            if metadata.get(name):
                metadata[name] = apply_func(func, metadata[name])


def canonicalize_schema_item(args):
    """
    Users who type in schema items can specify a partial argument list.
    Return the canonicalize version (a triple).
    """
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
    """
    Apply post-processing function |func| to |arg|.
    |func| is a string representing a list of functions (which are to be
    applied to |arg| in succession).  Each function is either:
    - 'duration', 'date', 'size' for special formatting
    - '%...' for sprintf-style formatting
    - s/.../... for regular expression substitution
    - [a:b] for taking substrings
    """
    FUNC_DELIM = ' | '
    if isinstance(arg, tuple):
        # tuples are (bundle_uuid, genpath) which have not been fleshed out
        return arg + (func,)
    try:
        if func is None:
            return arg
        # String encoding of a function: size s/a/b
        for f in func.split(FUNC_DELIM):
            if f == 'str':
                arg = str(arg)
            elif f == 'date':
                arg = formatting.date_str(float(arg)) if arg is not None else None
            elif f == 'duration':
                arg = formatting.duration_str(float(arg)) if arg is not None else None
            elif f == 'size':
                arg = formatting.size_str(float(arg)) if arg is not None else None
            elif f.startswith('%'):
                arg = (f % float(arg)) if arg is not None else None
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
    # Single fields
    uuid = ['uuid[0:8]', 'uuid', '[0:8]']
    name = ['name']
    summary = ['summary']
    data_size = ['data_size', 'data_size', 'size']
    time = ['time', 'time', 'duration']
    state = ['state']
    description = ['description']
    created = ['created', 'created', 'date']

    schemas = {}

    # Schemas corresponding to one field
    schemas['uuid'] = [uuid]
    schemas['name'] = [name]
    schemas['summary'] = [summary]
    schemas['data_size'] = [data_size]
    schemas['time'] = [time]
    schemas['state'] = [state]
    schemas['description'] = [description]
    schemas['created'] = [created]

    # Schemas involving multiple fields
    schemas['default'] = [uuid, name, summary, data_size, state, description]
    schemas['program'] = [uuid, name, data_size, description]
    schemas['dataset'] = [uuid, name, data_size, description]
    schemas['make'] = [uuid, name, summary, data_size, state, description]
    schemas['run'] = [uuid, name, summary, data_size, time, state, description]

    for key in schemas:
        schemas[key] = canonicalize_schema_items(schemas[key])

    return schemas


def interpret_items(schemas, raw_items):
    """
    schemas: initial mapping from name to list of schema items (columns of a table)
    raw_items: list of (raw) worksheet items (triples) to interpret
    Return {'items': interpreted_items, ...}, where interpreted_items is a list of:
    {
        'mode': display mode ('markup' | 'contents' | 'image' | 'html', etc.)
        'interpreted': one of
            - rendered string
            - target = (bundle_uuid, genpath)
            - (header = (col1, ..., coln), rows = [{col1:value1, ..., coln:valuen}, ...]) [for tables]
            - {keywords: [...]} for mode = 'search' or 'wsearch'
        'properties': dict of properties (e.g., width, maxlines, etc.),
        'bundle_info': bundle_info or list of bundle_infos,
        'subworksheet_info': subworksheet,
    }
    In addition, return an alignment between the raw items and the interpreted items.
    Each interpreted item has a focusIndex, and possibly consists of a list of
    table rows (indexed by subFocusIndex).  Here is an example:
      --- Raw ---                   --- Interpreted ---
      rawIndex                                         (focusIndex, subFocusIndex)
      0        % display table
      1        [bundle]             [table - row 0     (0, 0)
      2        [bundle]                    - row 1]    (0, 1)
      3
      4        hello                [markup            (1, 0)
      5        world                       ]
      6        [worksheet]          [worksheet]        (2, 0)
      7
    The mapping should be computed as follows:
    - Some raw items contribute directly to a particular interpreted item.
    - Others (blank lines, directives, schema definitions) don't.
    - Those that don't should get mapped to the next interpreted item.
    """
    raw_to_interpreted = []  # rawIndex => (focusIndex, subFocusIndex)

    # Set default schema
    current_schema = None
    default_display = ('table', 'default')
    current_display = default_display
    interpreted_items = []
    bundle_infos = []

    def get_schema(args):  # args is a list of schema names
        args = args if len(args) > 0 else ['default']
        schema = []
        for arg in args:
            # If schema doesn't exist, then treat as item (e.g., uuid).
            schema += schemas.get(arg, canonicalize_schema_items([arg.split(':', 2)]))
        return schema

    def is_missing(info):
        return 'metadata' not in info

    def parse_properties(args):
        properties = {}
        for item in args:
            if '=' not in item:
                raise UsageError('Expected <key>=<value>, but got %s' % item)
            key, value = item.split('=', 1)
            properties[key] = value
        return properties

    def genpath_to_target(bundle_info, genpath):
        # bundle_info, '/stdout' => target = (uuid, 'stdout')
        if not is_file_genpath(genpath):
            raise UsageError('Not file genpath: %s' % genpath)
        # strip off the leading / from genpath to create a subpath in the target.
        return (bundle_info['uuid'], genpath[1:])

    def flush_bundles():
        """
        Having collected bundles in |bundle_infos|, flush them into |interpreted_items|,
        potentially as a single table depending on the mode.
        """
        if len(bundle_infos) == 0:
            return

        def raise_genpath_usage_error():
            raise UsageError('Expected \'% display ' + mode + ' (genpath)\', but got \'% display ' + ' '.join(
                [mode] + args) + '\'')

        # Print out the curent bundles somehow
        mode = current_display[0]
        args = current_display[1:]
        properties = {}
        if mode == 'hidden':
            pass
        elif mode == 'contents' or mode == 'image' or mode == 'html':

            for item_index, bundle_info in bundle_infos:
                if is_missing(bundle_info):
                    interpreted_items.append({
                        'mode': TYPE_MARKUP,
                        'interpreted': 'ERROR: cannot access bundle',
                        'properties': {},
                    })
                    continue

                # Parse arguments
                if len(args) == 0:
                    raise_genpath_usage_error()
                interpreted = genpath_to_target(bundle_info, args[0])
                properties = parse_properties(args[1:])

                interpreted_items.append({
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
            for item_index, bundle_info in bundle_infos:
                header = ('key', 'value')
                rows = []
                for (name, genpath, post) in schema:
                    rows.append({
                        'key': name + ':',
                        'value': apply_func(post, interpret_genpath(bundle_info, genpath))
                    })
                interpreted_items.append({
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
            processed_bundle_infos = []
            for item_index, bundle_info in bundle_infos:
                if 'metadata' in bundle_info:
                    rows.append({
                                    name: apply_func(post, interpret_genpath(bundle_info, genpath))
                                    for (name, genpath, post) in schema
                                    })
                    processed_bundle_infos.append(copy.deepcopy(bundle_info))
                else:
                    # The front-end relies on the name metadata field existing
                    processed_bundle_info = copy.deepcopy(bundle_info)
                    processed_bundle_info['metadata'] = {
                        'name': '<invalid>'
                    }
                    rows.append({
                                    name: apply_func(post, interpret_genpath(processed_bundle_info, genpath))
                                    for (name, genpath, post) in schema
                                    })
                    processed_bundle_infos.append(processed_bundle_info)
            interpreted_items.append({
                'mode': mode,
                'interpreted': (header, rows),
                'properties': properties,
                'bundle_info': processed_bundle_infos
            })
        elif mode == 'graph':
            # display graph <genpath> <properties>
            if len(args) == 0:
                raise_genpath_usage_error()
            # interpreted is list of {
            #   'uuid': ...,
            #   'display_name': ..., # What to show as the description of a bundle
            #   'target': (bundle_uuid, subpath)
            # }
            properties = parse_properties(args[1:])
            interpreted = [{
                'uuid': bundle_info['uuid'],
                'display_name': interpret_genpath(bundle_info, properties.get('display_name', 'name')),
                'target': genpath_to_target(bundle_info, args[0])
            } for item_index, bundle_info in bundle_infos]

            interpreted_items.append({
                'mode': mode,
                'interpreted': interpreted,
                'properties': properties,
                'bundle_info': bundle_infos[0][1]  # Only show the first one for now
                #'bundle_info': [copy.deepcopy(bundle_info) for item_index, bundle_info in bundle_infos]
            })
        else:
            raise UsageError('Unknown display mode: %s' % mode)
        bundle_infos[:] = []  # Clear

    def get_command(value_obj):  # For directives only
        return value_obj[0] if len(value_obj) > 0 else None

    # Go through all the raw items...
    last_was_empty_line = False
    for raw_index, item in enumerate(raw_items):
        new_last_was_empty_line = True
        try:
            (bundle_info, subworksheet_info, value_obj, item_type) = item

            is_bundle = (item_type == TYPE_BUNDLE)
            is_search = (item_type == TYPE_DIRECTIVE and get_command(value_obj) == 'search')
            is_directive = (item_type == TYPE_DIRECTIVE)
            if not is_bundle:
                flush_bundles()
            # Reset display to minimize long distance dependencies of directives
            if not (is_bundle or is_search):
                current_display = default_display
            # Reset schema to minimize long distance dependencies of directives
            if not is_directive:
                current_schema = None

            if item_type == TYPE_BUNDLE:
                raw_to_interpreted.append((len(interpreted_items), len(bundle_infos)))
                bundle_infos.append((raw_index, bundle_info))
            elif item_type == TYPE_WORKSHEET:
                raw_to_interpreted.append((len(interpreted_items), 0))
                interpreted_items.append({
                    'mode': TYPE_WORKSHEET,
                    'interpreted': subworksheet_info,
                    'properties': {},
                    'subworksheet_info': subworksheet_info,
                })
            elif item_type == TYPE_MARKUP:
                new_last_was_empty_line = (value_obj == '')
                if len(interpreted_items) > 0 and interpreted_items[-1]['mode'] == TYPE_MARKUP and \
                   not last_was_empty_line and not new_last_was_empty_line:
                    # Join with previous markup item
                    interpreted_items[-1]['interpreted'] += '\n' + value_obj
                elif not new_last_was_empty_line:
                    interpreted_items.append({
                        'mode': TYPE_MARKUP,
                        'interpreted': value_obj,
                        'properties': {},
                    })
                # Important: set raw_to_interpreted after so we can focus on current item.
                if new_last_was_empty_line:
                    raw_to_interpreted.append(None)
                else:
                    raw_to_interpreted.append((len(interpreted_items) - 1, 0))
            elif item_type == TYPE_DIRECTIVE:
                command = get_command(value_obj)
                if command == '%' or command == '' or command is None:
                    # Comment
                    pass
                elif command == 'schema':
                    # Start defining new schema
                    if len(value_obj) < 2:
                        raise UsageError("`schema` missing name")
                    name = value_obj[1]
                    schemas[name] = current_schema = []
                elif command == 'addschema':
                    # Add to schema
                    if current_schema is None:
                        raise UsageError("`addschema` must be preceded by `schema` directive")
                    if len(value_obj) < 2:
                        raise UsageError("`addschema` missing name")
                    name = value_obj[1]
                    current_schema += schemas[name]
                elif command == 'add':
                    # Add to schema
                    if current_schema is None:
                        raise UsageError("`add` must be preceded by `schema` directive")
                    schema_item = canonicalize_schema_item(value_obj[1:])
                    current_schema.append(schema_item)
                elif command == 'display':
                    # Set display
                    current_display = value_obj[1:]
                elif command == 'search':
                    # Display bundles based on query
                    keywords = value_obj[1:]
                    mode = command
                    data = {'keywords': keywords, 'display': current_display, 'schemas': schemas}
                    interpreted_items.append({
                        'mode': mode,
                        'interpreted': data,
                        'properties': {},
                    })
                elif command == 'wsearch':
                    # Display worksheets based on query
                    keywords = value_obj[1:]
                    mode = command
                    data = {'keywords': keywords}
                    interpreted_items.append({
                        'mode': mode,
                        'interpreted': data,
                        'properties': {},
                    })
                else:
                    raise UsageError("unknown directive `%s`" % command)

                # Only search/wsearch contribute an interpreted item
                if command == 'search' or command == 'wsearch':
                    raw_to_interpreted.append((len(interpreted_items) - 1, 0))
                else:
                    raw_to_interpreted.append(None)
            else:
                raise RuntimeError('Unknown worksheet item type: %s' % item_type)

            # Flush bundles once more at the end
            if raw_index == len(raw_items) - 1:
                flush_bundles()

        except UsageError as e:
            current_schema = None
            bundle_infos[:] = []
            interpreted_items.append({
                'mode': TYPE_MARKUP,
                'interpreted': 'Error on line %d: %s' % (raw_index, e.message),
                'properties': {},
            })
            raw_to_interpreted.append((len(interpreted_items) - 1, 0))

        except StandardError:
            current_schema = None
            bundle_infos[:] = []
            import traceback
            traceback.print_exc()
            interpreted_items.append({
                'mode': TYPE_MARKUP,
                'interpreted': 'Unexpected error while parsing line %d' % raw_index,
                'properties': {},
            })
            raw_to_interpreted.append((len(interpreted_items) - 1, 0))

        finally:
            last_was_empty_line = new_last_was_empty_line

    # TODO: fix inconsistencies resulting from UsageErrors thrown in flush_bundles()
    if len(raw_to_interpreted) != len(raw_items):
        print >>sys.stderr, "WARNING: Length of raw_to_interpreted does not match length of raw_items"

    # Package the result
    interpreted_to_raw = {}
    next_interpreted_index = None
    # Go in reverse order so we can assign raw items that map to None to the next interpreted item
    for raw_index, interpreted_index in reversed(list(enumerate(raw_to_interpreted))):
        if interpreted_index is None:  # e.g., blank line, directive
            interpreted_index = next_interpreted_index
            raw_to_interpreted[raw_index] = interpreted_index
        else:
            interpreted_index_str = str(interpreted_index[0]) + ',' + str(interpreted_index[1])
            if interpreted_index_str not in interpreted_to_raw:  # Bias towards the last item
                interpreted_to_raw[interpreted_index_str] = raw_index
        next_interpreted_index = interpreted_index

    # Return the result
    result = {}
    result['items'] = interpreted_items
    result['raw_to_interpreted'] = raw_to_interpreted
    result['interpreted_to_raw'] = interpreted_to_raw
    return result


def check_worksheet_not_frozen(worksheet):
    if worksheet.frozen:
        raise PermissionError('Cannot mutate frozen worksheet %s(%s).' % (worksheet.uuid, worksheet.name))
