import * as React from 'react';
import _ from 'underscore';

// See codalab.lib.formatting in codalab-worksheets
export function renderDuration(s) {
    // s: number of seconds
    // Return a human-readable string.
    // Example: 100 => "1m40s", 10000 => "2h46m"
    // Checking s == null here will cover two cases: 1) s is undefined 2) s is null

    function _ignoreZeroDuration(duration, unit) {
        // Avoid redundant zero when rendering duration
        // Example: 100y0d => 100y
        return Math.round(duration) === 0 ? '' : Math.round(duration) + unit;
    }

    if (s == null) {
        return '<none>';
    }

    var m = Math.floor(s / 60);
    if (m === 0) return Math.round(s * 10) / 10 + 's';

    s -= m * 60;
    var h = Math.floor(m / 60);
    if (h === 0) return Math.round(m) + 'm' + _ignoreZeroDuration(s, 's');

    m -= h * 60;
    var d = Math.floor(h / 24);
    if (d === 0) return Math.round(h) + 'h' + _ignoreZeroDuration(m, 'm');

    h -= d * 24;
    var y = Math.floor(d / 365);
    if (y === 0) return Math.round(d) + 'd' + _ignoreZeroDuration(h, 'h');

    d -= y * 365;
    return Math.round(y) + 'y' + _ignoreZeroDuration(d, 'd');
}

/**
 * Pad given integer x with leading zeros to produce string with numDigits.
 * @param x           number to convert to string
 * @param numDigits   number of characters desired
 */
export function padInt(x, numDigits) {
    var s = String(Math.round(x));
    var prefix = new Array(numDigits - s.length + 1).join('0');
    return prefix + s;
}

export function addUTCTimeZone(date) {
    // Append 'Z' to convert the time to ISO format (in UTC).
    if (date) {
        date += 'Z';
    }
    return date;
}

export function renderDate(epochSeconds) {
    // epochSeconds: unix timestamp
    // Return a human-readable string.
    var dt = new Date(epochSeconds * 1000);
    var hour = dt.getHours();
    var min = dt.getMinutes();
    var sec = dt.getSeconds();
    return dt.toDateString() + ' ' + padInt(hour, 2) + ':' + padInt(min, 2) + ':' + padInt(sec, 2);
}

export function renderSize(size) {
    // size: number of bytes
    // Return a human-readable string.
    var units = ['', 'k', 'm', 'g', 't'];
    for (var i = 0; i < units.length; i++) {
        var unit = units[i];
        if (size < 100 && size !== Math.floor(size)) return Math.round(size * 10) / 10.0 + unit;
        if (size < 1024) return Math.round(size) + unit;
        size /= 1024.0;
    }
}

export function renderFormat(value, type) {
    switch (type) {
        case 'list':
            return value.join(' ');
        case 'date':
            return renderDate(value);
        case 'size':
            return renderSize(value);
        case 'duration':
            return renderDuration(value);
        case 'bool':
            return String(value);
        default:
            return value;
    }
}

function serializeBool(formatted) {
    switch (formatted) {
        case 'true':
            return true;
        case 'false':
            return false;
        default:
            return Boolean(parseInt(formatted));
    }
}

export function serializeFormat(formatted, type) {
    // Formatted fields like size and duration are validated server-side.
    switch (type) {
        case 'list':
            return formatted.split(/\s*[\s,|]\s*/);
        case 'bool':
            return serializeBool(formatted);
        case 'int':
            return parseInt(formatted);
        case 'float':
            return parseFloat(formatted);
        default:
            return formatted;
    }
}

export function renderPermissions(state) {
    // Render permissions:
    // - state.permission_spec (what user has)
    // - state.group_permissions (what other people have)
    if (!state.permission_spec) return;

    function permissionToClass(permission) {
        var mapping = {
            read: 'ws-permission-read',
            all: 'ws-permission-all',
        };

        if (mapping.hasOwnProperty(permission)) {
            return mapping[permission];
        }

        console.error('Invalid permission:', permission);
        return '';
    }

    function wrapPermissionInColorSpan(permission) {
        return <span className={permissionToClass(permission)}>{permission}</span>;
    }

    return (
        <div>
            &#91;you({wrapPermissionInColorSpan(state.permission_spec)})
            {_.map(state.group_permissions || [], function(perm) {
                return (
                    <span key={perm.group_name}>
                        &nbsp;
                        {perm.group_name}
                        {'('}
                        {wrapPermissionInColorSpan(perm.permission_spec)}
                        {')'}
                    </span>
                );
            })}
            &#93;
        </div>
    );
}

export function shorten_uuid(uuid) {
    return uuid.slice(0, 8);
}

// Whether an interpreted item changed - used in shouldComponentUpdate.
export function worksheetItemPropsChanged(props, nextProps) {
    /*console.log('worksheetItemPropsChanged',
      props.active !== nextProps.active,
      props.focused !== nextProps.focused,
      props.subFocusIndex !== nextProps.subFocusIndex,
      props.version !== nextProps.version);*/
    return (
        props.active !== nextProps.active ||
        props.focused !== nextProps.focused ||
        props.focusIndex !== nextProps.focusIndex ||
        props.ws.info.blocks.length !== nextProps.ws.info.blocks.length ||
        (nextProps.focused && props.subFocusIndex !== nextProps.subFocusIndex) ||
        props.version !== nextProps.version
    );
}

// given an array of arguments, return a shell-safe command
export function buildTerminalCommand(args) {
    var ret = [];
    args.forEach(function(s) {
        if (/[^A-Za-z0-9_s/:=-]/.test(s)) {
            s = "'" + s.replace(/'/g, "'\\''") + "'";
            s = s
                .replace(/^(?:'')+/g, '') // unduplicate single-quote at the beginning
                .replace(/\\'''/g, "\\'"); // remove non-escaped single-quote if there are enclosed between 2 escaped
        }
        ret.push(s);
    });
    return ret.join(' ');
}

export function createAlertText(requestURL, responseText, solution) {
    var alertText = 'request failed: ' + requestURL;
    if (responseText) {
        alertText += '\n\nserver response: ' + responseText;
    }
    if (solution) {
        alertText += '\n\npotential solution: ' + solution;
    }
    return alertText;
}

// the five functions below are used for uplading files on the web. Some of them are the same as some functions on the CLI.
export const ARCHIVE_EXTS = ['.tar.gz', '.tgz', '.tar.bz2', '.zip', '.gz'];
export const NOT_NAME_CHAR_REGEX = /[^a-zA-Z0-9_.-]/gi;
export const BEGIN_NAME_REGEX = /[a-zA-Z_]/gi;

// same as shorten_name in /lib/spec_util.py
export function shortenName(name) {
    if (name.length <= 32) {
        return name;
    } else {
        return name.substring(0, 15) + '..' + name.substring(name.length - 15);
    }
}

// same as path_is_archive in /lib/zip_util.py
export function pathIsArchive(name) {
    for (var i = 0; i < ARCHIVE_EXTS.length; i++) {
        if (name.endsWith(ARCHIVE_EXTS[i])) {
            return true;
        }
    }
    return false;
}

// same as strip_archive_ext in /lib/zip_util.py
export function stripArchiveExt(name) {
    for (var i = 0; i < ARCHIVE_EXTS.length; i++) {
        if (name.endsWith(ARCHIVE_EXTS[i])) {
            return name.substring(0, name.length - ARCHIVE_EXTS[i].length);
        }
    }
    return name;
}

export function getArchiveExt(name) {
    for (var i = 0; i < ARCHIVE_EXTS.length; i++) {
        if (name.endsWith(ARCHIVE_EXTS[i])) {
            return name.substring(name.length - ARCHIVE_EXTS[i].length);
        }
    }
    return '';
}

// same as create_default_name in /lib/spec_util.py
export function createDefaultBundleName(name) {
    name = stripArchiveExt(name);
    name = name.replace(NOT_NAME_CHAR_REGEX, '-');
    name = name.replace(/-+/gi, '-'); // Collapse '---' => '-'
    var beginChar = name.charAt(0);
    if (!beginChar.match(BEGIN_NAME_REGEX)) {
        name = '_' + name;
    }
    name = shortenName(name);
    return name;
}

export function getDefaultBundleMetadata(name, description = '') {
    return {
        data: [
            {
                attributes: {
                    bundle_type: 'dataset',
                    metadata: {
                        description,
                        license: '',
                        name: createDefaultBundleName(name),
                        source_url: '',
                        tags: [],
                    },
                },
                type: 'bundles',
            },
        ],
    };
}

export function createHandleRedirectFn(worksheetUuid) {
    return function(e) {
        e.stopPropagation();
        e.preventDefault();

        window.location.href =
            '/account/login?next=' + encodeURIComponent('/worksheets/' + worksheetUuid);
    };
}

// Return the sort key at index subFocusIndex, if subFocusIndex is defined.
// Otherwise, return the largest sort_key.
export function getAfterSortKey(item, subFocusIndex) {
    // The default after_sort_key is -1 when inserting an item on top of the worksheet,
    // so the item's sort_key should always be >= 0 (sort_key > after_sort_key)
    if (!item) return -1;
    if (item.mode === 'image_block' || item.mode === 'contents_block') {
        // image_block and content_block store the sort_key in a different property than do other blocks
        return item['bundles_spec']['bundle_infos'][0]['sort_key'];
    }
    const sort_keys = item.sort_keys || [];
    if (sort_keys[subFocusIndex] || sort_keys[subFocusIndex] === 0) {
        return sort_keys[subFocusIndex];
    }
    const afterSortKey: number = Math.max(...sort_keys);
    return isFinite(afterSortKey) ? afterSortKey : -1;
}

export function getIds(item) {
    if (item.mode === 'markup_block') {
        return item.ids;
    } else if (item.mode === 'table_block') {
        if (item.bundles_spec && item.bundles_spec.bundle_infos) {
            return item.bundles_spec.bundle_infos.map((info) => info.id);
        }
    }
    return [];
}

/**
 * The way that the backend returns bundle metadata is not particularly
 * conducive to rendering bundle information in the CodaLab UI.
 *
 * Often when we're rendering a bundle field, we want to have certain pieces
 * of information about that field readily available.
 *
 * E.g. we might be interested in the field's name, value, description, type,
 * whether or not its editable, etc.
 *
 * This helper takes in unformatted bundle data and returns an object in which
 * each field name is a key, and each key's value has the following shape:
 *
 * <field_name>: {
 *     name:        <field_name>,
 *     value:       <field_value>,
 *     description: <field_description>,
 *     editable:    <field_is_editable>,
 *     type:        <field_type>,
 *     bundle_uuid: <bundle_uuid>,
 * }
 *
 * @param {object} bundle
 * @returns {object}
 */
export function formatBundle(bundle) {
    if (!bundle) {
        return {};
    }

    const { editableMetadataFields, metadata, metadataDescriptions, metadataType, owner } = bundle;

    // copy nested bundle fields into the top-level of an object
    const mergedBundle = {
        ...bundle,
        ...metadata,
        ...owner,
    };

    // remove the fields that don't need to be in our formatted bundle
    delete mergedBundle.editableMetadataFields;
    delete mergedBundle.metadata;
    delete mergedBundle.metadataDescriptions;
    delete mergedBundle.metadataType;

    // these fields will receive extra formatting below
    const unformattedFields = [
        'created',
        'data_size',
        'on_preemptible_worker',
        'time',
        'time_running',
        'time_preparing',
        'time_cleaning_up',
        'time_uploading_results',
        'time_user',
        'time_system',
        'started',
        'last_updated',
        'cpu_usage',
        'memory_usage',
    ];

    const result = {};
    Object.keys(mergedBundle).forEach((field) => {
        // build our formatted field object
        result[field] = {};
        result[field].name = field;
        result[field].description = metadataDescriptions[field];
        result[field].editable = bundle.permission > 1 && editableMetadataFields.includes(field);
        result[field].bundle_uuid = mergedBundle.uuid;
        result[field].type = metadataType[field];

        // format the fields that need extra formatting
        if (unformattedFields.includes(field)) {
            const value = mergedBundle[field];
            const type = result[field].type;
            result[field].value = renderFormat(value, type);
        } else {
            result[field].value = mergedBundle[field];
        }
    });

    return result;
}
