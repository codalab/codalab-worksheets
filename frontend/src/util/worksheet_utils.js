import * as React from 'react';
import $ from 'jquery';
import _ from 'underscore';

// See codalab.lib.formatting in codalab-worksheets
export function renderDuration(s) {
    // s: number of seconds
    // Return a human-readable string.
    // Example: 100 => "1m40s", 10000 => "2h46m"
    if (s == null) {
        return '<none>';
    }

    var m = Math.floor(s / 60);
    if (m === 0) return Math.round(s * 10) / 10 + 's';

    s -= m * 60;
    var h = Math.floor(m / 60);
    if (h === 0) return Math.round(m) + 'm' + Math.round(s) + 's';

    m -= h * 60;
    var d = Math.floor(h / 24);
    if (d === 0) return Math.round(h) + 'h' + Math.round(m) + 'm';

    h -= d * 24;
    var y = Math.floor(d / 365);
    if (y === 0) return Math.round(d) + 'd' + Math.round(h) + 'h';

    d -= y * 365;
    return Math.round(y) + 'y' + Math.round(d) + 'd';
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
            return value.join(' | ');
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
            &nbsp;&#91;you({wrapPermissionInColorSpan(state.permission_spec)})
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

export function keepPosInView(pos) {
    var navbarHeight = parseInt($('body').css('padding-top'));
    const worksheetContainerEl = $('#worksheet_container');
    var viewportHeight = Math.max(worksheetContainerEl.innerHeight() || 0);

    // How far is the pos from top of viewport?
    var distanceFromTopViewPort = pos - navbarHeight;

    if (distanceFromTopViewPort < 100 || distanceFromTopViewPort > viewportHeight * 0.8) {
        // If pos is off the top of the screen or it is more than 80% down the screen,
        // then scroll so that it is at 50% down the screen.
        // Where is the top of the element on the page and does it fit in the
        // the upper half of the page?
        var scrollTo =
            worksheetContainerEl.scrollTop() + distanceFromTopViewPort - viewportHeight * 0.5;
        worksheetContainerEl.stop(true).animate({ scrollTop: scrollTo }, 50);
    }
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
        props.ws.info.items.length !== nextProps.ws.info.items.length ||
        (nextProps.focused && props.subFocusIndex !== nextProps.subFocusIndex) ||
        props.version !== nextProps.version
    );
}

// given an array of arguments, return a shell-safe command
export function buildTerminalCommand(args) {
    var ret = [];
    args.forEach(function(s) {
        if (/[^A-Za-z0-9_\/:=-]/.test(s)) {
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
export const NOT_NAME_CHAR_REGEX = /[^a-zA-Z0-9_\.\-]/gi;
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
    name = name.replace(/\-+/gi, '-'); // Collapse '---' => '-'
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

export function getMinMaxKeys(item) {
    if (!item) {
        return { minKey: null, maxKey: null };
    }
    let minKey = null;
    let maxKey = null;
    if (item.mode === 'markup_block') {
        if (item.sort_keys && item.sort_keys.length > 0) {
            const { sort_keys, ids } = item;
            const keys = [];
            sort_keys.forEach((k, idx) => {
                const key = k || ids[idx];
                if (key !== null && key !== undefined) {
                    keys.push(key);
                }
            });
            if (keys.length > 0) {
                minKey = Math.min(...keys);
                maxKey = Math.max(...keys);
            }
        }
    } else if (item.mode === 'table_block') {
        if (item.bundles_spec && item.bundles_spec.bundle_infos) {
            const keys = [];
            item.bundles_spec.bundle_infos.forEach((info) => {
                const key = info.sort_key || info.id;
                if (key !== null && key !== undefined) {
                    keys.push(key);
                }
            });
            if (keys.length > 0) {
                minKey = Math.min(...keys);
                maxKey = Math.max(...keys);
            }
        }
    }
    return { minKey, maxKey };
}
