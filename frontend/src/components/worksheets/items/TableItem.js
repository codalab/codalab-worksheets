import * as React from 'react';
import Immutable from 'seamless-immutable';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';
import $ from 'jquery';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';

class TableItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    capture_keys() {
        // Open worksheet in new window/tab
        Mousetrap.bind(
            ['enter'],
            function(e) {
                window.open(this.refs['row' + this.props.subFocusIndex].props.url, '_blank');
            }.bind(this),
            'keydown',
        );

        // Paste uuid of focused bundle into console
        Mousetrap.bind(
            ['u'],
            function(e) {
                var uuid = this.refs['row' + this.props.subFocusIndex].props.uuid;
                $('#command_line')
                    .terminal()
                    .insert(uuid + ' ');
                //this.props.focusActionBar();
            }.bind(this),
            'keydown',
        );

        // Paste args of focused bundle into console
        Mousetrap.bind(
            ['a'],
            function(e) {
                var bundleInfo = this.refs['row' + this.props.subFocusIndex].props.bundleInfo;
                if (bundleInfo.args != null) {
                    $('#command_line')
                        .terminal()
                        .insert(bundleInfo.args);
                    e.preventDefault();
                    this.props.focusActionBar();
                }
            }.bind(this),
            'keydown',
        );
    }

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    shouldComponentUpdate(nextProps, nextState) {
        return worksheetItemPropsChanged(this.props, nextProps);
    }

    render() {
        if (this.props.active && this.props.focused) this.capture_keys();

        var self = this;
        var tableClassName = this.props.focused ? 'table focused' : 'table';
        var item = this.props.item;
        var canEdit = this.props.canEdit;
        var bundleInfos = item.bundles_spec.bundle_infos;
        var headerItems = item.header;
        var columnClasses = headerItems.map(function(item, index) {
            return (
                'table-column-' +
                encodeURIComponent(item)
                    .replace('%', '_')
                    .replace(/[^-_A-Za-z0-9]/g, '_')
            );
        });
        var headerHtml = headerItems.map(function(item, index) {
            return (
                <th key={index} className={columnClasses[index]}>
                    {item}
                </th>
            );
        });
        var rowItems = item.rows; // Array of {header: value, ...} objects
        var columnWithHyperlinks = [];
        Object.keys(rowItems[0]).forEach(function(x) {
            if (rowItems[0][x] && rowItems[0][x]['path']) columnWithHyperlinks.push(x);
        });
        var bodyRowsHtml = rowItems.map(function(rowItem, rowIndex) {
            var rowRef = 'row' + rowIndex;
            var rowFocused = self.props.focused && rowIndex == self.props.subFocusIndex;
            var url = '/bundles/' + bundleInfos[rowIndex].uuid;
            return (
                <TableRow
                    key={rowIndex}
                    ref={rowRef}
                    item={rowItem}
                    rowIndex={rowIndex}
                    focused={rowFocused}
                    focusIndex={self.props.focusIndex}
                    url={url}
                    bundleInfo={bundleInfos[rowIndex]}
                    uuid={bundleInfos[rowIndex].uuid}
                    headerItems={headerItems}
                    columnClasses={columnClasses}
                    canEdit={canEdit}
                    updateRowIndex={self.updateRowIndex}
                    columnWithHyperlinks={columnWithHyperlinks}
                    handleContextMenu={self.props.handleContextMenu}
                />
            );
        });
        return (
            <div className='ws-item'>
                <div className='type-table table-responsive'>
                    <table className={tableClassName}>
                        <thead>
                            <tr>{headerHtml}</tr>
                        </thead>
                        <tbody>{bodyRowsHtml}</tbody>
                    </table>
                </div>
            </div>
        );
    }
}

////////////////////////////////////////////////////////////

class TableRow extends React.Component {
    handleClick = () => {
        this.props.updateRowIndex(this.props.rowIndex);
    };

    render() {
        var focusedClass = this.props.focused ? 'focused' : '';
        var rowItems = this.props.item;
        var columnClasses = this.props.columnClasses;
        var baseUrl = this.props.url;
        var uuid = this.props.uuid;
        var columnWithHyperlinks = this.props.columnWithHyperlinks;
        var rowCells = this.props.headerItems.map(function(headerKey, col) {
            var rowContent = rowItems[headerKey];

            // See if there's a link
            var url;
            if (col == 0) {
                url = baseUrl;
            } else if (columnWithHyperlinks.indexOf(headerKey) != -1) {
                url = '/rest/bundles/' + uuid + '/contents/blob' + rowContent['path'];
                if ('text' in rowContent) {
                    rowContent = rowContent['text'];
                } else {
                    // In case text doesn't exist, content will default to basename of the path
                    // indexing 1 here since the path always starts with '/'
                    rowContent = rowContent['path'].split('/')[1];
                }
            }
            if (url)
                rowContent = (
                    <a href={url} className='bundle-link' target='_blank'>
                        {rowContent}
                    </a>
                );
            else rowContent = rowContent + '';

            return (
                <td key={col} className={columnClasses[col]}>
                    {rowContent}
                </td>
            );
        });

        return (
            <tr
                className={focusedClass}
                onClick={this.handleClick}
                onContextMenu={this.props.handleContextMenu.bind(
                    null,
                    this.props.bundleInfo.uuid,
                    this.props.focusIndex,
                    this.props.rowIndex,
                    this.props.bundleInfo.bundle_type === 'run',
                )}
            >
                {rowCells}
            </tr>
        );
    }
}

export default TableItem;
