import * as React from 'react';
import Immutable from 'seamless-immutable';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';
import $ from 'jquery';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';

class WorksheetItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    capture_keys() {
        // Open worksheet in same tab
        Mousetrap.bind(
            ['enter'],
            function(e) {
                this.props.openWorksheet(this.refs['row' + this.props.subFocusIndex].props.uuid);
            }.bind(this),
            'keydown',
        );

        // Open worksheet in new window/tab
        Mousetrap.bind(
            ['shift+enter'],
            function(e) {
                window.open(this.refs['row' + this.props.subFocusIndex].props.url, '_blank');
            }.bind(this),
            'keydown',
        );

        // Paste uuid of focused worksheet into console
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
    }

    updateRowIndex = (rowIndex, open) => {
        if (!open) {
            // Just highlight it
            this.props.setFocus(this.props.focusIndex, rowIndex);
        } else {
            // Actually open this worksheet.
            var uuid = this.refs['row' + rowIndex].props.uuid;
            this.props.openWorksheet(uuid);
        }
    };

    _getItems() {
        var item = this.props.item;
        if (item.mode == 'subworksheets_block') {
            return item.subworksheet_infos;
        } else {
            throw 'Invalid: ' + item.mode;
        }
    }

    shouldComponentUpdate(nextProps, nextState) {
        return worksheetItemPropsChanged(this.props, nextProps);
    }

    render() {
        if (this.props.active && this.props.focused) this.capture_keys();

        var self = this;
        var tableClassName = this.props.focused ? 'table focused' : 'table';
        var canEdit = this.props.canEdit;
        var items = this._getItems();

        var body_rows_html = items.map(function(row_item, row_index) {
            var row_ref = 'row' + row_index;
            var row_focused = self.props.focused && row_index == self.props.subFocusIndex;
            var url = '/worksheets/' + row_item.uuid;
            return (
                <TableWorksheetRow
                    key={row_index}
                    ref={row_ref}
                    item={row_item}
                    rowIndex={row_index}
                    focused={row_focused}
                    url={url}
                    uuid={row_item.uuid}
                    canEdit={canEdit}
                    updateRowIndex={self.updateRowIndex}
                />
            );
        });
        return (
            <div className='ws-item'>
                <div className='type-table table-responsive'>
                    <table className={tableClassName}>
                        <tbody>{body_rows_html}</tbody>
                    </table>
                </div>
            </div>
        );
    } // end of render function
}

////////////////////////////////////////////////////////////

class TableWorksheetRow extends React.Component {
    constructor(props) {
        super(props);
        this.state = {};
    }

    handleRowClick = () => {
        // Select row
        this.props.updateRowIndex(this.props.rowIndex, false);
    };

    handleTextClick = (event) => {
        var newWindow = true;
        // TODO: same window is broken, so always open in new window
        //var newWindow = event.ctrlKey;
        if (newWindow) {
            // Open in new window
            var item = this.props.item;
            var ws_url = '/worksheets/' + item.uuid;
            window.open(ws_url, '_blank');
        } else {
            // Open in same window
            this.props.updateRowIndex(this.props.rowIndex, true);
        }
    };

    render() {
        var item = this.props.item;
        var className = /*'type-worksheet' + */ this.props.focused ? ' focused' : '';
        return (
            <tr className={className}>
                <td>
                    <div onClick={this.handleRowClick}>
                        <a href='javascript:void(0)' onClick={this.handleTextClick}>
                            {`${item.title + " "}[${item.name}]`}
                        </a>
                    </div>
                </td>
            </tr>
        );
    }
}

export default WorksheetItem;
