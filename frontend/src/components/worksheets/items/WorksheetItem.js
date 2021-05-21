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
        this.rowRefs = {}; // Map of {elementId: ref}
    }

    capture_keys() {
        // Open worksheet in same tab
        Mousetrap.bind(
            ['enter'],
            function() {
                this.props.openWorksheet(this._get_uuid_from_element_id());
            }.bind(this),
            'keydown',
        );

        // Open worksheet in new window/tab
        Mousetrap.bind(
            ['shift+enter'],
            function() {
                const uuid = this._get_uuid_from_element_id();
                // Construct the URI by using the uuid of target worksheet
                const baseURI = document.getElementById(
                    `codalab-worksheet-item-${this.props.focusIndex}-subitem-${this.props.subFocusIndex}`,
                ).attributes[0].baseURI;
                const uriComponents = baseURI.split('/');
                uriComponents[uriComponents.length - 2] = uuid;
                window.open(uriComponents.join('/'), '_blank');
            }.bind(this),
            'keydown',
        );

        // Paste uuid of focused worksheet into console
        Mousetrap.bind(
            ['i'],
            function() {
                $('#command_line')
                    .terminal()
                    .insert(this._get_uuid_from_element_id() + ' ');
            }.bind(this),
            'keydown',
        );

        // unbind shortcuts that are active for table_block and markdown_block
        Mousetrap.unbind('a s');
        Mousetrap.unbind('x');
    }

    updateRowIndex = (rowIndex, open) => {
        if (!open) {
            // Just highlight it
            this.props.setFocus(this.props.focusIndex, rowIndex);
        } else {
            // Actually open this worksheet.
            const uuid = this._get_uuid_from_element_id();
            this.props.openWorksheet(uuid);
        }
    };

    _getItems() {
        var item = this.props.item;
        if (item.mode === 'subworksheets_block') {
            return item.subworksheet_infos;
        } else {
            throw new Error('Invalid: ' + item.mode);
        }
    }

    _get_uuid_from_element_id() {
        const id = `codalab-worksheet-item-${this.props.focusIndex}-subitem-${this.props.subFocusIndex}`;
        return this.rowRefs[id].current.props.uuid;
    }

    shouldComponentUpdate(nextProps, nextState) {
        return worksheetItemPropsChanged(this.props, nextProps);
    }

    render() {
        if (this.props.active && this.props.focused) this.capture_keys();

        var self = this;
        var tableClassName = this.props.focused ? 'table focused' : 'table';
        var items = this._getItems();

        const { focusIndex } = this.props;

        var body_rows_html = items.map(function(row_item, row_index) {
            var row_focused = self.props.focused && row_index === self.props.subFocusIndex;
            var url = '/worksheets/' + row_item.uuid;
            const id = `codalab-worksheet-item-${focusIndex}-subitem-${row_index}`;
            self.rowRefs[id] = React.createRef();
            return (
                <TableWorksheetRow
                    key={row_index}
                    id={id}
                    item={row_item}
                    rowIndex={row_index}
                    focused={row_focused}
                    url={url}
                    uuid={row_item.uuid}
                    updateRowIndex={self.updateRowIndex}
                    ref={self.rowRefs[id]}
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

    handleTextClick = () => {
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
            <tr className={className} id={this.props.id}>
                <td>
                    <div onClick={this.handleRowClick}>
                        <button className='link' onClick={this.handleTextClick}>{`${item.title +
                            ' '}[${item.name}]`}</button>
                    </div>
                </td>
            </tr>
        );
    }
}

export default WorksheetItem;
