import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Paper from '@material-ui/core/Paper';
import Button from '@material-ui/core/Button';
import IconButton from '@material-ui/core/IconButton';
import DeleteIcon from '@material-ui/icons/Delete';
import MoreIcon from '@material-ui/icons/MoreVert';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
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
        const { classes } = this.props;
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
            // className={columnClasses[index]}
            return (
                <TableCell key={index} component="th" classes={ { root: classes.root } }>
                    {item}
                </TableCell>
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
                <TableRowCustom
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
        // className='type-table table-responsive'
        return (
            <div className='ws-item'>
                <Paper>
                    <Table className={tableClassName}>
                        <TableHead>
                            <TableRow>{headerHtml}</TableRow>
                        </TableHead>
                        <TableBody>{bodyRowsHtml}</TableBody>
                    </Table>
                </Paper>
            </div>
        );
    }
}

////////////////////////////////////////////////////////////

class TableRowCustomBase extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            showButtons: false,
        };
    }

    handleClick = () => {
        this.props.updateRowIndex(this.props.rowIndex);
    };

    render() {
        const { classes } = this.props;
        const { showButtons } = this.state;
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

            //  className={columnClasses[col]}
            return (
                <TableCell key={col} classes={ { root: classes.root } }>
                    {rowContent}
                </TableCell>
            );
        });

        // className={focusedClass}
        return (
            <TableRow
                onClick={this.handleClick}
                onMouseEnter={() => { this.setState({ showButtons: true }) }}
                onMouseLeave={() => { this.setState({ showButtons: false })}}
                onContextMenu={this.props.handleContextMenu.bind(
                    null,
                    this.props.bundleInfo.uuid,
                    this.props.focusIndex,
                    this.props.rowIndex,
                    this.props.bundleInfo.bundle_type === 'run',
                )}
                classes={ { root: classes.row } }
            >
                {rowCells}
                {   showButtons &&
                    <td
                        className={ classes.rightButtonStripe }
                    >
                        <IconButton>
                            <MoreIcon />
                        </IconButton>
                        &nbsp;&nbsp;
                        <IconButton>
                            <DeleteIcon />
                        </IconButton>
                    </td>
                }
                {   showButtons &&
                    <td
                        className={ classes.topButtonStrip }
                    >
                        <Button variant='contained' color='primary' size="small">
                            New Run
                        </Button>
                        &nbsp;&nbsp;
                        <Button variant='contained' color='primary' size="small">
                            Upload Bundle
                        </Button>  
                    </td>
                }
                {   showButtons &&
                    <td
                        className={ classes.bottomButtonStrip }
                    >
                        <Button variant='contained' color='primary' size="small">
                            New Run
                        </Button>
                        &nbsp;&nbsp;
                        <Button variant='contained' color='primary' size="small">
                            Upload Bundle
                        </Button>
                    </td>
                }
            </TableRow>
        );
    }
}

const styles = (theme) => ({
    root: {
        verticalAlign: 'middle !important',
    },
    row: {
        position: 'relative',
    },
    rightButtonStripe: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        right: 0,
        top: 0,
        height: '100%',
    },
    topButtonStrip: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        top: -24,
        left: 'calc(50% - 120px)',
    },
    bottomButtonStrip: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        top: 'calc(100% - 24px)',
        left: 'calc(50% - 120px)',
    },
});

const TableRowCustom = withStyles(styles)(TableRowCustomBase);

export default withStyles(styles)(TableItem);
