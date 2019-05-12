import React, { Component } from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core/styles';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import IconButton from '@material-ui/core/IconButton';
import DeleteIcon from '@material-ui/icons/Delete';
import MoreIcon from '@material-ui/icons/MoreVert';

import BundleDetail from '../../BundleDetail';
import InsertButtons from './InsertButtons';
import { buildTerminalCommand } from '../../../../util/worksheet_utils';

class BundleRow extends Component {

    state = {
        showInsertButtons: 0,
    }

    handleClick = () => {
        this.props.updateRowIndex(this.props.rowIndex);
        const { showDetail } = this.state;
        this.setState({
            showDetail: !showDetail,
        });
    }

    showButtons = (ev) => {
        const row = ev.currentTarget;
        const {
            top,
            height,
        } = row.getBoundingClientRect();
        const { clientY } = ev;
        const onTop = (clientY >= top
                && clientY <= top + 0.25 * height);
        const onBotttom = (clientY >= top + 0.75 * height
                && clientY <= top + height);
        if (onTop) {
            this.setState({
                showInsertButtons: -1,
            });
        }
        if (onBotttom) {
            this.setState({
                showInsertButtons: 1,
            });
        }
    }

    deleteItem = (ev) => {
        ev.stopPropagation();
        const { uuid } = this.props.bundleInfo;
        $('#command_line')
            .terminal()
            .exec(buildTerminalCommand(['rm', uuid]));
    }

    showMore = (ev) => {
        ev.stopPropagation();
    }

    render() {
        const { showInsertButtons, showDetail } = this.state;
        const { bundleInfo, classes, onMouseMove } = this.props;
        var rowItems = this.props.item;
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
                <TableCell
                    key={col}
                    classes={ {
                        root: classes.root
                    } }
                >
                    {rowContent}
                </TableCell>
            );
        });

        return <TableBody
            classes={ { root: classes.tableBody } }
            onMouseMove={ this.showButtons }
            onMouseLeave={ () => {
                this.setState({
                    showInsertButtons: 0,
                });
            } }
        >
            <TableRow classes={ { root: classes.panelContainer } }>
                <TableCell
                    colSpan="100%"
                    classes={ { root: classes.panelCellContainer } }
                >
                    {
                        (showInsertButtons < 0) &&
                        <InsertButtons />
                    }
                </TableCell>
            </TableRow>
            <TableRow
                onClick={this.handleClick}
                onContextMenu={this.props.handleContextMenu.bind(
                    null,
                    bundleInfo.uuid,
                    this.props.focusIndex,
                    this.props.rowIndex,
                    bundleInfo.bundle_type === 'run',
                )}
            >
                { rowCells }
            </TableRow>
            <TableRow classes={ { root: classes.panelContainer } }>
                <TableCell
                    colSpan="100%"
                    classes={ { root: classes.panelCellContainer } }
                >
                    <div
                        className={ classes.rightButtonStripe }
                    >
                        <IconButton
                            onClick={ this.showMore }
                        >
                            <MoreIcon />
                        </IconButton>
                        &nbsp;&nbsp;
                        <IconButton
                            onClick={ this.deleteItem }
                        >
                            <DeleteIcon />
                        </IconButton>
                    </div>
                </TableCell>
            </TableRow>
            {
                showDetail &&
                <TableRow>
                    <TableCell colspan="100%" classes={ { root: classes.rootNoPad  } } >
                        <BundleDetail
                            uuid={ bundleInfo.uuid }
                            bundleMetadataChanged={ this.props.reloadWorksheet }
                            ref='bundleDetail'
                            onClose={ () => {
                                this.setState({
                                    showDetail: false,
                                });
                            } }
                        />
                    </TableCell>
                </TableRow>
            }
            <TableRow classes={ { root: classes.panelContainer } }>
                <TableCell
                    colSpan="100%"
                    classes={ { root: classes.panelCellContainer } }
                >
                    {
                        (showInsertButtons > 0) &&
                        <InsertButtons />
                    }
                </TableCell>
            </TableRow>
        </TableBody>        
    }
}

const styles = (theme) => ({
    tableBody: {
        '&:hover $rightButtonStripe': {
            display: 'flex',
        },
    },
    panelContainer: {
        display: 'block',
        height: '0px !important',
        padding: 0,
        margin: 0,
        border: 'none !important',
        overflow: 'visible',
    },
    panelCellContainer: {
        padding: 0,
        padding: 0,
        margin: 0,
        border: 'none !important',
        overflow: 'visible',
    },
    buttonsPanel: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        width: '100%',
        transform: 'translateY(-50%)',
    },
    rightButtonStripe: {
        display: 'none',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        left: '100%',
        transform: 'translateY(-100%) translateX(-100%)',
    },
    root: {
        verticalAlign: 'middle !important',
        border: 'none !important',
    },
    rootNoPad: {
        verticalAlign: 'middle !important',
        border: 'none !important',
        padding: '0px !important',
    }
});

export default withStyles(styles)(BundleRow);
