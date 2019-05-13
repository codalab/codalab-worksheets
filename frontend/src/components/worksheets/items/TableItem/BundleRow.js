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
        bundleInfoUpdates: {},
    }

    receiveBundleInfoUpdates = (update) => {
        let { bundleInfoUpdates } = this.state;
        // Use object spread to update.
        bundleInfoUpdates = {...bundleInfoUpdates, ...update};
        this.setState({ bundleInfoUpdates });
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
        const { showInsertButtons, showDetail, bundleInfoUpdates } = this.state;
        const { classes, onMouseMove } = this.props;
        const bundleInfo = { ...this.props.bundleInfo, ...bundleInfoUpdates };
        const rowItems = {...this.props.item, ...bundleInfoUpdates};
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
                            classes={ { root: classes.iconButtonRoot } }
                        >
                            <MoreIcon />
                        </IconButton>
                        &nbsp;&nbsp;
                        <IconButton
                            onClick={ this.deleteItem }
                            classes={ { root: classes.iconButtonRoot } }
                        >
                            <DeleteIcon />
                        </IconButton>
                    </div>
                </TableCell>
            </TableRow>
            {
                showDetail &&
                <TableRow>
                    <TableCell colSpan="100%" classes={ { root: classes.rootNoPad  } } >
                        <BundleDetail
                            uuid={ bundleInfo.uuid }
                            bundleMetadataChanged={ this.props.reloadWorksheet }
                            ref='bundleDetail'
                            onUpdate={ this.receiveBundleInfoUpdates }
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
        border: 'none !important',
        overflow: 'visible',
    },
    panelCellContainer: {
        padding: '0 !important',
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
    '@keyframes expandY': {
        from: { transform: 'scaleY(0.5)' },
        to: { transform: 'scaleY(1.0)' },
    },
    rootNoPad: {
        verticalAlign: 'middle !important',
        border: 'none !important',
        padding: '0px !important',
        animationName: 'expandY',
        animationDuration: '0.6s',
        transformOrigin: 'top',
    },
    iconButtonRoot: {
        backgroundColor: theme.color.grey.light,
    },
});

export default withStyles(styles)(BundleRow);
