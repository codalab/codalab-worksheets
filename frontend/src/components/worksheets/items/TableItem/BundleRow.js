import React, { Component } from 'react';
import $ from 'jquery';
import classNames from 'classnames';
import { withStyles } from '@material-ui/core/styles';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import IconButton from '@material-ui/core/IconButton';
import Button from '@material-ui/core/Button';

import DeleteIcon from '@material-ui/icons/Delete';
import MoreIcon from '@material-ui/icons/MoreVert';
import UploadIcon from '@material-ui/icons/CloudUpload';
import AddIcon from '@material-ui/icons/PlayCircleFilled';

import BundleDetail from '../../BundleDetail';
import NewUpload from '../../NewUpload';
import InsertButtons from './InsertButtons';
import { buildTerminalCommand } from '../../../../util/worksheet_utils';


class InsertButtons extends Component<{
    classes: {},
    showNewUpload: () => void,
    showNewRun: () => void,
}> {
    render() {
        const { classes, showNewUpload, showNewRun } = this.props;
        return (
            <div onMouseMove={ (ev) => { ev.stopPropagation(); } }
                 className={ classes.buttonsPanel }
            >
                <Button
                    key="upload"
                    variant="outlined"
                    size="small"
                    color="primary"
                    aria-label="New Upload"
                    onClick={ () => showNewUpload() }
                    classes={ { root: classes.buttonRoot } }
                >
                    <UploadIcon className={classes.buttonIcon} />
                    Upload
                </Button>
                <Button
                    key="run"
                    variant="outlined"
                    size="small"
                    color="primary"
                    aria-label="New Run"
                    onClick={ () => showNewRun() }
                    classes={ { root: classes.buttonRoot } }
                >
                    <AddIcon className={classes.buttonIcon} />
                    Run
                </Button>
            </div>
        );


    }
}

class BundleRow extends Component {

    state = {
        showDetail: false,
        showUpload: 0,
        showNewRun: 0,
        showInsertButtons: 0,
        bundleInfoUpdates: {},
        showDetail: false,
        showNewRun: false,
        showNewUpload: false,
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

    showNewUpload = (val) => () => {
        this.setState({ showNewUpload: val });
    }

    showNewRun = (val) => () => {
        this.setState({ showNewRun: val })
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
        const { showInsertButtons, showDetail, showUpload, showNewRun, bundleInfoUpdates } = this.state;
        const { classes, onMouseMove, bundleInfo, prevBundleInfo, item, worksheetUUID, reloadWorksheet } = this.props;
        const rowItems = {...item, ...bundleInfoUpdates};
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

        const edgeButtons = [
            // New Upload =============================================
            <Button
                key="upload"
                variant="outlined"
                size="small"
                color="primary"
                aria-label="New Upload"
                onClick={ () => this.setState({ showNewUpload: !showNewUpload }) }
            >
                <UploadIcon className={classes.buttonIcon} />
                Upload
            </Button>,

            // New Run ================================================
            <Button
                key="run"
                variant="outlined"
                size="small"
                color="primary"
                aria-label="New Run"
                onClick={ () => this.setState({ showNewRun: !showNewRun }) }
            >
                <AddIcon className={classes.buttonIcon} />
                Run
            </Button>,
        ];

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
                        <InsertButtons
                            classes={classes}
                            showNewUpload={ this.showNewUpload(-1) }
                            showNewRun={ this.showNewRun(-1) }
                        />
                    }
                </TableCell>
            </TableRow>
            {
                (showUpload === -1) &&
                <TableRow>
                    <TableCell colSpan="100%" classes={ { root: classes.rootNoPad  } } >
                        <NewUpload
                            after_sort_key={ prevBundleInfo ? prevBundleInfo.sort_key : bundleInfo.sort_key - 10 }
                            worksheetUUID={ worksheetUUID }
                            reloadWorksheet={ reloadWorksheet }
                        />
                    </TableCell>
                </TableRow>
            }
            {
                (showNewRun === -1) &&
                <TableRow>
                    <TableCell colSpan="100%" classes={ { root: classes.rootNoPad  } } >
                        <NewRun
                            ws={this.props.ws}
                            onSubmit={() => this.setState({ showNewRun: false })}
                            after_sort_key={ prevBundleInfo ? prevBundleInfo.sort_key : bundleInfo.sort_key - 10 }
                        />
                    </TableCell>
                </TableRow>
            }
            {
                (showDetail || showNewUpload == -1 || showNewRun == -1) &&
                <TableRow className={classes.spacerAbove} />
            }
            <TableRow
                hover
                onClick={this.handleClick}
                onContextMenu={this.props.handleContextMenu.bind(
                    null,
                    bundleInfo.uuid,
                    this.props.focusIndex,
                    this.props.rowIndex,
                    bundleInfo.bundle_type === 'run',
                )}
                className={classNames({
                    [classes.contentRow]: true,
                    [classes.detailPadding]: showDetail,
                })}
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
            {
                (showDetail || showNewUpload == 1 || showNewRun == 1) &&
                <TableRow className={classes.spacerBelow} />
            }
            {
                (showNewUpload === 1) &&
                <TableRow>
                    <TableCell colSpan="100%" classes={ { root: classes.rootNoPad  } } >
                        <NewUpload
                            after_sort_key={ bundleInfo.sort_key }
                            worksheetUUID={ worksheetUUID }
                            reloadWorksheet={ reloadWorksheet }
                            ws={this.props.ws}
                        />
                    </TableCell>
                </TableRow>
            }
            {
                (showNewRun === 1) &&
                <TableRow>
                    <TableCell colSpan="100%" classes={ { root: classes.rootNoPad  } } >
                        <NewRun
                            ws={this.props.ws}
                            onSubmit={() => this.setState({ showNewRun: false })}
                            after_sort_key={ bundleInfo.sort_key }
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
                        <InsertButtons
                            classes={classes}
                            showNewUpload={ this.showNewUpload(1) }
                            showNewRun={ this.showNewRun(1) }
                        />
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
    buttonsPanel: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        width: '100%',
        transform: 'translateY(-50%)',
    },
    buttonRoot: {
        width: 120,
        marginLeft: theme.spacing.unit,
        marginRight: theme.spacing.unit,
        backgroundColor: '#f7f7f7',
        '&:hover': {
            backgroundColor: '#f7f7f7',
        }
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
    contentRow: {
        height: 36,
    },
    spacerAbove: {
        height: theme.spacing.larger,
        borderBottom: `4px solid ${theme.color.grey.dark}`,
    },
    spacerBelow: {
        height: theme.spacing.larger,
        borderTop: `4px solid ${theme.color.grey.dark}`,
    },
});

export default withStyles(styles)(BundleRow);
