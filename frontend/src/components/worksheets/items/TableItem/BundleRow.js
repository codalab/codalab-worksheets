import classNames from 'classnames';
import React, { Component } from 'react';
import { withStyles } from '@material-ui/core/styles';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import IconButton from '@material-ui/core/IconButton';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ExpandLessIcon from '@material-ui/icons/ExpandLess';
import Checkbox from '@material-ui/core/Checkbox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import CheckBoxIcon from '@material-ui/icons/CheckBox';
import NewRun from '../../NewRun';

import * as Mousetrap from '../../../../util/ws_mousetrap_fork';
import BundleDetail from '../../BundleDetail';

// The approach taken in this design is to hack the HTML `Table` element by using one `TableBody` for each `BundleRow`.
// We need the various columns to be aligned for all `BundleRow` within a `Table`, therefore using `div` is not an
// option. Instead, we must make use of zero-height rows.

class BundleRow extends Component {
    constructor(props) {
        super(props);
        this.state = {
            showDetail: false,
            showNewUpload: 0,
            showNewRun: 0,
            bundleInfoUpdates: {},
            openDelete: false,
            runProp: {},
            hovered: false,
            uniqueIdentifier: Math.random() * 10000,
        };
    }

    // BULK OPERATION RELATED CODE
    handleCheckboxChange = (event) => {
        this.props.handleCheckBundle(
            this.props.uuid,
            this.state.uniqueIdentifier,
            event.target.checked,
            this.props.refreshCheckBox,
        );
        this.props.childrenCheck(this.props.rowIndex, event.target.checked);
    };

    componentDidMount() {
        if (this.props.checkStatus) {
            this.props.handleCheckBundle(
                this.props.uuid,
                this.state.uniqueIdentifier,
                true,
                this.props.refreshCheckBox,
            );
        }
    }

    componentDidUpdate(prevProp) {
        if (this.props.checkStatus !== prevProp.checkStatus) {
            this.props.handleCheckBundle(
                this.props.uuid,
                this.state.uniqueIdentifier,
                this.props.checkStatus,
                this.props.refreshCheckBox,
            );
        }
        if (this.props.uuid !== prevProp.uuid) {
            this.setState({ showDetail: false });
        }
    }
    // BULK OPERATION RELATED CODE

    receiveBundleInfoUpdates = (update) => {
        let { bundleInfoUpdates } = this.state;
        // Use object spread to update.
        bundleInfoUpdates = { ...bundleInfoUpdates, ...update };
        this.setState({ bundleInfoUpdates });
    };

    handleDetailClick = () => {
        const { showDetail } = this.state;
        this.setState({
            showDetail: !showDetail,
        });
    };

    handleSelectRowClick = () => {
        this.props.updateRowIndex(this.props.rowIndex);
    };

    showNewUpload = (val) => () => {
        this.setState({ showNewUpload: val });
    };

    showNewRun = (val) => () => {
        this.setState({ showNewRun: val });
    };

    rerunItem = (runProp) => {
        this.setState({
            showDetail: false,
            showNewRun: 1,
            runProp: runProp,
        });
    };

    render() {
        const { showDetail, showNewRun, bundleInfoUpdates, runProp } = this.state;
        const {
            classes,
            bundleInfo,
            item,
            reloadWorksheet,
            checkStatus,
            showNewRerun,
            onHideNewRerun,
            editPermission,
            focusIndex,
            ws,
        } = this.props;
        const rowItems = { ...item, ...bundleInfoUpdates };
        var baseUrl = this.props.url;
        var uuid = this.props.uuid;
        var columnWithHyperlinks = this.props.columnWithHyperlinks;
        var worksheetName = this.props.worksheetName;
        var worksheetUrl = this.props.worksheetUrl;
        var rowCells = this.props.headerItems.map((headerKey, col) => {
            var rowContent = rowItems[headerKey];
            // See if there's a link
            var url;
            var showDetailButton;
            var checkBox;
            if (headerKey === 'host_worksheet' && worksheetUrl !== undefined) {
                url = worksheetUrl;
                rowContent = worksheetName;
            } else if (col === 0) {
                url = baseUrl;
                checkBox = (
                    <Checkbox
                        icon={
                            <CheckBoxOutlineBlankIcon
                                color={
                                    this.props.focused || this.state.hovered ? 'action' : 'disabled'
                                }
                                fontSize='small'
                            />
                        }
                        checkedIcon={<CheckBoxIcon fontSize='small' />}
                        onChange={this.handleCheckboxChange}
                        checked={checkStatus || false}
                    />
                );
                showDetailButton = (
                    <IconButton onClick={this.handleDetailClick} style={{ padding: 2 }}>
                        {showDetail ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    </IconButton>
                );
            } else if (columnWithHyperlinks.indexOf(headerKey) !== -1) {
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
                    <a
                        href={url}
                        className='bundle-link'
                        target='_blank'
                        style={{ display: 'inline-block', width: 60 }}
                    >
                        {rowContent}
                    </a>
                );
            if (
                !rowContent ||
                (typeof rowContent === 'object' && !React.isValidElement(rowContent))
            ) {
                rowContent = '';
            }

            return (
                <TableCell
                    key={col}
                    classes={{
                        root: classNames({
                            [classes.rootNoPad]: true,
                            [classes.noCheckBox]: !(editPermission && checkBox),
                            [classes.withCheckBox]: editPermission && checkBox,
                        }),
                    }}
                    onMouseEnter={(e) => this.setState({ hovered: true })}
                    onMouseLeave={(e) => this.setState({ hovered: false })}
                >
                    {editPermission && checkBox}
                    {showDetailButton}
                    {rowContent}
                </TableCell>
            );
        });
        if (this.props.focused) {
            // Use e.preventDefault to avoid openning selected link
            Mousetrap.bind(
                ['enter'],
                (e) => {
                    e.preventDefault();
                    if (!this.props.confirmBundleRowAction(e.code)) {
                        this.setState((state) => ({ showDetail: !state.showDetail }));
                    }
                },
                'keydown',
            );
            Mousetrap.bind(
                ['shift+enter'],
                (e) => {
                    e.preventDefault();
                    window.open(this.props.url, '_blank');
                },
                'keydown',
            );
            Mousetrap.bind(['escape'], () => this.setState({ showDetail: false }), 'keydown');
            Mousetrap.bind(['x'], (e) => {
                if (!editPermission) {
                    return;
                }
                if (!this.props.confirmBundleRowAction(e.code)) {
                    this.props.handleCheckBundle(
                        uuid,
                        this.state.uniqueIdentifier,
                        !this.props.checkStatus,
                        this.props.refreshCheckBox,
                    );
                    this.props.childrenCheck(this.props.rowIndex, !this.props.checkStatus);
                }
            });

            if (
                this.props.focusIndex >= 0 &&
                ws.info.items[this.props.focusIndex].mode === 'table_block'
            ) {
                const isRunBundle = bundleInfo.bundle_type === 'run' && bundleInfo.metadata;
                const isDownloadableRunBundle =
                    bundleInfo.state !== 'preparing' &&
                    bundleInfo.state !== 'starting' &&
                    bundleInfo.state !== 'created' &&
                    bundleInfo.state !== 'staged';
                Mousetrap.bind(['a s'], (e) => {
                    if (!isRunBundle || isDownloadableRunBundle) {
                        const bundleDownloadUrl =
                            '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
                        window.open(bundleDownloadUrl, '_blank');
                    }
                });
            }

            // unbind shortcuts that are active for markdown_block and worksheet_block
            Mousetrap.unbind('i');
        }
        return (
            <TableBody classes={{ root: classes.tableBody }}>
                {/** ---------------------------------------------------------------------------------------------------
                 *  Main Content
                 */}
                <TableRow
                    onClick={this.handleSelectRowClick}
                    className={classNames({
                        [classes.contentRow]: true,
                        [classes.highlight]: this.props.focused,
                        [classes.lowlight]: !this.props.focused && showDetail,
                    })}
                >
                    {rowCells}
                </TableRow>
                {/** ---------------------------------------------------------------------------------------------------
                 *  Bundle Detail (below)
                 */}
                {showDetail && (
                    <TableRow>
                        <TableCell
                            colSpan='100%'
                            classes={{
                                root: classNames({
                                    [classes.rootNoPad]: true,
                                    [classes.bundleDetail]: true,
                                    [classes.highlight]: this.props.focused,
                                    [classes.lowlight]: !this.props.focused,
                                }),
                            }}
                        >
                            <BundleDetail
                                uuid={bundleInfo.uuid}
                                bundleMetadataChanged={this.props.reloadWorksheet}
                                ref='bundleDetail'
                                onUpdate={this.receiveBundleInfoUpdates}
                                onClose={() => {
                                    this.setState({
                                        showDetail: false,
                                    });
                                }}
                                rerunItem={this.rerunItem}
                                isFocused={this.props.focused}
                                focusIndex={focusIndex}
                                showNewRerun={showNewRerun}
                                onHideNewRerun={onHideNewRerun}
                                showDetail={showDetail}
                                handleDetailClick={this.handleDetailClick}
                                editPermission={editPermission}
                            />
                        </TableCell>
                    </TableRow>
                )}
                {/** ---------------------------------------------------------------------------------------------------
                 *  Rerun
                 */}
                {showNewRun === 1 && (
                    <TableRow>
                        <TableCell colSpan='100%' classes={{ root: classes.insertPanel }}>
                            <div className={classes.insertBox}>
                                <NewRun
                                    ws={ws}
                                    onSubmit={() => {
                                        this.setState({ showNewRun: 0, showDetail: false });
                                        onHideNewRerun();
                                    }}
                                    after_sort_key={bundleInfo.sort_key}
                                    reloadWorksheet={reloadWorksheet}
                                    defaultRun={runProp}
                                />
                            </div>
                        </TableCell>
                    </TableRow>
                )}
            </TableBody>
        );
    }
}

const styles = (theme) => ({
    tableBody: {
        '&:hover $rightButtonStripe': {
            display: 'flex',
        },
    },
    rightButtonStripe: {
        display: 'none',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        left: '100%',
        transform: 'translateY(-100%) translateX(-100%)',
    },
    rootNoPad: {
        verticalAlign: 'middle !important',
        border: 'none !important',
        padding: '0px !important',
        wordWrap: 'break-word',
    },
    noCheckBox: {
        maxWidth: 200,
        minWidth: 110,
    },
    withCheckBox: {
        maxWidth: 200,
        minWidth: 130,
    },
    bundleDetail: {
        paddingLeft: `${theme.spacing.largest}px !important`,
        paddingRight: `${theme.spacing.largest}px !important`,
    },
    contentRow: {
        height: 26,
        borderBottom: '2px solid #ddd',
        borderLeft: '3px solid transparent',
        padding: 0,
        '&:hover': {
            boxShadow:
                'inset 1px 0 0 #dadce0, inset -1px 0 0 #dadce0, 0 1px 2px 0 rgba(60,64,67,.3), 0 1px 3px 1px rgba(60,64,67,.15)',
            zIndex: 1,
        },
    },
    checkBox: {
        '&:hover': {
            backgroundColor: '#ddd',
        },
    },
    highlight: {
        backgroundColor: `${theme.color.primary.lightest} !important`,
        borderLeft: '3px solid #1d91c0',
    },
    lowlight: {
        backgroundColor: `${theme.color.grey.light} !important`,
    },
});

export default withStyles(styles)(BundleRow);
