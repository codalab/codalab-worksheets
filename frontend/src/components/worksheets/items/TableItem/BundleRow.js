import React, { Component } from 'react';
import classNames from 'classnames';
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
            showDetail: false,
            openDelete: false,
            runProp: {},
            hovered: false,
            uniqueIdentifier: Math.random()*10000,
        };
    }

    // BULK OPERATION RELATED CODE
    handleCheckboxChange = event => {
        this.props.handleCheckBundle(this.props.uuid, this.state.uniqueIdentifier, event.target.checked, this.props.refreshCheckBox);
        this.props.childrenCheck(this.props.rowIndex, event.target.checked);
    };

    componentDidMount(){
        if (this.props.checkStatus){
            this.props.handleCheckBundle(this.props.uuid, this.state.uniqueIdentifier, true, this.props.refreshCheckBox);
        }
    }

    componentDidUpdate(prevProp){
        if (this.props.checkStatus !== prevProp.checkStatus){
            this.props.handleCheckBundle(this.props.uuid, this.state.uniqueIdentifier, this.props.checkStatus,  this.props.refreshCheckBox);
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
    }

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
    }

    render() {
        const {
            showDetail,
            showNewUpload,
            showNewRun,
            bundleInfoUpdates,
            openDelete,
            runProp,
        } = this.state;
        const {
            classes,
            onMouseMove,
            bundleInfo,
            prevBundleInfo,
            item,
            worksheetUUID,
            reloadWorksheet,
            isLast,
            checkStatus,
        } = this.props;
        const rowItems = { ...item, ...bundleInfoUpdates };
        var baseUrl = this.props.url;
        var uuid = this.props.uuid;
        var columnWithHyperlinks = this.props.columnWithHyperlinks;
        var rowCells = this.props.headerItems.map((headerKey, col) => {
            var rowContent = rowItems[headerKey];

            // See if there's a link
            var url;
            var showDetailButton;
            var checkBox;
            if (col === 0) {
                url = baseUrl;
                checkBox = <Checkbox
                                icon={<CheckBoxOutlineBlankIcon color={this.props.focused || this.state.hovered ? 'action' : 'disabled'} fontSize="small" />}
                                checkedIcon={<CheckBoxIcon fontSize="small" />}
                                onChange={this.handleCheckboxChange}
                                checked={checkStatus||false}
                            />
                showDetailButton = 
                        <IconButton onClick={this.handleDetailClick} style={{ padding: 2 }}>
                            {this.state.showDetail?
                            <ExpandLessIcon/>:
                            <ExpandMoreIcon/>}
                        </IconButton>;
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
                    <a href={url} className='bundle-link' target='_blank' style={{ display: 'inline-block', width: 60 }}>
                        {rowContent}
                    </a>
                );
            // else rowContent = rowContent + '';

            return (
                <TableCell
                    key={col}
                    classes={{
                        root: classes.rootNoPad,
                    }}
                    onMouseEnter = {e=>this.setState({hovered: true})}
                    onMouseLeave = {e=>this.setState({hovered: false})}
                >   
                    {checkBox}
                    {showDetailButton}
                    {rowContent}
                </TableCell>
            );
        });

         // Keyboard opening/closing
        if (this.props.focused) {
             // Use e.preventDefault to avoid openning selected link
            Mousetrap.bind(
                ['enter'], 
                (e) => {
                    e.preventDefault();
                    if (!this.props.confirmBundleRowAction(e.code)){
                        this.setState((state) => ({ showDetail: !state.showDetail }))
                    }
                }, 
                'keydown'
            );
            Mousetrap.bind(['escape'], () => this.setState({ showDetail: false }), 'keydown');
            Mousetrap.bind(['x'],
                (e) => {
                    if (!this.props.confirmBundleRowAction(e.code)){
                        this.props.handleCheckBundle(uuid, this.state.uniqueIdentifier, !this.props.checkStatus, this.props.refreshCheckBox);
                        this.props.childrenCheck(this.props.rowIndex, !this.props.checkStatus);
                    }
                }, 'keydown'
            );
            Mousetrap.bind(['space'],
                (e) => {
                    if (!this.props.confirmBundleRowAction(e.code)){
                        e.preventDefault();
                        this.props.handleSelectAllSpaceHit();
                    }
                }, 'keydown'
            );
        }

        return (
            <TableBody
                classes={{ root: classes.tableBody }}
            >
                {/** ---------------------------------------------------------------------------------------------------
                  *  Main Content
                  */}
                <TableRow
                    onClick={this.handleSelectRowClick}
                    onContextMenu={this.props.handleContextMenu.bind(
                        null,
                        bundleInfo.uuid,
                        this.props.focusIndex,
                        this.props.rowIndex,
                        bundleInfo.bundle_type === 'run',
                    )}
                    className={classNames({
                        [classes.contentRow]: true,
                        [classes.highlight]: this.props.focused,
                        [classes.lowlight]: !this.props.focused && this.state.showDetail,
                    })}
                >
                    {rowCells}
                </TableRow>
                {/** ---------------------------------------------------------------------------------------------------
                  *  Bundle Detail (below)
                  */}
                {showDetail && (
                    <TableRow>
                        <TableCell colSpan='100%' classes={{ root: classNames({
                            [classes.rootNoPad]: true,
                            [classes.bundleDetail]: true,
                            [classes.highlight]: this.props.focused,
                            [classes.lowlight]: !this.props.focused,
                        })}}>
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
                                rerunItem={ this.rerunItem }
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
                                    ws={this.props.ws}
                                    onSubmit={() => this.setState({ showNewRun: 0 })}
                                    after_sort_key={bundleInfo.sort_key}
                                    reloadWorksheet={reloadWorksheet}
                                    defaultRun={ runProp }
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
    panelContainer: {
        display: 'block',
        height: '0px !important',
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
        transform: 'translateY(-18px)',
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
        wordWrap: 'break-word',
        maxWidth: 100,
    },
    rootNoPad: {
        verticalAlign: 'middle !important',
        border: 'none !important',
        padding: '0px !important',
        wordWrap: 'break-word',
        maxWidth: 100,
    },
    bundleDetail: {
        paddingLeft: `${theme.spacing.largest}px !important`,
        paddingRight: `${theme.spacing.largest}px !important`,
    },
    iconButtonRoot: {
        backgroundColor: theme.color.grey.lighter,
        padding: "1px 2px",
        marginBottom: 3,
        marginRight: 1,
    },
    buttonRoot: {
        width: 120,
        marginLeft: theme.spacing.unit,
        marginRight: theme.spacing.unit,
        backgroundColor: '#f7f7f7',
        '&:hover': {
            backgroundColor: '#f7f7f7',
        },
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
    contentRow: {
        height: 26,
        borderBottom: '2px solid #ddd',
        borderLeft: '3px solid transparent',
        padding: 0,
        '&:hover': {
            boxShadow:'inset 1px 0 0 #dadce0, inset -1px 0 0 #dadce0, 0 1px 2px 0 rgba(60,64,67,.3), 0 1px 3px 1px rgba(60,64,67,.15)',
            zIndex: 1,
        },
    },
    checkBox:{
        '&:hover': {
            backgroundColor: '#ddd',
        }
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
