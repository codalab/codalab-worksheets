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

class SchemaRow extends Component {
    constructor(props) {
        super(props);
        this.state = {};
    }

    render() {
        const { showDetail, showNewRun, bundleInfoUpdates, runProp } = this.state;
        const {
            classes,
            schemaHeaders,
            reloadWorksheet,
            editPermission,
            focusIndex,
            rowItem,
            ws,
        } = this.props;
        var worksheetName = this.props.worksheetName;
        var worksheetUrl = this.props.worksheetUrl;
        var rowCells = schemaHeaders.map((headerKey, col) => {
            console.log(headerKey);
            let rowContent = rowItem[headerKey];
            return (
                <TableCell
                    key={col}
                    onMouseEnter={(e) => this.setState({ hovered: true })}
                    onMouseLeave={(e) => this.setState({ hovered: false })}
                >
                    {/* {editPermission && checkBox}
                    {showDetailButton} */}
                    {rowContent}
                </TableCell>
            );
        });
        // if (this.props.focused) {
        //     // Use e.preventDefault to avoid openning selected link
        //     Mousetrap.bind(
        //         ['enter'],
        //         (e) => {
        //             e.preventDefault();
        //             if (!this.props.confirmBundleRowAction(e.code)) {
        //                 this.setState((state) => ({ showDetail: !state.showDetail }));
        //             }
        //         },
        //         'keydown',
        //     );
        //     Mousetrap.bind(
        //         ['shift+enter'],
        //         (e) => {
        //             e.preventDefault();
        //             window.open(this.props.url, '_blank');
        //         },
        //         'keydown',
        //     );
        //     Mousetrap.bind(['escape'], () => this.setState({ showDetail: false }), 'keydown');
        //     Mousetrap.bind(['x'], (e) => {
        //         if (!editPermission) {
        //             return;
        //         }
        //         if (!this.props.confirmBundleRowAction(e.code)) {
        //             this.props.handleCheckBundle(
        //                 uuid,
        //                 this.state.uniqueIdentifier,
        //                 !this.props.checkStatus,
        //                 this.props.refreshCheckBox,
        //             );
        //             this.props.childrenCheck(this.props.rowIndex, !this.props.checkStatus);
        //         }
        //     });

        //     if (
        //         this.props.focusIndex >= 0 &&
        //         ws.info.items[this.props.focusIndex].mode === 'table_block'
        //     ) {
        //         const isRunBundle = bundleInfo.bundle_type === 'run' && bundleInfo.metadata;
        //         const isDownloadableRunBundle =
        //             bundleInfo.state !== 'preparing' &&
        //             bundleInfo.state !== 'starting' &&
        //             bundleInfo.state !== 'created' &&
        //             bundleInfo.state !== 'staged';
        //         Mousetrap.bind(['a s'], (e) => {
        //             if (!isRunBundle || isDownloadableRunBundle) {
        //                 const bundleDownloadUrl =
        //                     '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
        //                 window.open(bundleDownloadUrl, '_blank');
        //             }
        //         });
        //     }

        //     // unbind shortcuts that are active for markdown_block and worksheet_block
        //     Mousetrap.unbind('i');
        // }
        return (
            <TableBody classes={{ root: classes.tableBody }}>
                {/** ---------------------------------------------------------------------------------------------------
                 *  Main Content
                 */}
                <TableRow
                    className={classNames({
                        [classes.contentRow]: true,
                        [classes.highlight]: this.props.focused,
                        [classes.lowlight]: !this.props.focused && showDetail,
                    })}
                >
                    {rowCells}
                </TableRow>
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

export default withStyles(styles)(SchemaRow);
