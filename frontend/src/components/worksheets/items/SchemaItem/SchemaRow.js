import classNames from 'classnames';
import React, { Component } from 'react';
import { withStyles } from '@material-ui/core/styles';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import { SchemaEditableField } from '../../../EditableField';
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
                    style={{ paddingLeft: '30px', width: '300px' }}
                    component='th'
                    scope='row'
                >
                    {/* {editPermission && checkBox}
                    {showDetailButton} */}
                    <SchemaEditableField
                        canEdit={true}
                        fieldName={headerKey}
                        value={rowContent}

                        // onChange={bundleMetadataChanged}
                    />
                </TableCell>
            );
        });

        return (
            <TableBody classes={{ root: classes.tableBody }}>
                {/** ---------------------------------------------------------------------------------------------------
                 *  Main Content
                 */}
                <TableRow
                    className={classNames({
                        // [classes.contentRow]: true,
                        // [classes.lowlight]: true,
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
        width: '200px',
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
