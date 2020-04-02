// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableCell from './SchemaCell';
import TableRow from '@material-ui/core/TableRow';
import { getMinMaxKeys } from '../../../../util/worksheet_utils';
import SchemaRow from './SchemaRow';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ExpandLessIcon from '@material-ui/icons/ExpandLess';
import IconButton from '@material-ui/core/IconButton';
import Checkbox from '@material-ui/core/Checkbox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import SvgIcon from '@material-ui/core/SvgIcon';
import CheckBoxIcon from '@material-ui/icons/CheckBox';
import * as Mousetrap from '../../../../util/ws_mousetrap_fork';
import classNames from 'classnames';
import Paper from '@material-ui/core/Paper';

class SchemaItem extends React.Component<{
    worksheetUUID: string,
    item: {},
    reloadWorksheet: () => any,
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            yposition: -1,
            rowcenter: -1,
            rowIdx: -1,
            insertBefore: -1,
            hovered: false,
            showSchemaDetail: false,
        };
    }

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    render() {
        const { classes, worksheetUUID, setFocus, prevItem, editPermission } = this.props;
        var className = 'type-markup ' + (this.props.focused ? 'focused' : '');

        let prevItemProcessed = null;
        if (prevItem) {
            const { maxKey } = getMinMaxKeys(prevItem);
            prevItemProcessed = { sort_key: maxKey };
        }

        const schemaItem = this.props.item;
        const schemaHeaders = schemaItem.header;
        let headerHtml, bodyRowsHtml;
        headerHtml =
            this.state.showSchemaDetail &&
            schemaHeaders.map((item, index) => {
                return (
                    <TableCell
                        onMouseEnter={(e) => this.setState({ hovered: true })}
                        onMouseLeave={(e) => this.setState({ hovered: false })}
                        component='th'
                        key={index}
                        style={{ paddingLeft: '30px' }}
                    >
                        {item}
                    </TableCell>
                );
            });

        bodyRowsHtml =
            this.state.showSchemaDetail &&
            schemaItem.field_rows.map((rowItem, rowIndex) => {
                let rowRef = 'row' + rowIndex;
                let rowFocused = this.props.focused && rowIndex === this.props.subFocusIndex;
                return (
                    <SchemaRow
                        key={rowIndex}
                        ref={rowRef}
                        worksheetUUID={worksheetUUID}
                        item={rowItem}
                        rowIndex={rowIndex}
                        focused={rowFocused}
                        focusIndex={this.props.focusIndex}
                        setFocus={setFocus}
                        rowItem={rowItem}
                        schemaHeaders={schemaHeaders}
                        updateRowIndex={this.updateRowIndex}
                        reloadWorksheet={this.props.reloadWorksheet}
                        ws={this.props.ws}
                        isLast={rowIndex === schemaItem.field_rows.length - 1}
                        editPermission={editPermission}
                    />
                );
            });
        // bodyRowsHtml = this.state.showSchemaDetail &&

        if (this.props.focused) {
            // Use e.preventDefault to avoid openning selected link
            Mousetrap.bind(
                ['enter'],
                (e) => {
                    e.preventDefault();
                    this.setState((state) => ({ showSchemaDetail: !state.showSchemaDetail }));
                },
                'keydown',
            );
        }
        let showSchemasButton = (
            <IconButton
                onClick={() => this.setState({ showSchemaDetail: !this.state.showSchemaDetail })}
                style={{ padding: 2 }}
            >
                {this.state.showSchemaDetail ? <ExpandLessIcon /> : <ExpandMoreIcon />}
            </IconButton>
        );

        var className = 'type-markup ' + (this.props.focused ? 'focused' : '');

        return (
            <div
                className='ws-item'
                onClick={() => {
                    this.props.setFocus(this.props.focusIndex, 0);
                }}
            >
                <div
                    // className={classNames({
                    //     [classes.highlight]: this.props.focused && this.props.subFocusIndex === 0,
                    // })}
                    className={`${className}`}
                >
                    {showSchemasButton}
                    {'Schema: ' + schemaItem.schema_name}
                </div>
                {/* <TableContainer component={Paper} style={{ overflowX: 'auto'}}> */}
                <Table>
                    <TableHead>
                        <TableRow
                            style={{
                                height: 36,
                                borderTop: '2px solid #DEE2E6',
                                // backgroundColor: '#F8F9FA',
                            }}
                        >
                            {headerHtml}
                        </TableRow>
                    </TableHead>
                    {bodyRowsHtml}
                </Table>
                {/* </TableContainer> */}
            </div>
        );
    }
}

class _TableContainer extends React.Component {
    render() {
        const { classes, children, ...others } = this.props;
        return (
            <div className={classes.tableContainer} {...others}>
                {children}
            </div>
        );
    }
}

const styles = (theme) => ({
    tableContainer: {
        position: 'relative',
        backgroundColor: 'white',
        borderColor: '3px solid black',
    },
});

// const TableContainer = withStyles(styles)(_TableContainer);

export default withStyles(styles)(SchemaItem);
