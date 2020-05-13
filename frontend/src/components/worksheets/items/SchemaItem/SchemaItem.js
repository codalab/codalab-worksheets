// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableBody from '@material-ui/core/TableBody';
import TableCell from './SchemaCell';
import TableRow from '@material-ui/core/TableRow';
import { getAfterSortKey } from '../../../../util/worksheet_utils';
import TextField from '@material-ui/core/TextField';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ExpandLessIcon from '@material-ui/icons/ExpandLess';
import IconButton from '@material-ui/core/IconButton';
import * as Mousetrap from '../../../../util/ws_mousetrap_fork';

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
            rowContents: [],
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
            const { maxKey } = getAfterSortKey(prevItem);
            prevItemProcessed = { sort_key: maxKey };
        }

        const schemaItem = this.props.item;
        const schemaHeaders = schemaItem.header;
        console.log(schemaItem);
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
                let rowCells = schemaHeaders.map((headerKey, col) => {
                    let rowContent = rowItem[headerKey];
                    console.log(rowItem[headerKey]);
                    return (
                        <TableCell
                            key={col}
                            onMouseEnter={(e) => this.setState({ hovered: true })}
                            onMouseLeave={(e) => this.setState({ hovered: false })}
                            style={{ paddingLeft: '30px', width: '300px' }}
                            component='th'
                            scope='row'
                        >
                            <TextField
                                id='standard-multiline-static'
                                multiline
                                defaultValue={rowContent || '<none>'}
                            />
                        </TableCell>
                    );
                });
                return (
                    <TableBody>
                        <TableRow>{rowCells}</TableRow>
                    </TableBody>
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
                <div className={`${className}`}>
                    {showSchemasButton}
                    {'Schema: ' + schemaItem.schema_name}
                </div>
                <Table>
                    <TableHead>
                        <TableRow
                            style={{
                                height: 36,
                                borderTop: '2px solid #DEE2E6',
                            }}
                        >
                            {headerHtml}
                        </TableRow>
                    </TableHead>
                    {bodyRowsHtml}
                </Table>
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

export default withStyles(styles)(SchemaItem);
