// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableBody from '@material-ui/core/TableBody';
import TableCell from './SchemaCell';
import TableRow from '@material-ui/core/TableRow';
import CheckIcon from '@material-ui/icons/Check';
import TextField from '@material-ui/core/TextField';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import ExpandLessIcon from '@material-ui/icons/ExpandLess';
import IconButton from '@material-ui/core/IconButton';
import * as Mousetrap from '../../../../util/ws_mousetrap_fork';
import ArrowDropUpIcon from '@material-ui/icons/ArrowDropUp';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import DeleteSweepIcon from '@material-ui/icons/DeleteSweep';
import AddCircleIcon from '@material-ui/icons/AddCircle';
import EditIcon from '@material-ui/icons/Edit';
import ClearIcon from '@material-ui/icons/Clear';

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
            rows: [...this.props.item.field_rows],
            editing: false,
        };
    }

    toggleEdit = (clear, save) => () => {
        if (clear) {
            this.setState({ rows: this.props.item.field_rows, editing: !this.state.editing });
            return;
        }
        this.setState({ editing: !this.state.editing });
        if (save) {
            let updatedSchema = [];
            this.state.rows.forEach((fields) => {
                if (!fields.field) {
                    return;
                }
                let curRow = '% add ' + fields.field;
                if (!fields['generated-path']) {
                    updatedSchema.push(curRow);
                    return;
                }
                curRow = curRow + ' ' + fields['generated-path'];
                if (!fields['post-processing']) {
                    updatedSchema.push(curRow);
                    return;
                }
                curRow = curRow + ' ' + fields['post-processing'];
                updatedSchema.push(curRow);
            });
            console.log(updatedSchema);
            this.props.updateSchemaItem(
                updatedSchema,
                this.props.item.start_index,
                this.props.item.field_rows.length,
            );
        }
    };

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    addFieldRowAfter = (idx) => (e) => {
        let newRow = {};
        const schemaItem = this.props.item;
        const schemaHeaders = schemaItem.header;
        schemaHeaders.forEach((header) => {
            newRow[header] = '';
        });
        let curRow = this.state.rows;
        curRow.splice(idx + 1, 0, newRow);
        this.setState({ rows: curRow });
    };

    changeFieldValue = (idx, key) => (e) => {
        this.state.rows[idx][key] = e.target.value;
        this.setState({ rows: this.state.rows });
    };

    moveFieldRow = (idx, direction) => () => {
        // -1 for moving up, 1 for moving down
        let newIndex = idx + direction;
        [this.state.rows[newIndex], this.state.rows[idx]] = [
            this.state.rows[idx],
            this.state.rows[newIndex],
        ];
        console.log(this.state.rows);
        this.setState({ rows: this.state.rows });
    };

    removeFieldRow = (idx) => () => {
        console.log(1, this.state.rows);
        this.state.rows.splice(idx, 1);
        console.log(2, this.state.rows);
        this.setState({ rows: this.state.rows }, console.log('Removed', this.state.rows));
    };

    render() {
        const { classes, worksheetUUID, setFocus, prevItem, editPermission } = this.props;
        const { editing } = this.state;
        var className = 'type-markup ' + (this.props.focused ? 'focused' : '');
        const schemaItem = this.props.item;
        // console.log('ITEM: ', schemaItem);
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
                        style={{ padding: '5', fontSize: '16px', maxWidth: '100' }}
                    >
                        {item}
                    </TableCell>
                );
            });
        if (headerHtml) {
            headerHtml.push(
                <TableCell
                    key={headerHtml.length}
                    onMouseEnter={(e) => this.setState({ hovered: true })}
                    onMouseLeave={(e) => this.setState({ hovered: false })}
                    style={{ padding: '5' }}
                    component='th'
                    scope='row'
                >
                    <IconButton disabled={!editing} onClick={this.addFieldRowAfter(-1)}>
                        <AddCircleIcon />
                    </IconButton>
                    {!editing ? (
                        <IconButton onClick={this.toggleEdit(false, false)}>
                            <EditIcon />
                        </IconButton>
                    ) : (
                        <IconButton onClick={this.toggleEdit(false, true)}>
                            <CheckIcon />
                        </IconButton>
                    )}
                    {editing && (
                        <IconButton onClick={this.toggleEdit(true, false)}>
                            <ClearIcon />
                        </IconButton>
                    )}
                </TableCell>,
            );
        }
        bodyRowsHtml =
            this.state.showSchemaDetail &&
            this.state.rows.map((rowItem, ind) => {
                let rowCells = schemaHeaders.map((headerKey, col) => {
                    return (
                        <TableCell
                            key={col}
                            onMouseEnter={(e) => this.setState({ hovered: true })}
                            onMouseLeave={(e) => this.setState({ hovered: false })}
                            style={{ padding: '5' }}
                            component='th'
                            scope='row'
                        >
                            <TextField
                                id='standard-multiline-static'
                                multiline
                                placeholder={'<none>'}
                                value={this.state.rows[ind][headerKey] || ''}
                                disabled={!editing}
                                onChange={this.changeFieldValue(ind, headerKey)}
                            />
                        </TableCell>
                    );
                });
                rowCells.push(
                    <TableCell
                        key={rowCells.length}
                        onMouseEnter={(e) => this.setState({ hovered: true })}
                        onMouseLeave={(e) => this.setState({ hovered: false })}
                        style={{ padding: '5' }}
                        component='th'
                        scope='row'
                    >
                        <IconButton disabled={!editing} onClick={this.addFieldRowAfter(ind)}>
                            <AddCircleIcon />
                        </IconButton>
                        <IconButton disabled={!editing} onClick={this.removeFieldRow(ind)}>
                            <DeleteSweepIcon />
                        </IconButton>
                        <IconButton
                            disabled={!editing || ind === 0}
                            onClick={this.moveFieldRow(ind, -1)}
                        >
                            <ArrowDropUpIcon />
                        </IconButton>
                        <IconButton
                            disabled={!editing || ind === this.state.rows.length - 1}
                            onClick={this.moveFieldRow(ind, 1)}
                        >
                            <ArrowDropDownIcon />
                        </IconButton>
                    </TableCell>,
                );
                return (
                    <TableBody>
                        <TableRow>{rowCells}</TableRow>
                    </TableBody>
                );
            });
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
        console.log('ROWS:', this.state.rows, this.props.item.field_rows);
        return (
            editPermission && (
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
                            <TableRow>{headerHtml}</TableRow>
                        </TableHead>
                        {bodyRowsHtml}
                    </Table>
                </div>
            )
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
