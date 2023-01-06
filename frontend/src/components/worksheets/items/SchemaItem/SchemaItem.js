// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Button from '@material-ui/core/Button';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import SaveIcon from '@material-ui/icons/Save';
import TextField from '@material-ui/core/TextField';
import ArrowDropDownRoundedIcon from '@material-ui/icons/ArrowDropDownRounded';
import IconButton from '@material-ui/core/IconButton';
import * as Mousetrap from '../../../../util/ws_mousetrap_fork';
import ArrowDropUpIcon from '@material-ui/icons/ArrowDropUp';
import ArrowDropDownIcon from '@material-ui/icons/ArrowDropDown';
import DeleteSweepIcon from '@material-ui/icons/DeleteSweep';
import AddCircleIcon from '@material-ui/icons/AddCircle';
import RestoreIcon from '@material-ui/icons/Restore';
import classNames from 'classnames';
import ViewListIcon from '@material-ui/icons/ViewList';
import ArrowRightRoundedIcon from '@material-ui/icons/ArrowRightRounded';
import Tooltip from '@material-ui/core/Tooltip';
import HelpOutlineOutlinedIcon from '@material-ui/icons/HelpOutlineOutlined';
import { getAfterSortKey } from '../../../../util/worksheet_utils';
import DeleteForeverIcon from '@material-ui/icons/DeleteForever';
import Grid from '@material-ui/core/Grid';
import { DEFAULT_POST_PROCESSOR } from '../../../../constants';

class SchemaItem extends React.Component<{
    worksheetUUID: string,
    item: {},
    reloadWorksheet: () => any,
}> {
    constructor(props) {
        super(props);
        this.state = {
            showSchemaDetail: false,
            rows: [...this.props.item.field_rows],
            curSchemaName: this.props.item.schema_name,
            newAddedRow: -1,
            deleting: false,
            missingSchemaName: false,
        };
    }

    toggleEdit = (clear, save) => () => {
        if (!this.state.curSchemaName) {
            this.setState({
                missingSchemaName: true,
            });
            return;
        }
        if (!this.props.editPermission) return;
        if (clear) {
            this.clearChanges();
            return;
        }
        if (save) {
            this.saveSchema();
        }
    };

    clearChanges = () => {
        this.setState({
            rows: [...this.props.item.field_rows],
            curSchemaName: this.props.item.schema_name,
            newAddedRow: -1,
        });
    };

    saveSchema = () => {
        const { schema_name, ids } = this.props.item;
        let updatedSchema = ['schema ' + this.state.curSchemaName];
        let fromAddSchema = false;
        // Note: When using the add-item end point,
        // we do not need to add % before the actual directive for the items
        this.state.rows.forEach((fields) => {
            if (!fields['field']) {
                return;
            }
            if (!fromAddSchema && fields.from_schema_name !== schema_name) {
                // these rows correspond to addschema
                fromAddSchema = true;
                updatedSchema.push('addschema ' + fields.from_schema_name);
                return;
            } else if (fromAddSchema && fields.from_schema_name !== schema_name) {
                // These rows doesn't occupy any source lines
                return;
            } else {
                fromAddSchema = false;
            }

            let curRow = "add '" + fields['field'] + "'";
            if (!fields['generalized-path']) {
                updatedSchema.push(curRow);
                return;
            }
            curRow = curRow + " '" + fields['generalized-path'] + "'";
            if (!fields['post-processor']) {
                updatedSchema.push(curRow);
                return;
            }
            curRow = curRow + " '" + fields['post-processor'] + "'";
            updatedSchema.push(curRow);
        });
        // TODO: Comparing with TextEditorItem, unclear why the after sort key is wrong here, but if we don't -1
        //       we will move one line below the desired one
        this.props.updateSchemaItem(
            updatedSchema,
            ids,
            getAfterSortKey(this.props.item) - 1 >= 0
                ? getAfterSortKey(this.props.item) - 1
                : this.props.after_sort_key,
            this.props.create,
            false,
        );
        if (this.props.create) this.props.onSubmit();
    };

    addFieldRowAfter = (idx) => (e) => {
        if (!this.props.editPermission) return;
        const schemaItem = this.props.item;
        const schemaHeaders = schemaItem.header;
        let newRow = { from_schema_name: schemaItem.schema_name };
        schemaHeaders.forEach((header) => {
            // Set post-processor to null for new, unedited rows to indicate that the row can be autofilled.
            newRow[header] = header === 'post-processor' ? null : '';
        });
        let curRow = [...this.state.rows];
        curRow.splice(idx + 1, 0, newRow);
        this.setState({ rows: curRow, newAddedRow: idx + 1 });
    };

    changeFieldValue = (idx, key) => (e) => {
        if (!this.props.editPermission) return;
        const { rows } = this.state;
        let copiedRows = [...rows];
        // replace new line with space, remove single quotes since we use that to quote fields with space when saving
        copiedRows.splice(
            idx,
            1,
            this.autofillTypes({
                ...rows[idx],
                [key]: e.target.value.replace(/\n/g, ' ').replace("'", ''),
            }),
        );
        this.setState({ rows: [...copiedRows] });
    };

    autofillTypes = (row) => {
        // Only autofill if the post-processor is null.
        // Null means the post-processor has never been manually changed or autofilled.
        if (row['post-processor'] !== null) {
            return row;
        }

        row['post-processor'] =
            DEFAULT_POST_PROCESSOR[row['generalized-path'].toLowerCase()] ?? row['post-processor'];

        return row;
    };

    changeSchemaName = (e) => {
        if (!this.props.editPermission) return;
        // replace new line with space, remove single quotes since we use that to quote fields with space when saving
        this.setState({
            curSchemaName: e.target.value
                .replace(/\n/g, ' ')
                .replace("'", '')
                .replace(' ', ''),
            missingSchemaName: false,
        });
    };

    checkIfTextChanged = () => {
        // checks whether any of the textfields in the rows changed compared with the original values
        let originalRows = this.props.item.field_rows;
        if (this.state.rows.length !== originalRows.length) return true;
        const headerKeys = this.props.item.header;
        for (let ind = 0; ind < originalRows.length; ind++) {
            for (let keyInd = 0; keyInd < headerKeys.length; keyInd++) {
                const key = headerKeys[keyInd];
                if (originalRows[ind][key] !== this.state.rows[ind][key]) {
                    return true;
                }
            }
        }
        return false;
    };

    moveFieldRow = (idx, direction) => () => {
        if (!this.props.editPermission) return;
        // -1 for moving up, 1 for moving down
        const { rows } = this.state;
        let copiedRows = [...rows];
        let newIndex = idx + direction;
        [copiedRows[newIndex], copiedRows[idx]] = [copiedRows[idx], copiedRows[newIndex]];
        if (copiedRows[idx].from_schema_name !== this.props.item.schema_name) {
            // if the last row we switched with was generated by addschema
            // we should check and keep switching
            // until we meet a non-addschema row or top/end of table
            idx += direction;
            newIndex += direction;
            while (
                newIndex >= 0 &&
                newIndex < rows.length &&
                rows[newIndex].from_schema_name !== this.props.item.schema_name
            ) {
                [copiedRows[newIndex], copiedRows[idx]] = [copiedRows[idx], copiedRows[newIndex]];
                idx += direction;
                newIndex += direction;
            }
        }
        this.setState({ rows: copiedRows });
    };

    removeFieldRow = (idx) => () => {
        if (!this.props.editPermission) return;
        const { rows } = this.state;
        let copiedRows = [...rows];
        copiedRows.splice(idx, 1);
        this.setState({ rows: copiedRows });
    };

    deleteThisSchema = () => {
        this.setState({ showSchemaDetail: false, deleting: false });
        this.props.updateSchemaItem([], this.props.item.ids, null, false, true);
        Mousetrap.unbind(['backspace', 'del']);
    };

    getSchemaItemId = () => {
        return document.getElementById(
            'codalab-worksheet-item-' +
                this.props.focusIndex +
                '-subitem-' +
                this.props.subFocusIndex,
        )
            ? 'codalab-worksheet-item-' +
                  this.props.focusIndex +
                  '-subitem-' +
                  this.props.subFocusIndex +
                  '-schema'
            : 'codalab-worksheet-item-' + this.props.focusIndex + '-schema';
    };

    componentDidUpdate(prevProps, prevState) {
        if (this.state.newAddedRow !== -1 && this.state.rows.length === prevState.rows.length + 1) {
            document.getElementById('textbox-' + this.state.newAddedRow + '-0').focus();
        }
        if (this.props.focused) this.capture_keys();
    }

    // Scroll the newly opened schema editor into view
    componentDidMount() {
        if (this.props.create) {
            const node = document.getElementById(this.getSchemaItemId());
            if (node) {
                node.scrollIntoView({ block: 'start', behavior: 'smooth' });
            }
        }
    }

    capture_keys = () => {
        // Delete the schema
        Mousetrap.bind(
            ['backspace', 'del'],
            function(ev) {
                ev.preventDefault();
                this.setState({ deleting: true });
                if (!this.props.item.error && this.props.focused) {
                    if (this.props.editPermission) {
                        this.props.setDeleteSchemaItemCallback(this.deleteThisSchema);
                    }
                }
            }.bind(this),
        );
    };

    render() {
        const { classes, editPermission, focused, item } = this.props;
        const { showSchemaDetail, rows } = this.state;
        const schemaItem = item;
        const schemaHeaders = schemaItem.header;
        const schemaName = schemaItem.schema_name;
        let headerHtml, bodyRowsHtml;
        const explanations = {
            field: 'field: Column name',
            'generalized-path':
                'generalized-path: A bundle metadata field (e.g., uuid, name, time, state) or a file path inside the bundle (e.g., /stdout, /stats.json).',
            'post-processor':
                'post-processor: (Optional) How to render the value (e.g., %.3f renders 3 decimal points, [0:8] takes the first 8 characters, duration renders seconds, size renders bytes).',
        };
        const headerText = {
            field: 'Column name',
            'generalized-path': 'Path to render',
            'post-processor': 'How to render',
        };
        headerHtml =
            (showSchemaDetail || this.props.create) &&
            schemaHeaders.map((header, index) => {
                return (
                    <TableCell
                        component='th'
                        key={index}
                        style={{ padding: '5', fontSize: '16px', maxWidth: '100' }}
                    >
                        {headerText[header]}
                        <Tooltip
                            title={
                                explanations[header] +
                                ' Click for more information and examples on schemas.'
                            }
                        >
                            <IconButton
                                href='https://codalab-worksheets.readthedocs.io/en/latest/Worksheet-Markdown/#schemas'
                                target='_blank'
                            >
                                <HelpOutlineOutlinedIcon fontSize='small' />
                            </IconButton>
                        </Tooltip>
                    </TableCell>
                );
            });
        if (headerHtml && editPermission) {
            headerHtml.push(
                <TableCell
                    key={headerHtml.length}
                    style={{ padding: '5' }}
                    component='th'
                    scope='row'
                >
                    {
                        <Tooltip title={'Add a new row before the first line'}>
                            <IconButton onClick={this.addFieldRowAfter(-1)}>
                                <AddCircleIcon />
                            </IconButton>
                        </Tooltip>
                    }
                    {
                        <Tooltip title={'Save all changes'}>
                            <IconButton
                                onClick={this.toggleEdit(false, true)}
                                disabled={
                                    /* disable if no textField value has changed compared with the initial state*/
                                    this.state.curSchemaName === schemaName &&
                                    !this.checkIfTextChanged()
                                }
                            >
                                <SaveIcon />
                            </IconButton>
                        </Tooltip>
                    }
                    {
                        <Tooltip title={'Revert all unsaved changes'}>
                            <IconButton
                                onClick={this.toggleEdit(true, false)}
                                disabled={
                                    /* disable if no textField value has changed compared with the initial state*/
                                    this.state.curSchemaName === schemaName &&
                                    !this.checkIfTextChanged()
                                }
                            >
                                <RestoreIcon />
                            </IconButton>
                        </Tooltip>
                    }{' '}
                    {
                        <Tooltip
                            title={
                                'Deletes whole schema, bundle blocks using the schema will be affected'
                            }
                        >
                            <IconButton
                                outlined
                                onClick={() => {
                                    if (this.props.create) {
                                        this.props.onSubmit();
                                        return;
                                    }
                                    this.props.setDeleteSchemaItemCallback(this.deleteThisSchema);
                                }}
                            >
                                <DeleteForeverIcon fontSize='small' />
                            </IconButton>
                        </Tooltip>
                    }
                </TableCell>,
            );
        }
        bodyRowsHtml =
            (showSchemaDetail || this.props.create) &&
            rows.map((rowItem, ind) => {
                let rowCells = schemaHeaders.map((headerKey, col) => {
                    return (
                        <TableCell
                            key={col}
                            style={{ padding: '5', borderBottom: 'none' }}
                            component='th'
                            scope='row'
                        >
                            <TextField
                                id={'textbox-' + ind + '-' + col}
                                error={
                                    headerKey === 'field' && this.state.rows[ind]['field'] === ''
                                }
                                helperText={
                                    headerKey === 'field' &&
                                    this.state.rows[ind]['field'] === '' &&
                                    'Fields with empty names will not be saved'
                                }
                                value={rowItem[headerKey] || ''}
                                multiline
                                disabled={
                                    !editPermission || rowItem.from_schema_name !== schemaName
                                }
                                onChange={this.changeFieldValue(ind, headerKey)}
                            />
                        </TableCell>
                    );
                });

                if (editPermission) {
                    if (rowItem.from_schema_name === schemaName) {
                        rowCells.push(
                            <TableCell
                                key={rowCells.length}
                                style={{ padding: '5', whiteSpace: 'nowrap' }}
                                component='th'
                                scope='row'
                            >
                                <Tooltip title={'Add a new row after this row'}>
                                    <IconButton onClick={this.addFieldRowAfter(ind)}>
                                        <AddCircleIcon />
                                    </IconButton>
                                </Tooltip>
                                <Tooltip title={'Delete this row'}>
                                    <IconButton onClick={this.removeFieldRow(ind)}>
                                        <DeleteSweepIcon />
                                    </IconButton>
                                </Tooltip>
                                <Tooltip title={'Move this row up'}>
                                    <IconButton
                                        disabled={ind === 0}
                                        onClick={this.moveFieldRow(ind, -1)}
                                    >
                                        <ArrowDropUpIcon />
                                    </IconButton>
                                </Tooltip>
                                <Tooltip title={'Move this row down'}>
                                    <IconButton
                                        disabled={ind === rows.length - 1}
                                        onClick={this.moveFieldRow(ind, 1)}
                                    >
                                        <ArrowDropDownIcon />
                                    </IconButton>
                                </Tooltip>
                            </TableCell>,
                        );
                    } else {
                        rowCells.push(
                            <TableCell>
                                Generated by another schema: {rowItem.from_schema_name}
                            </TableCell>,
                        );
                    }
                }
                return (
                    <TableBody>
                        <TableRow>{rowCells}</TableRow>
                    </TableBody>
                );
            });
        let schemaTable = null;
        if (showSchemaDetail || this.props.create) {
            schemaTable = (
                <Table className={classNames(classes.fullTable)}>
                    <TableHead>
                        <TableRow>{headerHtml}</TableRow>
                    </TableHead>
                    {bodyRowsHtml}
                </Table>
            );
        }
        if (!this.state.deleting && (focused || this.props.create)) {
            Mousetrap.bind(
                ['enter'],
                (e) => {
                    e.preventDefault();
                    this.setState(
                        { showSchemaDetail: !showSchemaDetail },
                        () =>
                            this.state.showSchemaDetail &&
                            this.setState({
                                rows: [...this.props.item.field_rows],
                                curSchemaName: this.props.item.schema_name,
                            }),
                    );
                },
                'keydown',
            );
            Mousetrap.bindGlobal(['ctrl+enter'], () => {
                if (
                    this.state.curSchemaName === '' ||
                    (!this.checkIfTextChanged() && this.state.curSchemaName === schemaName)
                )
                    return;
                this.saveSchema();
                Mousetrap.unbindGlobal(['ctrl+enter']);
            });
            Mousetrap.bindGlobal(['esc'], () => {
                if (this.props.create) {
                    Mousetrap.unbindGlobal(['ctrl+enter']);
                    this.props.onSubmit();
                }
                this.clearChanges();
                Mousetrap.unbindGlobal(['esc']);
            });
        }
        return (
            <div
                onClick={() => {
                    if (this.props.create) return;
                    this.props.setFocus(this.props.focusIndex, 0);
                }}
                id={this.getSchemaItemId()}
            >
                <Grid container direction='row'>
                    <Tooltip
                        title={
                            showSchemaDetail || this.props.create
                                ? ''
                                : 'Click to view schema: ' + schemaName
                        }
                        placement='right'
                    >
                        <Button
                            color='secondary'
                            variant='outlined'
                            onClick={() => {
                                this.setState(
                                    { showSchemaDetail: !showSchemaDetail },
                                    () =>
                                        this.state.showSchemaDetail &&
                                        this.setState({
                                            rows: [...this.props.item.field_rows],
                                            curSchemaName: this.props.item.schema_name,
                                        }),
                                );
                            }}
                            style={{
                                paddingLeft: '10px',
                                width: '100%',
                                height: '20px',
                            }}
                            className={classNames(focused ? classes.highlight : '')}
                        >
                            {showSchemaDetail || this.props.create ? (
                                <ArrowDropDownRoundedIcon />
                            ) : (
                                <ArrowRightRoundedIcon />
                            )}
                            <ViewListIcon style={{ padding: '0px' }} />
                        </Button>
                    </Tooltip>
                    {(showSchemaDetail || this.props.create) && (
                        <TextField
                            autoFocus
                            variant='outlined'
                            id='standard-multiline-static'
                            InputProps={{
                                style: {
                                    padding: 8,
                                },
                            }}
                            multiline
                            error={this.state.missingSchemaName}
                            helperText={
                                this.state.missingSchemaName ? 'Schema name can not be empty' : ''
                            }
                            size='small'
                            disabled={!editPermission}
                            value={this.state.curSchemaName}
                            style={{ paddingLeft: '20px' }}
                            onChange={this.changeSchemaName}
                        />
                    )}
                </Grid>
                {schemaTable}
            </div>
        );
    }
}

const styles = (theme) => ({
    highlight: {
        backgroundColor: `${theme.color.primary.lightest} !important`,
    },
});

export default withStyles(styles)(SchemaItem);
