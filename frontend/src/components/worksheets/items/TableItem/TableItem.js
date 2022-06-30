// @flow
import React, { useEffect } from 'react';
import classNames from 'classnames';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableCell from './TableCell';
import TableRow from '@material-ui/core/TableRow';
import ViewListIcon from '@material-ui/icons/ViewList';
import IconButton from '@material-ui/core/IconButton';
import SaveIcon from '@material-ui/icons/Save';
import RestoreIcon from '@material-ui/icons/Restore';
import TextField from '@material-ui/core/TextField';
import Tooltip from '@material-ui/core/Tooltip';
import BundleRow from './BundleRow';
import BundleStateTooltip from '../../../BundleStateTooltip';
import { getIds } from '../../../../util/worksheet_utils';
import { FETCH_STATUS_SCHEMA } from '../../../../constants';
import { fetchAsyncBundleContents } from '../../../../util/apiWrapper';
import * as Mousetrap from '../../../../util/ws_mousetrap_fork';

class TableItem extends React.Component<{
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
            checked: false,
            hovered: false,
            childrenCheckState: new Array(this.props.item.rows.length).fill(false),
            numSelectedChild: 0,
            indeterminateCheckState: false,
            curSchemaNames: this.props.item.using_schemas.join(' '),
            openSchemaTextBox: false,
        };
        this.copyCheckedBundleRows = this.copyCheckedBundleRows.bind(this);
        this.showCheckedBundleRowsContents = this.showCheckedBundleRowsContents.bind(this);
    }

    // BULK OPERATION RELATED CODE
    // The functions below are code for handling row selection
    // The main idea is to let TableItem maintain its BundleRows' check status
    // this.state.childrenCheckState are the checkStatus of the bundle rows that belong to this table
    // BundleRow can also update itself through childrenCheck callback that TableItems passes

    refreshCheckBox = () => {
        let childrenStatus = new Array(this.props.item.rows.length).fill(false);
        this.setState({
            numSelectedChild: 0,
            childrenCheckState: childrenStatus,
            indeterminateCheckState: false,
            checked: false,
        });
    };

    componentDidUpdate(prevProps, prevState) {
        if (this.props.item.rows.length !== prevProps.item.rows.length) {
            let childrenStatus = new Array(this.props.item.rows.length).fill(false);
            this.setState({
                numSelectedChild: 0,
                childrenCheckState: childrenStatus,
                indeterminateCheckState: false,
                checked: false,
            });
        }
        // If the schema was changed through editing source
        let prevSchemas = prevProps.item.using_schemas.join(' ');
        let newSchemas = this.props.item.using_schemas.join(' ');
        if (prevSchemas !== newSchemas) {
            this.setState({
                curSchemaNames: newSchemas,
            });
        }
        if (this.state.openSchemaTextBox && !prevState.openSchemaTextBox) {
            document.getElementById('table-schema-' + this.props.itemID).focus();
        }
    }

    childrenCheck = (rowIndex, check) => {
        let childrenStatus = this.state.childrenCheckState;
        childrenStatus[rowIndex] = check;
        let selectedChildren = check
            ? this.state.numSelectedChild + 1
            : this.state.numSelectedChild - 1;
        let indeterminateCheckState =
            selectedChildren < this.state.childrenCheckState.length && selectedChildren > 0;
        let selfChecked = selectedChildren > 0;
        this.setState({
            numSelectedChild: selectedChildren,
            childrenCheckState: childrenStatus,
            indeterminateCheckState: indeterminateCheckState,
            checked: selfChecked,
        });
    };

    copyCheckedBundleRows = () => {
        let item = this.props.item;
        let bundleInfos = item.bundles_spec.bundle_infos;
        let ids = getIds(item).filter((item, index) => {
            return this.state.childrenCheckState[index];
        });
        let checkedBundleInfos = bundleInfos.filter((item, index) => {
            return this.state.childrenCheckState[index];
        });
        return checkedBundleInfos.map((bundle, index) => {
            let bundleIdName = {};
            bundleIdName.uuid = bundle.uuid;
            bundleIdName.name = bundle.metadata.name;
            bundleIdName.id = ids[index];
            return bundleIdName;
        });
    };

    showCheckedBundleRowsContents = () => {
        let item = this.props.item;
        let rows = [...Array(item.rows.length).keys()].filter((item, index) => {
            return this.state.childrenCheckState[index];
        });
        return rows.map((rowIndex, index) => {
            let bundleIndex = {};
            // Get the raw index of each checked bundle row
            bundleIndex.rawIndex = item.first_bundle_source_index + rowIndex;
            return bundleIndex;
        });
    };

    // BULK OPERATION RELATED CODE ABOVE

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    changeSchemaName = (e) => {
        this.setState({ curSchemaNames: e.target.value.replace(/\n/g, ' ') });
    };

    render() {
        const { classes, worksheetUUID, setFocus, editPermission } = this.props;
        // Provide copy data callback
        this.props.addCopyBundleRowsCallback(this.props.itemID, this.copyCheckedBundleRows);
        this.props.addShowContentBundleRowsCallback(
            this.props.itemID,
            this.showCheckedBundleRowsContents,
        );
        let tableClassName = this.props.focused ? 'table focused' : 'table';
        let item = this.props.item;
        let bundleInfos = item.bundles_spec.bundle_infos;
        let headerItems = item.header;
        let headerHtml = headerItems.map((item, index) => {
            const showStateTooltip = item === 'state';
            return (
                <TableCell
                    onMouseEnter={(e) => this.setState({ hovered: true })}
                    onMouseLeave={(e) => this.setState({ hovered: false })}
                    component='th'
                    key={index}
                    classes={{
                        root: classNames({
                            [classes.tableHeader]: true,
                        }),
                    }}
                    style={
                        index === 0
                            ? {
                                  paddingLeft: editPermission ? '30px' : '70px',
                                  paddingBottom: 0,
                                  paddingTop: 0,
                              }
                            : {}
                    }
                >
                    {editPermission && index === 0 && (
                        <Tooltip title={'Change the schemas of this table'}>
                            <IconButton>
                                <ViewListIcon
                                    style={{ padding: '0px', height: 15 }}
                                    onClick={() => {
                                        this.setState({
                                            openSchemaTextBox: !this.state.openSchemaTextBox,
                                        });
                                    }}
                                />
                            </IconButton>
                        </Tooltip>
                    )}
                    {item}
                    {showStateTooltip && <BundleStateTooltip />}
                </TableCell>
            );
        });
        let rowItems = item.rows; // Array of {header: value, ...} objects
        let columnWithHyperlinks = [];
        Object.keys(rowItems[0]).forEach(function(x) {
            if (rowItems[0][x] && rowItems[0][x]['path']) columnWithHyperlinks.push(x);
        });
        let bodyRowsHtml = rowItems.map((rowItem, rowIndex) => {
            let bundleInfo = bundleInfos[rowIndex];
            let rowFocused = this.props.focused && rowIndex === this.props.subFocusIndex;
            let url = '/bundles/' + bundleInfo.uuid;
            let worksheet = bundleInfo.host_worksheet;
            let worksheetName, worksheetUrl;
            if (worksheet !== undefined) {
                worksheetName = worksheet.name;
                worksheetUrl = '/worksheets/' + worksheet.uuid;
            }
            return (
                <BundleRow
                    key={rowIndex}
                    id={`codalab-worksheet-item-${this.props.focusIndex}-subitem-${rowIndex}`}
                    worksheetUUID={worksheetUUID}
                    item={rowItem}
                    rowIndex={rowIndex}
                    focused={rowFocused}
                    focusIndex={this.props.focusIndex}
                    setFocus={setFocus}
                    showNewRerun={this.props.showNewRerun}
                    onHideNewRerun={this.props.onHideNewRerun}
                    url={url}
                    bundleInfo={bundleInfo}
                    uuid={bundleInfo.uuid}
                    headerItems={headerItems}
                    updateRowIndex={this.updateRowIndex}
                    columnWithHyperlinks={columnWithHyperlinks}
                    reloadWorksheet={this.props.reloadWorksheet}
                    ws={this.props.ws}
                    checkStatus={this.state.childrenCheckState[rowIndex]}
                    isLast={rowIndex === rowItems.length - 1}
                    handleCheckBundle={this.props.handleCheckBundle}
                    confirmBundleRowAction={this.props.confirmBundleRowAction}
                    childrenCheck={this.childrenCheck}
                    refreshCheckBox={this.refreshCheckBox}
                    worksheetName={worksheetName}
                    worksheetUrl={worksheetUrl}
                    editPermission={editPermission}
                    after_sort_key={this.props.after_sort_key}
                    showNewRun={
                        this.props.showNewButtonsAfterEachBundleRow &&
                        this.props.showNewRun &&
                        rowFocused
                    }
                    showNewText={
                        this.props.showNewButtonsAfterEachBundleRow &&
                        this.props.showNewText &&
                        rowFocused
                    }
                    showNewSchema={
                        this.props.showNewButtonsAfterEachBundleRow &&
                        this.props.showNewSchema &&
                        rowFocused
                    }
                    onHideNewRun={this.props.onHideNewRun}
                    onHideNewText={this.props.onHideNewText}
                    onHideNewSchema={this.props.onHideNewSchema}
                    updateSchemaItem={this.props.updateSchemaItem}
                    ids={this.props.ids}
                />
            );
        });
        if (this.state.openSchemaTextBox) {
            Mousetrap.bindGlobal(['ctrl+enter'], () => {
                this.setState({ openSchemaTextBox: false });
                this.props.updateBundleBlockSchema(
                    this.state.curSchemaNames,
                    'table',
                    this.props.item.first_bundle_source_index,
                );
                Mousetrap.unbindGlobal(['ctrl+enter']);
            });
        }
        return (
            <div className='ws-item'>
                <TableContainer style={{ overflowX: 'auto', overflowY: 'hidden' }}>
                    <Table className={tableClassName} style={{ marginBottom: '0.5px' }}>
                        <TableHead>
                            <TableRow
                                style={{
                                    height: 32,
                                    borderTop: '0px solid #DEE2E6',
                                    backgroundColor: '#F8F9FA',
                                }}
                            >
                                {headerHtml}
                            </TableRow>
                        </TableHead>
                        {this.state.openSchemaTextBox && (
                            <TableRow
                                style={{
                                    borderBottom: '2px solid #DEE2E6',
                                    padding: '0px',
                                }}
                            >
                                <TableCell colSpan='100%' style={{ padding: '0px 0px 0px 30px' }}>
                                    <TextField
                                        variant='outlined'
                                        InputProps={{
                                            style: {
                                                padding: '10px 10px',
                                            },
                                        }}
                                        id={'table-schema-' + this.props.itemID}
                                        multiline
                                        value={this.state.curSchemaNames || ''}
                                        onChange={this.changeSchemaName}
                                        size='small'
                                        placeholder={'Using default schema'}
                                    />
                                    <IconButton
                                        onClick={() => {
                                            this.setState({ openSchemaTextBox: false });
                                            this.props.updateBundleBlockSchema(
                                                this.state.curSchemaNames,
                                                'table',
                                                this.props.item.first_bundle_source_index,
                                            );
                                        }}
                                        disabled={
                                            this.state.curSchemaNames ===
                                            this.props.item.using_schemas.join(' ')
                                        }
                                        style={{ marginTop: '5px' }}
                                    >
                                        <SaveIcon />
                                    </IconButton>
                                    <IconButton
                                        onClick={() => {
                                            this.setState({
                                                curSchemaNames: this.props.item.using_schemas.join(
                                                    ' ',
                                                ),
                                            });
                                        }}
                                        disabled={
                                            this.state.curSchemaNames ===
                                            this.props.item.using_schemas.join(' ')
                                        }
                                        style={{ marginTop: '5px' }}
                                    >
                                        <RestoreIcon />
                                    </IconButton>
                                </TableCell>
                            </TableRow>
                        )}
                        {bodyRowsHtml}
                    </Table>
                </TableContainer>
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

const styles = () => ({
    tableContainer: {
        position: 'relative',
    },
    tableHeader: {
        position: 'sticky',
        top: 0,
        backgroundColor: '#F8F9FA',
        zIndex: 1,
        color: '#000000',
        height: 26,
    },
});

const TableContainer = withStyles(styles)(_TableContainer);

const TableWrapper = (props) => {
    const { item, onAsyncItemLoad } = props;
    useEffect(() => {
        (async function() {
            if (item.status.code === FETCH_STATUS_SCHEMA.BRIEFLY_LOADED) {
                try {
                    const { contents } = await fetchAsyncBundleContents({
                        contents: item.rows,
                    });
                    onAsyncItemLoad({
                        ...item,
                        rows: contents,
                        status: {
                            code: FETCH_STATUS_SCHEMA.READY,
                            error_message: '',
                        },
                    });
                } catch (e) {
                    console.error(e);
                    // TODO: better error message handling here.
                }
            }
        })();
        // TODO: see how we can add onAsyncItemLoad as a dependency, if needed.
    }, [item, item.rows, item.status, onAsyncItemLoad]);
    return <TableItem {...props} />;
};

export default withStyles(styles)(TableWrapper);
