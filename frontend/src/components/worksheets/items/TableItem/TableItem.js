// @flow
import React, { useEffect } from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableCell from './TableCell';
import TableRow from '@material-ui/core/TableRow';
import BundleRow from './BundleRow';
import Checkbox from '@material-ui/core/Checkbox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import SvgIcon from '@material-ui/core/SvgIcon';
import CheckBoxIcon from '@material-ui/icons/CheckBox';
import { semaphore } from '../../../../util/async_loading_utils';
import { FETCH_STATUS_SCHEMA } from '../../../../constants';

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
        };
        this.copyCheckedBundleRows = this.copyCheckedBundleRows.bind(this);
    }

    // BULK OPERATION RELATED CODE
    // The functions below are code for handling row selection
    // The main idea is to let TableItem maintain its BundleRows' check status
    // this.state.childrenCheckState are the checkStatus of the bundle rows that belong to this table
    // BundleRow can also update itself through childrenCheck callback that TableItems passes
    // handleSelectAllClick & handleSelectAllSpaceHit handles select all events through click & space keydown

    refreshCheckBox = () => {
        let childrenStatus = new Array(this.props.item.rows.length).fill(false);
        this.setState({
            numSelectedChild: 0,
            childrenCheckState: childrenStatus,
            indeterminateCheckState: false,
            checked: false,
        });
    };

    componentDidUpdate(prevProps) {
        if (this.props.item.rows.length !== prevProps.item.rows.length) {
            let childrenStatus = new Array(this.props.item.rows.length).fill(false);
            this.setState({
                numSelectedChild: 0,
                childrenCheckState: childrenStatus,
                indeterminateCheckState: false,
                checked: false,
            });
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

    handleSelectAllClick = (event) => {
        if (event.target !== event.currentTarget) {
            return;
        }
        let numSelectedChild = 0;
        let childrenStatus = new Array(this.state.childrenCheckState.length).fill(
            event.target.checked,
        );
        numSelectedChild = event.target.checked ? childrenStatus.length : 0;
        this.setState({
            checked: event.target.checked,
            childrenCheckState: [...childrenStatus],
            numSelectedChild: numSelectedChild,
            indeterminateCheckState: false,
        });
    };

    copyCheckedBundleRows = () => {
        let item = this.props.item;
        let bundleInfos = item.bundles_spec.bundle_infos;
        let result = bundleInfos.filter((item, index) => {
            return this.state.childrenCheckState[index];
        });
        result = result.map((bundle) => {
            let bundleIdName = {};
            bundleIdName.uuid = bundle.uuid;
            bundleIdName.name = bundle.metadata.name;
            return bundleIdName;
        });
        return result;
    };

    // BULK OPERATION RELATED CODE ABOVE

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    render() {
        const { worksheetUUID, setFocus, editPermission } = this.props;
        // Provide copy data callback
        this.props.addCopyBundleRowsCallback(this.props.itemID, this.copyCheckedBundleRows);
        var tableClassName = this.props.focused ? 'table focused' : 'table';
        var item = this.props.item;
        var canEdit = this.props.canEdit;
        var bundleInfos = item.bundles_spec.bundle_infos;
        var headerItems = item.header;
        var headerHtml = headerItems.map((item, index) => {
            let checkbox;
            if (index === 0) {
                checkbox = (
                    <Checkbox
                        checked={this.state.checked}
                        onChange={this.handleSelectAllClick}
                        value='checked'
                        icon={
                            <CheckBoxOutlineBlankIcon
                                color={this.state.hovered ? 'action' : 'disabled'}
                                fontSize='small'
                            />
                        }
                        checkedIcon={<CheckBoxIcon fontSize='small' />}
                        indeterminate={this.state.indeterminateCheckState}
                        indeterminateIcon={
                            <SvgIcon fontSize='small'>
                                <path d='M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-2 10H7v-2h10v2z' />
                            </SvgIcon>
                        }
                    />
                );
            }
            return (
                <TableCell
                    onMouseEnter={(e) => this.setState({ hovered: true })}
                    onMouseLeave={(e) => this.setState({ hovered: false })}
                    component='th'
                    key={index}
                >
                    {checkbox}
                    {item}
                </TableCell>
            );
        });
        var rowItems = item.rows; // Array of {header: value, ...} objects
        var columnWithHyperlinks = [];
        Object.keys(rowItems[0]).forEach(function(x) {
            if (rowItems[0][x] && rowItems[0][x]['path']) columnWithHyperlinks.push(x);
        });
        var bodyRowsHtml = rowItems.map((rowItem, rowIndex) => {
            let bundleInfo = bundleInfos[rowIndex];
            let rowRef = 'row' + rowIndex;
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
                    ref={rowRef}
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
                    canEdit={canEdit}
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
                    onHideNewRun={this.props.onHideNewRun}
                    onHideNewText={this.props.onHideNewText}
                    ids={this.props.ids}
                />
            );
        });
        return (
            <div className='ws-item'>
                <TableContainer style={{ overflowX: 'auto' }}>
                    <Table className={tableClassName}>
                        <TableHead>
                            <TableRow
                                style={{
                                    height: 36,
                                    borderTop: '2px solid #DEE2E6',
                                    backgroundColor: '#F8F9FA',
                                }}
                            >
                                {headerHtml}
                            </TableRow>
                        </TableHead>
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

const styles = (theme) => ({
    tableContainer: {
        position: 'relative',
    },
});

const TableContainer = withStyles(styles)(_TableContainer);

async function fetchAsyncTableContents({ contents }) {
    return semaphore.use(async () => {
        const response = await $.ajax({
            type: 'POST',
            contentType: 'application/json',
            url: '/rest/interpret/genpath-table-contents',
            async: true,
            data: JSON.stringify({ contents }),
            dataType: 'json',
            cache: false,
        });
        return response;
    });
}

const TableWrapper = (props) => {
    const { item, onAsyncItemLoad } = props;
    useEffect(() => {
        (async function() {
            if (item.status.code === FETCH_STATUS_SCHEMA.BRIEFLY_LOADED) {
                try {
                    const { contents } = await fetchAsyncTableContents({ contents: item.rows });
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
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [item.rows, item.status]);
    return <TableItem {...props} />;
};

export default TableWrapper;
