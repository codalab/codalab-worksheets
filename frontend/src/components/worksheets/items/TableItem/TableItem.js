// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableCell from './TableCell';
import TableRow from '@material-ui/core/TableRow';
import { getMinMaxKeys } from '../../../../util/worksheet_utils';
import BundleRow from './BundleRow';
import Checkbox from '@material-ui/core/Checkbox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import SvgIcon from '@material-ui/core/SvgIcon';
import CheckBoxIcon from '@material-ui/icons/CheckBox';

class TableItem extends React.Component<{
    worksheetUUID: string,
    item: {},
    handleContextMenu: () => any,
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
            selectChildren:{},
            deselectChildren:{},
            numSelectedChild: 0,
            indeterminateCheckState: false,
        };
    }

    addControlSelectCallBack = (index, controlChildSelectFunc)=>{
        this.state.selectChildren[index] = controlChildSelectFunc;
    }

    changeSelfCheckCallBack = (childCheck)=>{
        console.log(this.state.numSelectedChild);
        if (childCheck){
            this.state.numSelectedChild += 1;
            if (this.state.numSelectedChild === Object.keys(this.state.selectChildren).length){
                this.setState({indeterminateCheckState:false, checked: true});
            }else{
                this.setState({indeterminateCheckState:true, checked: true});
            }
        }else{
            this.state.numSelectedChild -= 1;
            if (this.state.numSelectedChild <= 0){
                this.setState({numSelectedChild:0,indeterminateCheckState:false, checked: false});
            }else{
                this.setState({indeterminateCheckState:true, checked: true});
            }
        }
    }

    handleSelectAll = event => {
        if (event.target !== event.currentTarget){
            return;
        }
        let numSelectedChild = 0;
        Object.keys(this.state.selectChildren).map((rowIndex)=>{
            this.state.selectChildren[rowIndex](event.target.checked);
        })
        numSelectedChild = event.target.checked? Object.keys(this.state.selectChildren).length : 0;
        this.setState({ checked: event.target.checked, numSelectedChild: numSelectedChild, indeterminateCheckState: false });
    };

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    render() {
        const { worksheetUUID, setFocus, prevItem } = this.props;

        let prevItemProcessed = null;
        if (prevItem) {
            const { maxKey } = getMinMaxKeys(prevItem);
            prevItemProcessed = { sort_key: maxKey };
        }

        var tableClassName = this.props.focused ? 'table focused' : 'table';
        var item = this.props.item;
        var canEdit = this.props.canEdit;
        var bundleInfos = item.bundles_spec.bundle_infos;
        var headerItems = item.header;
        var headerHtml = headerItems.map((item, index) => {
            let checkbox;
            if (index === 0){
                checkbox = <Checkbox
                                checked={this.state.checked}
                                onChange={this.handleSelectAll}
                                value="checked"
                                icon={<CheckBoxOutlineBlankIcon fontSize="small" />}
                                checkedIcon={<CheckBoxIcon fontSize="small" />}
                                indeterminate={this.state.indeterminateCheckState}
                                indeterminateIcon={<SvgIcon fontSize="small">
                                                        <path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-2 10H7v-2h10v2z"/>
                                                    </SvgIcon>}
                                inputProps={{
                                'aria-label': 'select all checkbox',
                                }}
                                style={{marginRight: 30}}
                            />;
            }
            return (
                <TableCell component='th' key={index} style={{ paddingLeft: 0 }}>
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
            var rowRef = 'row' + rowIndex;
            var rowFocused = this.props.focused && rowIndex === this.props.subFocusIndex;
            var url = '/bundles/' + bundleInfos[rowIndex].uuid;
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
                    url={url}
                    bundleInfo={bundleInfos[rowIndex]}
                    prevBundleInfo={rowIndex > 0
                        ? bundleInfos[rowIndex - 1]
                        : prevItemProcessed }
                    uuid={bundleInfos[rowIndex].uuid}
                    headerItems={headerItems}
                    canEdit={canEdit}
                    updateRowIndex={this.updateRowIndex}
                    columnWithHyperlinks={columnWithHyperlinks}
                    handleContextMenu={this.props.handleContextMenu}
                    reloadWorksheet={this.props.reloadWorksheet}
                    ws={this.props.ws}
                    isLast={rowIndex === rowItems.length - 1}
                    handleCheckBundle={this.props.handleCheckBundle}
                    addControlSelectCallBack={this.addControlSelectCallBack}
                    changeSelfCheckCallBack={this.changeSelfCheckCallBack}
                    alreadyChecked={this.state.checked}
                />
            );
        });
        return (
            <div className='ws-item'>
                <TableContainer onMouseLeave={this.removeButtons}>
                    <Table className={tableClassName}>
                        <TableHead>
                            <TableRow style={{ height: 36 }}>{headerHtml}</TableRow>
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

export default TableItem;
