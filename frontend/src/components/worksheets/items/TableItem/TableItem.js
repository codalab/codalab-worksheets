// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableCell from './TableCell';
import TableRow from '@material-ui/core/TableRow';
import Immutable from 'seamless-immutable';
import { worksheetItemPropsChanged, getMinMaxKeys } from '../../../../util/worksheet_utils';
import BundleRow from './BundleRow';

class TableItem extends React.Component<{
    worksheetUUID: string,
    item: {},
    handleContextMenu: () => any,
    reloadWorksheet: () => any,
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            yposition: -1,
            rowcenter: -1,
            rowIdx: -1,
            insertBefore: -1,
        });
    }

    updateRowIndex = (rowIndex) => {
        this.props.setFocus(this.props.focusIndex, rowIndex);
    };

    shouldComponentUpdate(nextProps, nextState) {
        return worksheetItemPropsChanged(this.props, nextProps);
    }

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
        var headerHtml = headerItems.map(function(item, index) {
            let styleDict = index == 0 ?  {paddingLeft: 42} : {};
            return (
                <TableCell component='th' key={index} style={ styleDict }>
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
