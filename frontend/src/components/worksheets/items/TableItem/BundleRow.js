import classNames from 'classnames';
import React, { Component } from 'react';
import { withStyles } from '@material-ui/core/styles';
import Dialog from '@material-ui/core/Dialog';
import Tooltip from '@material-ui/core/Tooltip';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import Checkbox from '@material-ui/core/Checkbox';
import CheckBoxOutlineBlankIcon from '@material-ui/icons/CheckBoxOutlineBlank';
import CheckBoxIcon from '@material-ui/icons/CheckBox';
import ExpandIcon from '../../../Icons/ExpandIcon';
import NewRun from '../../NewRun';

import * as Mousetrap from '../../../../util/ws_mousetrap_fork';
import TextEditorItem from '../TextEditorItem';
import SchemaItem from '../SchemaItem';
import { DEFAULT_SCHEMA_ROWS } from '../../../../constants';

// The approach taken in this design is to hack the HTML `Table` element by using one `TableBody` for each `BundleRow`.
// We need the various columns to be aligned for all `BundleRow` within a `Table`, therefore using `div` is not an
// option. Instead, we must make use of zero-height rows.

class BundleRow extends Component {
    constructor(props) {
        super(props);
        this.state = {
            bundleInfoUpdates: {},
            openDelete: false,
            runProp: {},
            hovered: false,
            uniqueIdentifier: Math.random() * 10000,
        };
    }

    // BULK OPERATION RELATED CODE
    handleCheckboxChange = (event) => {
        this.props.handleCheckBundle(
            this.props.uuid,
            this.state.uniqueIdentifier,
            event.target.checked,
            this.props.refreshCheckBox,
        );
        this.props.childrenCheck(this.props.rowIndex, event.target.checked);
    };

    componentDidMount() {
        if (this.props.checkStatus) {
            this.props.handleCheckBundle(
                this.props.uuid,
                this.state.uniqueIdentifier,
                true,
                this.props.refreshCheckBox,
            );
        }
    }

    componentDidUpdate(prevProp) {
        if (this.props.checkStatus !== prevProp.checkStatus) {
            this.props.handleCheckBundle(
                this.props.uuid,
                this.state.uniqueIdentifier,
                this.props.checkStatus,
                this.props.refreshCheckBox,
            );
        }
    }
    // BULK OPERATION RELATED CODE

    receiveBundleInfoUpdates = (update) => {
        let { bundleInfoUpdates } = this.state;
        // Use object spread to update.
        bundleInfoUpdates = { ...bundleInfoUpdates, ...update };
        this.setState({ bundleInfoUpdates });
    };

    handleSelectRowClick = () => {
        this.props.updateRowIndex(this.props.rowIndex);
    };

    render() {
        const { bundleInfoUpdates } = this.state;
        const {
            classes,
            bundleInfo,
            item,
            reloadWorksheet,
            checkStatus,
            openBundle,
            onHideNewRun,
            ws,
        } = this.props;
        const rowItems = { ...item, ...bundleInfoUpdates };
        var baseUrl = this.props.url;
        var uuid = this.props.uuid;
        var columnWithHyperlinks = this.props.columnWithHyperlinks;
        var worksheetName = this.props.worksheetName;
        var worksheetUrl = this.props.worksheetUrl;
        var rowCells = this.props.headerItems.map((headerKey, col) => {
            var rowContent = rowItems[headerKey];
            // See if there's a link
            var url;
            var openBundleButton;
            var checkBox;
            if (headerKey === 'host_worksheet' && worksheetUrl !== undefined) {
                url = worksheetUrl;
                rowContent = worksheetName;
            } else if (col === 0) {
                url = baseUrl;
                checkBox = (
                    <Checkbox
                        icon={<CheckBoxOutlineBlankIcon fontSize='small' />}
                        checkedIcon={<CheckBoxIcon fontSize='small' />}
                        onChange={this.handleCheckboxChange}
                        checked={checkStatus || false}
                        classes={{ root: classes.checkBox }}
                    />
                );
                openBundleButton = (
                    <Tooltip title='Open full bundle details.'>
                        <button
                            onClick={() => {
                                openBundle(uuid, this.props.after_sort_key);
                            }}
                            className={classes.openBundleBtn}
                            aria-label='Open full bundle details.'
                        >
                            <ExpandIcon className={classes.expandIcon} />
                        </button>
                    </Tooltip>
                );
            } else if (columnWithHyperlinks.indexOf(headerKey) !== -1) {
                url = '/rest/bundles/' + uuid + '/contents/blob' + rowContent['path'];
                if ('text' in rowContent) {
                    rowContent = rowContent['text'];
                } else {
                    // In case text doesn't exist, content will default to basename of the path
                    // indexing 1 here since the path always starts with '/'
                    rowContent = rowContent['path'].split('/')[1];
                }
            }
            if (Array.isArray(rowContent)) {
                if (rowContent.length === 1) {
                    // This means that the user has no access to the cell since PermissionError occurred.
                    // ['Forbidden'] will be returned
                    rowContent = <span style={{ color: 'grey' }}>Forbidden</span>;
                } else if (rowContent.length === 3) {
                    // Cell is a bundle genpath triple -- see is_bundle_genpath_triple() in backend.
                    // This means that the cell is only briefly loaded.
                    rowContent = <span style={{ color: 'grey' }}>Loading...</span>;
                }
            }
            if (url)
                rowContent = (
                    <a
                        href={url}
                        className='bundle-link'
                        target='_blank'
                        rel='noopener noreferrer'
                        // Instead of setting a fixed width for the table cell, provide a width range to allow the cell be adaptive
                        style={{ display: 'inline-block', minWidth: 60, maxWidth: 230 }}
                    >
                        {rowContent}
                    </a>
                );
            if (
                !rowContent ||
                (typeof rowContent === 'object' && !React.isValidElement(rowContent))
            ) {
                rowContent = '';
            }

            return (
                <TableCell
                    key={col}
                    classes={{
                        root: classNames({
                            [classes.rootNoPad]: true,
                            [classes.noCheckBox]: !checkBox,
                            [classes.withCheckBox]: checkBox,
                        }),
                    }}
                    onMouseEnter={(e) => this.setState({ hovered: true })}
                    onMouseLeave={(e) => this.setState({ hovered: false })}
                >
                    {checkBox}
                    {openBundleButton}
                    {rowContent}
                </TableCell>
            );
        });
        if (this.props.focused) {
            // Use e.preventDefault to avoid openning selected link
            Mousetrap.bind(
                ['shift+enter'],
                (e) => {
                    e.preventDefault();
                    window.open(this.props.url, '_blank');
                },
                'keydown',
            );
            Mousetrap.bind(['x'], (e) => {
                if (!this.props.confirmBundleRowAction(e.code)) {
                    this.props.handleCheckBundle(
                        uuid,
                        this.state.uniqueIdentifier,
                        !this.props.checkStatus,
                        this.props.refreshCheckBox,
                    );
                    this.props.childrenCheck(this.props.rowIndex, !this.props.checkStatus);
                }
            });

            if (
                this.props.focusIndex >= 0 &&
                ws.info.blocks[this.props.focusIndex].mode === 'table_block'
            ) {
                const isRunBundle = bundleInfo.bundle_type === 'run' && bundleInfo.metadata;
                const isDownloadableRunBundle =
                    bundleInfo.state !== 'preparing' &&
                    bundleInfo.state !== 'starting' &&
                    bundleInfo.state !== 'created' &&
                    bundleInfo.state !== 'staged';
                Mousetrap.bind(['a s'], (e) => {
                    if (!isRunBundle || isDownloadableRunBundle) {
                        const bundleDownloadUrl =
                            '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
                        window.open(bundleDownloadUrl, '_blank');
                    }
                });
            }

            // unbind shortcuts that are active for markdown_block and worksheet_block
            Mousetrap.unbind('i');
        }

        return (
            <>
                <TableBody classes={{ root: classes.tableBody }} id={this.props.id}>
                    {/** ---------------------------------------------------------------------------------------------------
                     *  Main Content
                     */}
                    <TableRow
                        onClick={this.handleSelectRowClick}
                        className={classNames({
                            [classes.contentRow]: true,
                            [classes.highlight]: this.props.focused,
                        })}
                    >
                        {rowCells}
                    </TableRow>
                    {/** ---------------------------------------------------------------------------------------------------
                     *  Insert the new text/schema below the bundle row, so add 1 to after_sort_key
                     */}
                    {this.props.showNewText && (
                        <TableRow>
                            <TableCell colSpan='100%' classes={{ root: classes.insertPanel }}>
                                <div className={classes.insertBox}>
                                    <TextEditorItem
                                        ids={this.props.ids}
                                        mode='create'
                                        after_sort_key={this.props.after_sort_key + 1}
                                        worksheetUUID={this.props.worksheetUUID}
                                        reloadWorksheet={reloadWorksheet}
                                        closeEditor={() => {
                                            this.props.onHideNewText();
                                        }}
                                    />
                                </div>
                            </TableCell>
                        </TableRow>
                    )}
                    {this.props.showNewSchema && (
                        <TableRow>
                            <TableCell colSpan='100%' classes={{ root: classes.insertPanel }}>
                                <div className={classes.insertBox}>
                                    <SchemaItem
                                        after_sort_key={this.props.after_sort_key + 1}
                                        ws={this.props.ws}
                                        onSubmit={() => this.props.onHideNewSchema()}
                                        reloadWorksheet={reloadWorksheet}
                                        editPermission={true}
                                        item={{
                                            field_rows: DEFAULT_SCHEMA_ROWS,
                                            header: ['field', 'generalized-path', 'post-processor'],
                                            schema_name: '',
                                            sort_keys: [this.props.after_sort_key + 2],
                                        }}
                                        create={true}
                                        updateSchemaItem={this.props.updateSchemaItem}
                                    />
                                </div>
                            </TableCell>
                        </TableRow>
                    )}
                </TableBody>
                {/** ---------------------------------------------------------------------------------------------------
                 *  New Run Dialog
                 */}
                <Dialog open={this.props.showNewRun} onClose={onHideNewRun} maxWidth='md'>
                    <NewRun
                        after_sort_key={this.props.after_sort_key}
                        ws={this.props.ws}
                        onError={this.props.onError}
                        onSubmit={() => onHideNewRun()}
                        reloadWorksheet={reloadWorksheet}
                    />
                </Dialog>
            </>
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
        padding: '0px 4px !important',
        wordWrap: 'break-word', // Allows unbreakable words to be broken to avoid overflow
    },
    noCheckBox: {
        maxWidth: 200,
        minWidth: 110,
    },
    withCheckBox: {
        maxWidth: 230,
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
            backgroundColor: theme.color.grey.lightest,
            borderTop: `2px solid ${theme.color.grey.base}`,
            borderBottom: `2px solid ${theme.color.grey.base}`,
        },
    },
    checkBox: {
        color: theme.color.grey.dark,
        '&:hover': {
            color: theme.color.grey.darker,
        },
    },
    highlight: {
        backgroundColor: `${theme.color.primary.lightest} !important`,
        borderLeft: '3px solid #1d91c0',
    },
    lowlight: {
        backgroundColor: `${theme.color.grey.light} !important`,
    },
    insertPanel: {
        paddingLeft: '32px !important', // align with bundle detail
        paddingRight: '32px !important', // align with bundle detail
    },
    openBundleBtn: {
        backgroundColor: 'transparent',
        border: 'none',
        verticalAlign: 'text-top',
        paddingLeft: 8,
        paddingRight: 11,
    },
    expandIcon: {
        height: 13,
        width: 13,
        fill: theme.color.grey.darkest,
    },
});

export default withStyles(styles)(BundleRow);
