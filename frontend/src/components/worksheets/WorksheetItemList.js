import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import Dialog from '@material-ui/core/Dialog';
import * as Mousetrap from '../../util/ws_mousetrap_fork';
import { getAfterSortKey, getIds } from '../../util/worksheet_utils';
import ContentsItem from './items/ContentsItem';
import GraphItem from './items/GraphItem';
import ImageItem from './items/ImageItem';
import MarkdownItem from './items/MarkdownItem';
import RecordItem from './items/RecordItem';
import TableItem from './items/TableItem';
import SchemaItem from './items/SchemaItem';
import WorksheetItem from './items/WorksheetItem';
import ItemWrapper from './items/ItemWrapper';
import PlaceholderItem from './items/PlaceholderItem';
import NewUpload from './NewUpload/NewUpload';
import ImageEditor from './items/ImageEditor';
import TextEditorItem from './items/TextEditorItem';
import NewRun from './NewRun';
import { withStyles } from '@material-ui/core/styles';
import { DEFAULT_SCHEMA_ROWS } from '../../constants';

export const BLOCK_TO_COMPONENT = {
    markup_block: MarkdownItem,
    table_block: TableItem,
    contents_block: ContentsItem,
    subworksheets_block: WorksheetItem,
    record_block: RecordItem,
    image_block: ImageItem,
    graph_block: GraphItem,
    placeholder_block: PlaceholderItem,
    schema_block: SchemaItem,
};

// Create a worksheet item based on props and add it to worksheet_items.
// - item: information about the table to display
// - index: integer representing the index in the list of items
// - focused: whether this item has the focus
// - editPermission: whether we have permission to edit this item
// - setFocus: call back to select this item
// - updateWorksheetSubFocusIndex: call back to notify parent of which row is selected (for tables)
const addWorksheetItems = function(props, worksheet_items, prevItem, afterItem) {
    var item = props.item;

    // Determine URL corresponding to item.
    var url = null;
    if (
        item.bundles_spec &&
        item.bundles_spec.bundle_infos[0] &&
        item.bundles_spec.bundle_infos[0].uuid
    )
        url = '/bundles/' + item.bundles_spec.bundle_infos[0].uuid;
    if (item.subworksheet_info) url = '/worksheets/' + item.subworksheet_info.uuid;

    props.key = props.id = 'codalab-worksheet-item-' + props.focusIndex;
    props.url = url;
    props.prevItem = prevItem;
    props.after_sort_key = getAfterSortKey(item, props.subFocusIndex);
    props.ids = getIds(item);
    // showNewButtonsAfterEachBundleRow is set to true when we have a bundle table, because in this case,
    // we must show the new upload / new run buttons after each row in the table (in the BundleRow component)
    // as opposed to at the end of the table (in ItemWrapper).
    props.showNewButtonsAfterEachBundleRow =
        props.item.mode === 'table_block' && !props.item.loadedFromPlaceholder;
    const constructor = BLOCK_TO_COMPONENT[item.mode];

    let elem;
    if (constructor) {
        elem = React.createElement(constructor, props);
    } else {
        elem = React.createElement(
            'div',
            null,
            React.createElement(
                'strong',
                null,
                'Internal error unsupported block mode:',
                item.mode,
            ),
        );
    }
    worksheet_items.push(
        <ItemWrapper
            prevItem={prevItem}
            item={item}
            afterItem={afterItem}
            ws={props.ws}
            worksheetUUID={props.worksheetUUID}
            reloadWorksheet={props.reloadWorksheet}
            showNewRun={
                !props.showNewButtonsAfterEachBundleRow && props.focused && props.showNewRun
            }
            showNewText={
                !props.showNewButtonsAfterEachBundleRow && props.focused && props.showNewText
            }
            showNewSchema={
                !props.showNewButtonsAfterEachBundleRow && props.focused && props.showNewSchema
            }
            onError={props.onError}
            onHideNewRun={props.onHideNewRun}
            onHideNewText={props.onHideNewText}
            onHideNewSchema={props.onHideNewSchema}
            openBundle={props.openBundle}
            updateSchemaItem={props.updateSchemaItem}
            saveAndUpdateWorksheet={props.saveAndUpdateWorksheet}
            key={props.key}
            focusIndex={props.focusIndex}
            subFocusIndex={props.subFocusIndex}
            after_sort_key={props.after_sort_key}
            ids={props.ids}
            id={props.id}
        >
            {elem}
        </ItemWrapper>,
    );
};

class WorksheetItemList extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            newUploadKey: Math.random() + '',
        });
    }

    static displayName = 'WorksheetItemList';

    componentDidUpdate() {
        var info = this.props.ws.info;
        if (!info || !info.blocks.length) {
            $('.empty-worksheet').fadeIn('fast');
        }
    }

    capture_keys() {
        // Move focus to the top, above the first item of worksheet
        Mousetrap.bind(
            ['g g'],
            function() {
                $('body')
                    .stop(true)
                    .animate({ scrollTop: 0 }, 'fast');
                this.props.setFocus(-1, 0);
            }.bind(this),
            'keydown',
        );

        // Move focus to the bottom
        Mousetrap.bind(
            ['shift+g'],
            function() {
                this.props.setFocus(this.props.ws.info.blocks.length - 1, 'end');
            }.bind(this),
            'keydown',
        );
    }

    bundleUuidToIndex() {
        // bundle uuid -> an array of [index, subIndex], corresponding to positions where the bundle occurs
        // E.g. 0x47bda9 -> [[0, 1], [2, 3]], which means bundle 0x47bda9 appears twice in the current worksheet
        var uuidToIndex = {};
        var info = this.props.ws.info;
        if (info && info.blocks.length > 0) {
            var items = info.blocks;
            for (var index = 0; index < items.length; index++) {
                if (items[index].bundles_spec) {
                    for (
                        var subIndex = 0;
                        subIndex < items[index].bundles_spec.bundle_infos.length;
                        subIndex++
                    ) {
                        var bundle_info = items[index].bundles_spec.bundle_infos[subIndex];
                        if (!(bundle_info.uuid in uuidToIndex)) uuidToIndex[bundle_info.uuid] = [];
                        uuidToIndex[bundle_info.uuid].push([index, subIndex]);
                    }
                }
            }
        }
        return uuidToIndex;
    }

    handleClickForDeselect = (event) => {
        //Deselect if clicking between worksheet row items
        if (event.target === event.currentTarget) {
            this.props.setFocus(-1, 0, false);
        }
    };

    render() {
        if (this.props.active) this.capture_keys(); // each item capture keys are handled dynamically after this call

        // Create items
        var items_display;
        var info = this.props.ws.info;
        if (info && info.blocks.length === 0) {
            // Create a "dummy" item at the beginning so that only empty text can be added.
            info.blocks = [
                {
                    isDummyItem: true,
                    text: '',
                    mode: 'markup_block',
                    sort_keys: [-1], // the dummy item represents the top of the worksheet, so its sort key is -1
                    ids: [null],
                    is_refined: true,
                },
            ];
        }
        let focusedItem;
        if (info && info.blocks.length > 0) {
            var worksheet_items = [];
            info.blocks.forEach(
                function(item, index) {
                    const focused = index === this.props.focusIndex;

                    // focused determines whether clicking on Cell/Upload/Run will
                    // apply to this cell. If nothing is focused (focusIndex = -1),
                    // prepend to the top by default.
                    if (focused) {
                        focusedItem = item;
                    }
                    var props = {
                        worksheetUUID: info.uuid,
                        item: item,
                        version: this.props.version,
                        active: this.props.active,
                        focused,
                        focusIndex: index,
                        subFocusIndex: this.props.subFocusIndex,
                        setFocus: this.props.setFocus,
                        focusTerminal: this.props.focusTerminal,
                        openWorksheet: this.props.openWorksheet,
                        openBundle: this.props.openBundle,
                        reloadWorksheet: this.props.reloadWorksheet,
                        ws: this.props.ws,
                        showNewRun: this.props.showNewRun,
                        showNewText: this.props.showNewText,
                        showNewRerun: this.props.showNewRerun,
                        showNewSchema: this.props.showNewSchema,
                        onHideNewRun: this.props.onHideNewRun,
                        onHideNewText: this.props.onHideNewText,
                        onHideNewRerun: this.props.onHideNewRerun,
                        onHideNewSchema: this.props.onHideNewSchema,
                        onHideNewImage: this.props.onHideNewImage,
                        onError: this.props.onError,
                        handleCheckBundle: this.props.handleCheckBundle,
                        confirmBundleRowAction: this.props.confirmBundleRowAction,
                        setDeleteItemCallback: this.props.setDeleteItemCallback,
                        editPermission: info && info.edit_permission,
                        addCopyBundleRowsCallback: this.props.addCopyBundleRowsCallback,
                        addShowContentBundleRowsCallback: this.props
                            .addShowContentBundleRowsCallback,
                        itemID: index,
                        updateBundleBlockSchema: this.props.updateBundleBlockSchema,
                        saveAndUpdateWorksheet: this.props.saveAndUpdateWorksheet,
                        onAsyncItemLoad: (item) => this.props.onAsyncItemLoad(index, item),
                        updateSchemaItem: this.props.updateSchemaItem,
                        setDeleteSchemaItemCallback: this.props.setDeleteSchemaItemCallback,
                    };
                    addWorksheetItems(
                        props,
                        worksheet_items,
                        index > 0 ? info.blocks[index - 1] : null,
                        index < info.blocks.length - 1 ? info.blocks[index + 1] : null,
                    );
                }.bind(this),
            );
            items_display = (
                <>
                    {/*Show new runs/text at the top of worksheet when no blocks are focused*/}
                    {this.props.showNewText && !focusedItem && (
                        <TextEditorItem
                            mode='create'
                            after_sort_key={-1}
                            worksheetUUID={info.uuid}
                            reloadWorksheet={() => this.props.reloadWorksheet(undefined, (0, 0))}
                            closeEditor={() => {
                                this.props.onHideNewText();
                            }}
                        />
                    )}
                    {this.props.showNewSchema && !focusedItem && (
                        <SchemaItem
                            after_sort_key={-1}
                            ws={this.props.ws}
                            onSubmit={() => this.props.onHideNewSchema()}
                            reloadWorksheet={() => this.props.reloadWorksheet(undefined, (0, 0))}
                            editPermission={true}
                            item={{
                                field_rows: DEFAULT_SCHEMA_ROWS,
                                header: ['field', 'generalized-path', 'post-processor'],
                                schema_name: '',
                                sort_keys: [0],
                            }}
                            create={true}
                            updateSchemaItem={this.props.updateSchemaItem}
                            setDeleteSchemaItemCallback={this.props.setDeleteSchemaItemCallback}
                            focusIndex={this.props.focusIndex}
                            subFocusIndex={this.props.subFocusIndex}
                        />
                    )}
                    <div className={this.props.classes.wsItemListContainer}>{worksheet_items}</div>
                    <NewUpload
                        key={this.state.newUploadKey}
                        after_sort_key={getAfterSortKey(focusedItem, this.props.subFocusIndex)}
                        worksheetUUID={info.uuid}
                        reloadWorksheet={this.props.reloadWorksheet}
                        // Reset newUploadKey so that NewUpload gets re-rendered. This way,
                        // it is possible to upload the same file multiple times in a row
                        // (otherwise, chrome will not call onchange on a file input when
                        // the file hasn't changed)
                        onUploadFinish={(e) => this.setState({ newUploadKey: Math.random() + '' })}
                        focusedItem={focusedItem}
                    />
                    <ImageEditor
                        key={this.state.newUploadKey + 1}
                        after_sort_key={getAfterSortKey(focusedItem, this.props.subFocusIndex)}
                        worksheetUUID={info.uuid}
                        reloadWorksheet={this.props.reloadWorksheet}
                        onUploadFinish={(e) => this.setState({ newUploadKey: Math.random() + '' })}
                        ws={this.props.ws}
                        focusIndex={this.props.focusIndex}
                        subFocusIndex={this.props.subFocusIndex}
                    />
                    <Dialog
                        open={!focusedItem && this.props.showNewRun}
                        onClose={this.props.onHideNewRun}
                        maxWidth='lg'
                    >
                        <NewRun
                            after_sort_key={-1}
                            ws={this.props.ws}
                            onSubmit={() => this.props.onHideNewRun()}
                            onError={this.props.onError}
                            reloadWorksheet={() => this.props.reloadWorksheet(undefined, (0, 0))}
                        />
                    </Dialog>
                </>
            );
        } else {
            items_display = null;
        }
        if (info && info.error)
            items_display = <p className='alert-danger'>Error in worksheet: {info.error}</p>;
        return (
            <div
                className={this.props.classes.wsItemsDisplayContainer}
                onClick={this.handleClickForDeselect}
            >
                {items_display}
            </div>
        );
    }
}

const styles = (theme) => ({
    insertBox: {
        border: `2px solid ${theme.color.primary.base}`,
        margin: '32px 64px !important',
    },
    wsItemsDisplayContainer: {
        display: 'flex',
        flexDirection: 'column',
        flex: 1,
    },
    wsItemListContainer: {
        display: 'flex',
        flexDirection: 'column',
        flex: 1,
        padding: 25,
    },
});

export default withStyles(styles)(WorksheetItemList);
