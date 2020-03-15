import * as React from 'react';
import Immutable from 'seamless-immutable';
import $ from 'jquery';
import * as Mousetrap from '../../util/ws_mousetrap_fork';
import { buildTerminalCommand, getMinMaxKeys } from '../../util/worksheet_utils';
import { ContextMenuEnum, ContextMenuMixin } from './ContextMenu';
import ContentsItem from './items/ContentsItem';
import GraphItem from './items/GraphItem';
import ImageItem from './items/ImageItem';
import MarkdownItem from './items/MarkdownItem';
import RecordItem from './items/RecordItem';
import TableItem from './items/TableItem';
import WorksheetItem from './items/WorksheetItem';
import ItemWrapper from './items/ItemWrapper';
import NewUpload from './NewUpload/NewUpload';

////////////////////////////////////////////////////////////

// Create a worksheet item based on props and add it to worksheet_items.
// - item: information about the table to display
// - index: integer representing the index in the list of items
// - focused: whether this item has the focus
// - canEdit: whether we're allowed to edit this item
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

    props.key = props.ref = 'item' + props.focusIndex;
    props.url = url;
    props.prevItem = prevItem;

    var constructor = {
        markup_block: MarkdownItem,
        table_block: TableItem,
        contents_block: ContentsItem,
        subworksheets_block: WorksheetItem,
        record_block: RecordItem,
        image_block: ImageItem,
        graph_block: GraphItem,
    }[item.mode];

    var elem;
    if (constructor) {
        elem = React.createElement(constructor, props);
    } else {
        elem = React.createElement(
            'div',
            null,
            React.createElement('strong', null, 'Internal error: ', item.mode),
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
            showNewRun={props.focusedForButtons && props.showNewRun}
            showNewText={props.focusedForButtons && props.showNewText}
            onHideNewUpload={props.onHideNewUpload}
            onHideNewRun={props.onHideNewRun}
            onHideNewText={props.onHideNewText}
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
        if (!info || !info.items.length) {
            $('.empty-worksheet').fadeIn('fast');
        }
    }

    capture_keys() {
        // Move focus to the top
        Mousetrap.bind(
            ['g g'],
            function() {
                $('body')
                    .stop(true)
                    .animate({ scrollTop: 0 }, 'fast');
                this.props.setFocus(0, 0);
            }.bind(this),
            'keydown',
        );

        // Move focus to the bottom
        Mousetrap.bind(
            ['shift+g'],
            function() {
                this.props.setFocus(this.props.ws.info.items.length - 1, 'end');
                $('html, body').animate({ scrollTop: $(document).height() }, 'fast');
            }.bind(this),
            'keydown',
        );
    }

    bundleUuidToIndex() {
        // bundle uuid -> an array of [index, subIndex], corresponding to positions where the bundle occurs
        // E.g. 0x47bda9 -> [[0, 1], [2, 3]], which means bundle 0x47bda9 appears twice in the current worksheet
        var uuidToIndex = {};
        var info = this.props.ws.info;
        if (info && info.items.length > 0) {
            var items = info.items;
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

    handleContextMenuSelection = (uuid, focusIndex, subFocusIndex, option) => {
        var type = option[0];
        var args = option[1];
        args.push(uuid);
        if (type === ContextMenuEnum.command.ADD_BUNDLE_TO_HOMEWORKSHEET) {
            args.push('/');
        } else if (type === ContextMenuEnum.command.DETACH_BUNDLE) {
            var uuidToIndex = this.bundleUuidToIndex();
            if (uuidToIndex[uuid].length > 1) {
                // if a bundle appears more than once in the current worksheet, take the last one
                for (var i = uuidToIndex[uuid].length - 1; i >= 0; i--) {
                    var indices = uuidToIndex[uuid][i];
                    if (indices[0] === focusIndex && indices[1] === subFocusIndex) break;
                }
                // index counting from the end
                args.push('-n', uuidToIndex[uuid].length - i);
            }
        }
        $('#command_line')
            .terminal()
            .exec(buildTerminalCommand(args));
    };

    handleContextMenu = (uuid, focusIndex, subFocusIndex, isRunBundle, e) => {
        e.preventDefault();
        this.props.setFocus(focusIndex, subFocusIndex, false);
        var bundleType = isRunBundle ? ContextMenuEnum.type.RUN : ContextMenuEnum.type.BUNDLE;
        ContextMenuMixin.openContextMenu(
            bundleType,
            this.handleContextMenuSelection.bind(undefined, uuid, focusIndex, subFocusIndex),
        );
    };

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
        if (info && info.items.length === 0) {
            // Create a "dummy" item at the beginning so that only empty text can be added.
            info.items = [
                {
                    isDummyItem: true,
                    text: '',
                    mode: 'markup_block',
                    sort_keys: [-1],
                    ids: [null],
                    is_refined: true,
                },
            ];
        }
        let focusedForButtonsItem;
        if (info && info.items.length > 0) {
            var worksheet_items = [];
            info.items.forEach(
                function(item, index) {
                    const focused = index === this.props.focusIndex;

                    // focusedForButtons determines whether clicking on Cell/Upload/Run will
                    // apply to this cell. If nothing is focused (focusIndex = -1),
                    // append to the end by default.
                    const focusedForButtons =
                        focused ||
                        (this.props.focusIndex === -1 && index === info.items.length - 1);

                    if (focusedForButtons) {
                        focusedForButtonsItem = item;
                    }
                    var props = {
                        worksheetUUID: info.uuid,
                        item: item,
                        version: this.props.version,
                        active: this.props.active,
                        focused,
                        focusedForButtons,
                        canEdit: this.props.canEdit,
                        focusIndex: index,
                        subFocusIndex: focused ? this.props.subFocusIndex : null,
                        setFocus: this.props.setFocus,
                        focusActionBar: this.props.focusActionBar,
                        openWorksheet: this.props.openWorksheet,
                        handleContextMenu: this.handleContextMenu,
                        reloadWorksheet: this.props.reloadWorksheet,
                        ws: this.props.ws,
                        showNewRun: this.props.showNewRun,
                        showNewText: this.props.showNewText,
                        showNewRerun: this.props.showNewRerun,
                        onHideNewUpload: this.props.onHideNewUpload,
                        onHideNewRun: this.props.onHideNewRun,
                        onHideNewText: this.props.onHideNewText,
                        onHideNewRerun: this.props.onHideNewRerun,
                        handleCheckBundle: this.props.handleCheckBundle,
                        confirmBundleRowAction: this.props.confirmBundleRowAction,
                        setDeleteItemCallback: this.props.setDeleteItemCallback,
                        editPermission: info && info.edit_permission,
                    };
                    addWorksheetItems(
                        props,
                        worksheet_items,
                        index > 0 ? info.items[index - 1] : null,
                        index < info.items.length - 1 ? info.items[index + 1] : null,
                    );
                }.bind(this),
            );
            items_display = (
                <>
                    {worksheet_items}
                    <NewUpload
                        key={this.state.newUploadKey}
                        after_sort_key={(getMinMaxKeys(focusedForButtonsItem) || {}).maxKey}
                        worksheetUUID={info.uuid}
                        reloadWorksheet={this.props.reloadWorksheet}
                        // Reset newUploadKey so that NewUpload gets re-rendered. This way,
                        // it is possible to upload the same file multiple times in a row
                        // (otherwise, chrome will not call onchange on a file input when
                        // the file hasn't changed)
                        onUploadFinish={(e) => this.setState({ newUploadKey: Math.random() + '' })}
                    />
                </>
            );
        } else {
            items_display = null;
        }
        if (info && info.error)
            items_display = <p className='alert-danger'>Error in worksheet: {info.error}</p>;
        return (
            <div id='worksheet_items' onClick={this.handleClickForDeselect}>
                {items_display}
            </div>
        );
    }
}

export default WorksheetItemList;
