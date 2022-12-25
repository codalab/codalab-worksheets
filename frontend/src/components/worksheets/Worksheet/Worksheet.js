import * as React from 'react';
import $ from 'jquery';
import _ from 'underscore';
import { withStyles } from '@material-ui/core/styles';
import {
    renderPermissions,
    getAfterSortKey,
    createAlertText,
    addUTCTimeZone,
} from '../../../util/worksheet_utils';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import WorksheetItemList from '../WorksheetItemList';
import InformationModal from '../InformationModal/InformationModal';
import WorksheetHeader from './WorksheetHeader';
import {
    NAVBAR_HEIGHT,
    HEADER_HEIGHT,
    WORKSHEET_HEADER_HEIGHT,
    FOOTER_HEIGHT,
    EXPANDED_WORKSHEET_WIDTH,
    NARROW_WORKSHEET_WIDTH,
    LOCAL_STORAGE_WORKSHEET_WIDTH,
    DIALOG_TYPES,
    AUTO_HIDDEN_DURATION,
    FINAL_BUNDLE_STATES,
} from '../../../constants';
import WorksheetTerminal from '../WorksheetTerminal';
import Loading from '../../Loading';
import Button from '@material-ui/core/Button';
import EditIcon from '@material-ui/icons/EditOutlined';
import SaveIcon from '@material-ui/icons/SaveOutlined';
import DeleteIcon from '@material-ui/icons/DeleteOutline';
import UndoIcon from '@material-ui/icons/UndoOutlined';
import ContractIcon from '@material-ui/icons/ExpandLessOutlined';
import ExpandIcon from '@material-ui/icons/ExpandMoreOutlined';
import './Worksheet.scss';
import ErrorMessage from '../ErrorMessage';
import BundleDetail from '../BundleDetail';
import { buildTerminalCommand } from '../../../util/worksheet_utils';
import {
    addItems,
    apiWrapper,
    defaultErrorHandler,
    executeCommand,
    getUser,
} from '../../../util/apiWrapper';
import Tooltip from '@material-ui/core/Tooltip';
import WorksheetDialogs from '../WorksheetDialogs';
import { ToastContainer, toast, Zoom } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import { Popover } from '@material-ui/core';

/*
Information about the current worksheet and its items.
*/

// TODO: dummy objects
let ace = window.ace;
toast.configure();

class Worksheet extends React.Component {
    constructor(props) {
        super(props);
        let localWorksheetWidthPreference = window.localStorage.getItem(
            LOCAL_STORAGE_WORKSHEET_WIDTH,
        );
        const { uuid, bundle_uuid } = this.props.match.params;
        this.state = {
            ws: {
                uuid,
                info: null,
            },
            bundleIsOpen: !!bundle_uuid,
            openBundleUUID: bundle_uuid,
            openBundleAfterSortKey: null,
            version: 0, // Increment when we refresh
            escCount: 0, // Increment when the user presses esc keyboard shortcut, a hack to allow esc shortcut to work
            activeComponent: 'itemList', // Where the focus is (terminal, itemList)
            inSourceEditMode: false, // Whether we're editing the worksheet
            editorEnabled: false, // Whether the editor is actually showing (sometimes lags behind inSourceEditMode)
            showTerminal: false, // Whether the terminal is shown
            focusIndex: -1, // Which worksheet items to be on (-1 is none)
            subFocusIndex: 0, // For tables, which row in the table
            numOfBundles: -1, // Number of bundles in this worksheet (-1 is just the initial value)
            focusedBundleUuidList: [], // Uuid of the focused bundle and that of all bundles after it
            userInfo: null, // User info of the current user. (null is the default)
            updatingBundleUuids: {},
            isUpdatingBundles: false,
            anchorEl: null,
            showNewRun: false,
            showNewText: false,
            showNewSchema: false,
            uploadAnchor: null,
            showRerun: false,
            isValid: true,
            checkedBundles: {},
            BulkBundleDialog: null,
            showBundleOperationButtons: false,
            uuidBundlesCheckedCount: {},
            openedDialog: null,
            errorDialogMessage: '',
            forceDelete: false,
            showInformationModal: false,
            deleteWorksheetConfirmation: false,
            deleteItemCallback: null,
            copiedBundleIds: '',
            showPasteButton: window.localStorage.getItem('CopiedBundles') !== '',
            worksheetWidthPercentage: localWorksheetWidthPreference || EXPANDED_WORKSHEET_WIDTH,
            messagePopover: {
                showMessage: false,
                messageContent: null,
            },
            showUpdateProgress: false,
            showWorksheetContent: false,
            executingCommand: false,
        };
        this.copyCallbacks = [];
        this.showContentCallbacks = [];
        this.bundleTableID = new Set();

        // Throttle so that if keys are held down, we don't suffer a huge lag.
        this.scrollToItem = _.throttle(this.__innerScrollToItem, 50).bind(this);
    }

    fetch(props) {
        // Set defaults
        props = props || {};
        props.success = props.success || function(data) {};
        props.error = props.error || function(xhr, status, err) {};
        if (props.async === undefined) {
            props.async = true;
        }
        const queryParams = {
            brief: props.brief ? 1 : 0,
        };

        return apiWrapper
            .fetchWorksheet(this.state.ws.uuid, queryParams)
            .then((info) => {
                info['date_created'] = addUTCTimeZone(info['date_created']);
                info['date_last_modified'] = addUTCTimeZone(info['date_last_modified']);
                this.setState({
                    ws: {
                        ...this.state.ws,
                        info,
                    },
                });
                props.success(info);
            })
            .catch((error) => props.error(error));
    }

    saveWorksheet(props) {
        if (this.state.ws.info === undefined) return;
        this.setState({ showUpdateProgress: true });
        props = props || {};
        props.success = props.success || function(data) {};
        props.error = props.error || function(xhr, status, err) {};
        apiWrapper
            .saveWorksheet(this.state.ws.uuid, this.state.ws.info.source.join('\n'))
            .then((data) => {
                console.log('Saved worksheet ' + this.state.ws.uuid);
                props.success(data);
            })
            .catch((error) => {
                props.error(error);
            });
    }

    deleteWorksheet(props) {
        if (this.state.ws.info === undefined) return;
        this.setState({ showUpdateProgress: true });
        apiWrapper
            .deleteWorksheet(this.state.ws.info.uuid)
            .then((data) => {
                console.log('Deleted worksheet ' + this.state.ws.info.uuid);
                props.success && props.success(data);
            })
            .catch((error) => props.error && props.error(error));
    }

    _setfocusIndex(index) {
        this.setState({ focusIndex: index });
    }
    _setWorksheetSubFocusIndex(index) {
        this.setState({ subFocusIndex: index });
    }

    // Return the number of rows occupied by this item.
    _numTableRows(item) {
        if (item) {
            if (item.mode === 'table_block') return item.bundles_spec.bundle_infos.length;
            if (item.mode === 'subworksheets_block') return item.subworksheet_infos.length;
        } else {
            return null;
        }
    }

    _createWsItemsIdDict(info) {
        let wsItemsIdDict = {};
        for (let i = 0; i < info.blocks.length; i++) {
            if (info.blocks[i].bundles_spec) {
                for (let j = 0; j < (this._numTableRows(info.blocks[i]) || 1); j++) {
                    wsItemsIdDict[info.blocks[i].bundles_spec.bundle_infos[j].uuid] = [i, j];
                }
            } else if (info.blocks[i].ids) {
                wsItemsIdDict[info.blocks[i].ids[0]] = [i, 0];
            }
        }
        return wsItemsIdDict;
    }

    handleClickForDeselect = (event) => {
        //Deselecting when clicking outside worksheet_items component
        if (event.target === event.currentTarget) {
            this.setFocus(-1, 0, false);
        }
    };

    // BULK OPERATION RELATED CODE BELOW======================================
    handleCheckBundle = (uuid, identifier, check, removeCheckAfterOperation) => {
        // This is a callback function that will be passed all the way down to bundle row
        // This is to allow bulk operations on bundles
        // It passes through Worksheet->WorksheetItemList->TableItem->BundleRow
        // When a bundle row is selected, it calls this function to notify Worksheet component
        // The uuid & a unique identifier is recorded so we can uncheck the tables after operation
        // The essential part of the below code is to record uuid when a bundle is clicked
        // and remove a uuid a bundle is unclicked
        // Because there a bundle may appear in multiple tables and get checked multiple times
        // Only remove a uuid if all checked rows are removed

        // TODO: This function should be cleaner, after my logic refactoring, the identifier
        //      shouldn't be necessary. However, if we want more control on what happens after
        //      bulk operation, this might be useful
        let bundlesCount = this.state.uuidBundlesCheckedCount;
        document.activeElement.blur();
        if (check) {
            //A bundle is checked
            if (
                uuid in this.state.checkedBundles &&
                identifier in this.state.checkedBundles[uuid]
            ) {
                return;
            }
            bundlesCount[uuid] = 1;
            let checkedBundles = this.state.checkedBundles;
            if (!(uuid in checkedBundles)) {
                checkedBundles[uuid] = {};
            }
            checkedBundles[uuid][identifier] = removeCheckAfterOperation;
            this.setState({
                checkedBundles: checkedBundles,
                uuidBundlesCheckedCount: bundlesCount,
                showBundleOperationButtons: true,
            });
            // return localIndex
        } else {
            // A bundle is unchecked
            if (
                !(
                    uuid in this.state.uuidBundlesCheckedCount &&
                    identifier in this.state.checkedBundles[uuid]
                )
            ) {
                return;
            }
            if (this.state.uuidBundlesCheckedCount[uuid] === 1) {
                delete this.state.uuidBundlesCheckedCount[uuid];
                delete this.state.checkedBundles[uuid];
            } else {
                bundlesCount[uuid] -= 1;
                this.setState({
                    uuidBundlesCheckedCount: bundlesCount,
                });
                delete this.state.checkedBundles[uuid][identifier];
            }
            if (Object.keys(this.state.uuidBundlesCheckedCount).length === 0) {
                this.setState({
                    uuidBundlesCheckedCount: {},
                    checkedBundles: {},
                    showBundleOperationButtons: false,
                });
            }
        }
    };

    handleSelectedBundleCommand = (cmd, worksheet_uuid = this.state.ws.uuid) => {
        // This function runs the command for bulk bundle operations
        // The uuid are recorded by handleCheckBundle
        // Refreshes the checkbox after commands
        // If the action failed, the check will persist
        let force_delete = cmd === 'rm' && this.state.forceDelete ? '--force' : null;
        this.setState({
            updating: true,
            executingCommand: true,
        });
        const bundleCount: number = Object.keys(this.state.uuidBundlesCheckedCount).length;
        // This toast info is used for showing a message when a command is being performed
        const toastId = toast.info(getToastMsg(cmd, 0, bundleCount), {
            position: 'top-right',
            hideProgressBar: false,
            closeOnClick: true,
            pauseOnHover: false,
            draggable: true,
            autoClose: false,
        });
        executeCommand(
            buildTerminalCommand([
                cmd,
                force_delete,
                ...Object.keys(this.state.uuidBundlesCheckedCount),
            ]),
            worksheet_uuid,
        )
            .then(() => {
                const fromDeleteCommand = cmd === 'rm';
                this.clearCheckedBundles(() => {
                    // This toast info is used for showing a message when a command has finished executing
                    toast.update(toastId, {
                        render: getToastMsg(cmd, 1, bundleCount),
                        type: toast.TYPE.SUCCESS,
                        position: 'top-right',
                        autoClose: 2000,
                        hideProgressBar: true,
                        closeOnClick: true,
                        pauseOnHover: false,
                        draggable: true,
                    });
                });
                this.setState({ executingCommand: false });
                this.reloadWorksheet(undefined, undefined, { fromDeleteCommand });
            })
            .catch((e) => {
                toast.dismiss();
                this.setState({
                    openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
                    errorDialogMessage: e,
                    forceDelete: false,
                    updating: false,
                    executingCommand: false,
                });
            });
    };

    handleForceDelete = (e) => {
        this.setState({ forceDelete: e.target.checked });
    };

    toggleBundleBulkMessageDialog = () => {
        this.setState({ BulkBundleDialog: null });
    };

    toggleErrorMessageDialog = () => {
        this.setState({ openedDialog: null, errorDialogMessage: '' });
    };

    onError = (message) => {
        this.setState({
            openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
            errorDialogMessage: message,
        });
    };

    executeBundleCommand = (cmd_type) => () => {
        this.handleSelectedBundleCommand(cmd_type);
        this.toggleCmdDialogNoEvent(cmd_type);
    };

    executeBundleCommandNoEvent = (cmd_type) => {
        this.handleSelectedBundleCommand(cmd_type);
        this.toggleCmdDialogNoEvent(cmd_type);
    };

    addCopyBundleRowsCallback = (tableID, callback) => {
        this.copyCallbacks[tableID] = callback;
    };

    addShowContentBundleRowsCallback = (tableID, callback) => {
        this.showContentCallbacks[tableID] = callback;
    };

    // Helper functions to deal with commands
    toggleCmdDialog = (cmd_type) => () => {
        this.handleCommand(cmd_type);
    };

    toggleCmdDialogNoEvent = (cmd_type) => {
        this.handleCommand(cmd_type);
    };

    handleCommand = (cmd_type) => {
        if (this.state.openedDialog) {
            this.setState({ openedDialog: null, deleteItemCallback: null });
            return;
        }
        if (cmd_type === 'deleteMarkdown') {
            // This is used to delete markdown blocks
            this.setState({ openedDialog: DIALOG_TYPES.OPEN_DELETE_MARKDOWN });
        }
        if (cmd_type === 'deleteSchema') {
            // This is used to delete schema
            this.setState({ openedDialog: DIALOG_TYPES.OPEN_DELETE_SCHEMA });
        }
        if (cmd_type === 'rm') {
            this.setState({ openedDialog: DIALOG_TYPES.OPEN_DELETE_BUNDLE });
        } else if (cmd_type === 'kill') {
            this.setState({ openedDialog: DIALOG_TYPES.OPEN_KILL });
        } else if (cmd_type === 'copy' || cmd_type === 'cut') {
            let validBundles = [];
            let actualCopiedCounts = 0;
            let tableIDs = Object.keys(this.copyCallbacks).sort();
            tableIDs.forEach((tableID) => {
                let copyBundleCallback = this.copyCallbacks[tableID];
                let bundlesChecked = copyBundleCallback();
                bundlesChecked.forEach((bundle) => {
                    if (bundle.name === '<invalid>') {
                        return;
                    }
                    validBundles.push(bundle);
                    actualCopiedCounts += 1;
                });
            });
            // Removes the last new line
            window.localStorage.setItem('CopiedBundles', JSON.stringify(validBundles));
            if (validBundles.length > 0) {
                this.setState({ showPasteButton: true });
            }
            const copycut = cmd_type === 'cut' ? 'Cut ' : 'Copied ';
            const toastString =
                actualCopiedCounts > 0
                    ? copycut + actualCopiedCounts + ' bundle' + (actualCopiedCounts > 1 ? 's' : '')
                    : 'No bundle(s) selected';
            if (cmd_type === 'cut') {
                // Remove the bundle lines
                this.removeItemsFromSource(validBundles.map((e) => e.id));
            }
            this.clearCheckedBundles(() => {
                toast.info(toastString, {
                    position: 'top-right',
                    autoClose: 1300,
                    hideProgressBar: true,
                    closeOnClick: true,
                    pauseOnHover: false,
                    draggable: true,
                });
            });
        } else if (cmd_type === 'paste') {
            this.pasteBundlesToWorksheet();
        }
    };

    removeItemsFromSource = (itemIds) => {
        let worksheetUUID = this.state.ws.uuid;
        const url = `/rest/worksheets/${worksheetUUID}/add-items`;
        const callback = () => {
            const textDeleted = true;
            const param = { textDeleted };
            this.setState({ deleting: false });
            this.reloadWorksheet(undefined, undefined, param);
        };
        const errorHandler = (error) => {
            this.setState({ deleting: false });
            alert(createAlertText(url, error));
        };
        addItems(worksheetUUID, { ids: itemIds })
            .then(callback)
            .catch(errorHandler);
    };

    moveFocusToBottom = () => {
        window.scrollTo(0, document.body.scrollHeight);
        this.setFocus(this.state.ws.info.blocks.length - 1, 'end');
    };

    confirmBundleRowAction = (code) => {
        if (!(this.state.openedDialog || this.state.BulkBundleDialog)) {
            // no dialog is opened, open bundle row detail
            return false;
        } else if (code === 'KeyX' || code === 'Space') {
            return true;
        } else if (this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_BUNDLE) {
            this.executeBundleCommandNoEvent('rm');
        } else if (this.state.openedDialog === DIALOG_TYPES.OPEN_KILL) {
            this.executeBundleCommandNoEvent('kill');
        }
        return true;
    };
    // BULK OPERATION RELATED CODE ABOVE======================================
    setDeleteItemCallback = (callback) => {
        this.setState({
            deleteItemCallback: callback,
            openedDialog: DIALOG_TYPES.OPEN_DELETE_MARKDOWN,
        });
    };

    pasteBundlesToWorksheet = () => {
        // Unchecks all bundles after pasting
        const data = JSON.parse(window.localStorage.getItem('CopiedBundles'));
        let items = [];
        data.forEach((bundle) => {
            items.push(bundle.uuid);
        });
        // remove the last new line character
        let worksheetUUID = this.state.ws.uuid;
        let after_sort_key;
        if (this.state.focusIndex !== -1 && this.state.focusIndex !== undefined) {
            let currentFocusedBlock = this.state.ws.info.blocks[this.state.focusIndex];
            console.log(this.state.subFocusIndex, currentFocusedBlock);
            after_sort_key = getAfterSortKey(currentFocusedBlock, this.state.subFocusIndex);
        }
        let url = `/rest/worksheets/${worksheetUUID}/add-items`;
        let actualData = { items };
        if (after_sort_key) {
            actualData['after_sort_key'] = after_sort_key;
        } else {
            // If no location for the insertion is specified,
            // insert the new item at the top of the worksheet by default.
            actualData['after_sort_key'] = 0;
        }
        actualData['item_type'] = 'bundle';
        const callback = () => {
            const moveIndex = true;
            const param = { moveIndex };
            this.reloadWorksheet(undefined, undefined, param);
        };
        const errorHandler = (error) => {
            alert(createAlertText(url, error));
        };
        addItems(worksheetUUID, actualData)
            .then(callback)
            .catch(errorHandler);
    };

    clearCheckedBundles = (clear_callback) => {
        // Clear the checks
        Object.keys(this.state.checkedBundles).forEach((uuid) => {
            if (this.state.checkedBundles[uuid] !== undefined) {
                Object.keys(this.state.checkedBundles[uuid]).forEach((identifier) => {
                    if (
                        this.state.checkedBundles[uuid] &&
                        this.state.checkedBundles[uuid][identifier] !== undefined
                    ) {
                        this.state.checkedBundles[uuid][identifier]();
                    }
                });
            }
        });

        this.setState(
            {
                uuidBundlesCheckedCount: {},
                checkedBundles: {},
                showBundleOperationButtons: false,
                updating: false,
                forceDelete: false,
            },
            clear_callback,
        );
        this.bundleTableID = new Set();
        this.copyCallbacks = {};
        this.showContentCallbacks = {};
    };

    onAsyncItemLoad = (focusIndex, item) => {
        this.setState({
            ws: {
                ...this.state.ws,
                info: {
                    ...this.state.ws.info,
                    // immutably change item at index *focusIndex*
                    blocks: Object.assign([], this.state.ws.info.blocks, { [focusIndex]: item }),
                },
            },
        });
    };

    updateBundleBlockSchema = (newSchemaName, mode, firstBundleSourceIndex) => {
        // newSchemaName: new schema name to be used
        // mode: table or record mode
        // firstBundleSourceIndex: the source index of the first bundle of the block
        //     It should be 1 for the following example
        //                  0 % display table abc
        //                  1 []{0x1234}
        let source = this.state.ws.info.source;
        let newDisplay = '% display ' + mode + ' ' + newSchemaName;
        // First check if the line above firstBundleSourceIndex is a % display directive.
        // If it is, we replace that line with % display <mode> <schema name>
        // otherwise we add % display <mode> <schema name> above the firstBundleSourceIndex
        let lineAbove = firstBundleSourceIndex > 0 ? source[firstBundleSourceIndex - 1] : '';
        let updatedSource = [];
        if (lineAbove.startsWith('% display')) {
            // replace the line above
            Object.assign(updatedSource, source, { [firstBundleSourceIndex - 1]: newDisplay });
        } else {
            // insert above firstBundleSourceIndex
            Object.assign(updatedSource, source.slice(0, firstBundleSourceIndex));
            updatedSource.push(newDisplay);
            updatedSource.push(...source.slice(firstBundleSourceIndex));
        }
        this.setState(
            {
                ws: {
                    ...this.state.ws,
                    info: {
                        ...this.state.ws.info,
                        source: updatedSource,
                    },
                },
            },
            this.saveAndUpdateWorksheet,
        );
    };

    updateSchemaItem = (rows, ids, after_sort_key, create, deletion) => {
        // rows: list of string representing the new schema:
        //      % schema example
        //      % add uuid uuid [0:8]
        // ids: ids of the row items
        // after_sort_key: used for add-items
        let worksheetUUID = this.state.ws.uuid;
        let url = `/rest/worksheets/${worksheetUUID}/add-items`;
        let actualData = { items: rows };
        actualData['item_type'] = 'directive';
        if (!create) actualData['ids'] = ids;
        actualData['after_sort_key'] = after_sort_key;
        const callback = () => {
            if (deletion) {
                const textDeleted = true;
                const param = { textDeleted };
                if (this.state.openedDialog) {
                    this.setState({ openedDialog: null, deleteItemCallback: null });
                }
                this.reloadWorksheet(undefined, undefined, param);
            } else {
                const moveIndex = true;
                const param = { moveIndex };
                this.reloadWorksheet(undefined, undefined, param);
            }
        };
        const errorHandler = (error) => {
            alert(createAlertText(url, error));
        };
        addItems(worksheetUUID, actualData)
            .then(callback)
            .catch(errorHandler);
        this.setState({
            messagePopover: {
                showMessage: true,
                messageContent: deletion ? 'Schema Deleted!' : 'Schema Saved!',
            },
        });
        this.autoHideOpenedMessagePopover();
    };

    setDeleteSchemaItemCallback = (callback) => {
        this.setState({
            deleteItemCallback: callback,
            openedDialog: DIALOG_TYPES.OPEN_DELETE_SCHEMA,
        });
    };

    autoHideOpenedMessagePopover() {
        const self = this;
        if (this.timer) {
            clearTimeout(this.timer);
        }
        this.timer = setTimeout(() => {
            self.setState({
                messagePopover: {
                    showMessage: false,
                    messageContent: null,
                },
            });
        }, AUTO_HIDDEN_DURATION);
    }

    setFocus = (index, subIndex, shouldScroll = true) => {
        let info = this.state.ws.info;

        // prevent multiple clicking from resetting the index
        if (index === this.state.focusIndex && subIndex === this.state.subFocusIndex) {
            return;
        }

        // Make sure that the screen doesn't scroll when the user normally press j / k,
        // until the target element is completely not on the screen
        function isOnScreen(element) {
            if (element.offset() === undefined) return false;
            let elementOffsetTop = element.offset().top;
            let elementHeight = element.height();
            let screenScrollTop = $(window).scrollTop();
            let screenHeight = $(window).height();
            // HEADER_HEIGHT is the sum of height of WorksheetHeader and NavBar.
            // Since they block the user's view, we should take them into account when calculating whether the item is on the screen or not.
            let scrollIsAboveElement =
                elementOffsetTop + elementHeight - screenScrollTop - HEADER_HEIGHT >= 0;
            let elementIsVisibleOnScreen = screenScrollTop + screenHeight - elementOffsetTop >= 0;
            return scrollIsAboveElement && elementIsVisibleOnScreen;
        }
        if (shouldScroll) {
            const element =
                $(`#codalab-worksheet-item-${index}-subitem-${subIndex}`)[0] === undefined
                    ? $(`#codalab-worksheet-item-${index}`)
                    : $(`#codalab-worksheet-item-${index}-subitem-${subIndex}`);

            shouldScroll = !isOnScreen(element);
        }

        // resolve to the last item that contains bundle(s)
        if (index === 'end') {
            index = -1;
            for (var i = info.blocks.length - 1; i >= 0; i--) {
                if (info.blocks[i].bundles_spec) {
                    index = i;
                    break;
                }
            }
        }
        // resolve to the last row of the selected item
        if (subIndex === 'end') {
            subIndex = (this._numTableRows(info.blocks[index]) || 1) - 1;
        }

        let focusedBundleUuidList = [];
        if (index !== -1) {
            // index !== -1 means something is selected.
            // focusedBundleUuidList is a list of uuids of all bundles and ids of all other items after the selected bundle (itself included)
            // Say the selected bundle has focusIndex 1 and subFocusIndex 2, then focusedBundleUuidList will include the uuids of
            // all the bundles that have focusIndex 1 and subFocusIndex >= 2, and also all the bundles that have focusIndex > 1
            for (let i = index; i < info.blocks.length; i++) {
                if (info.blocks[i].bundles_spec) {
                    var j = i === index ? subIndex : 0;
                    for (; j < (this._numTableRows(info.blocks[i]) || 1); j++) {
                        focusedBundleUuidList.push(
                            info.blocks[i].bundles_spec.bundle_infos[j].uuid,
                        );
                    }
                } else {
                    focusedBundleUuidList = focusedBundleUuidList.concat(info.blocks[i].ids);
                }
            }
        }

        // If we met a out of bound, we default it to the last item
        // A protection mechanism to avoid possible error
        if (index >= info.blocks.length) {
            index = info.blocks.length - 1;
            subIndex = 0;
        }

        // Change the focus - triggers updating of all descendants.
        this.setState({
            focusIndex: index,
            subFocusIndex: subIndex,
            focusedBundleUuidList: focusedBundleUuidList,
            showNewRun: false,
            showNewText: false,
            showNewSchema: false,
            uploadAnchor: null,
            showNewRerun: false,
        });
        if (shouldScroll) {
            this.scrollToItem(index, subIndex);
        }
    };

    __innerScrollToItem = (index, subIndex) => {
        let node;
        if (index !== -1) {
            node = document.getElementById(`codalab-worksheet-item-${index}-subitem-${subIndex}`)
                ? document.getElementById(`codalab-worksheet-item-${index}-subitem-${subIndex}`)
                : document.getElementById(`codalab-worksheet-item-${index}`);
        }
        if (node) {
            node.scrollIntoView({ block: 'center' });
        }
    };

    componentDidMount() {
        this.fetch({
            brief: true,
            success: function(data) {
                this.setState({
                    updating: false,
                    version: this.state.version + 1,
                    numOfBundles: this.getNumOfBundles(),
                    showWorksheetContent: true,
                });
                // Fix out of bounds.
            }.bind(this),
            error: function(xhr, status, err) {
                this.setState({
                    openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
                    errorDialogMessage: xhr.responseText,
                    isValid: false,
                });
            }.bind(this),
        });

        // Initialize history stack
        window.history.replaceState({ uuid: this.state.ws.uuid }, '', window.location.pathname);
        $('body').addClass('ws-interface');
        const callback = (data) => {
            var userInfo = data.data.attributes;
            userInfo.user_id = data.data.id;
            this.setState({
                userInfo: userInfo,
            });
        };
        getUser()
            .then(callback)
            .catch(defaultErrorHandler);
    }

    hasEditPermission() {
        var info = this.state.ws.info;
        return info && info.edit_permission;
    }

    handleTerminalFocus = (event) => {
        this.setState({ activeComponent: 'terminal' });
        // just scroll to the top of the page.
        // Add the stop() to keep animation events from building up in the queue
        $('#command_line').data('resizing', null);
        $('body')
            .stop(true)
            .animate({ scrollTop: 0 }, 250);
    };
    handleTerminalBlur = (event) => {
        // explicitly close terminal because we're leaving the terminal
        // $('#command_line').terminal().focus(false);
        this.setState({ activeComponent: 'itemList' });
        $('#command_line').data('resizing', null);
    };
    toggleInformationModal = () => {
        this.setState({ showInformationModal: !this.state.showInformationModal });
    };
    toggleWorksheetSize = () => {
        let newPercentage =
            this.state.worksheetWidthPercentage === NARROW_WORKSHEET_WIDTH
                ? EXPANDED_WORKSHEET_WIDTH
                : NARROW_WORKSHEET_WIDTH;
        window.localStorage.setItem(LOCAL_STORAGE_WORKSHEET_WIDTH, newPercentage);
        this.setState({ worksheetWidthPercentage: newPercentage });
    };
    setupEventHandlers() {
        // Load worksheet from history when back/forward buttons are used.
        let editPermission = this.state.ws.info && this.state.ws.info.edit_permission;

        window.onpopstate = function(event) {
            if (event.state === null) return;
            this.setState({
                ws: {
                    uuid: event.state.uuid,
                    info: null,
                },
            });
            this.reloadWorksheet();
        }.bind(this);

        if (this.state.activeComponent === 'terminal') {
            // no need for other keys, we have the terminal focused
            return;
        }

        if (!this.state.ws.info) {
            // disable all keyboard shortcuts when loading worksheet
            return;
        }
        if (!(this.state.openedDialog || this.state.BulkBundleDialog)) {
            // Only enable these shortcuts when no dialog is opened
            Mousetrap.bind(['shift+r'], () => {
                this.reloadWorksheet(undefined, undefined, undefined, () => {
                    toast.info('🦄 Worksheet refreshed!', {
                        position: 'top-right',
                        hideProgressBar: true,
                        autoClose: 1500,
                        closeOnClick: true,
                        pauseOnHover: false,
                        draggable: true,
                    });
                });
            });

            // Show/hide web terminal
            Mousetrap.bind(
                ['shift+c'],
                function(e) {
                    this.toggleTerminal();
                }.bind(this),
            );

            // Focus on web terminal
            Mousetrap.bind(
                ['c c'],
                function(e) {
                    this.focusTerminal();
                }.bind(this),
            );

            // Open source edit mode
            Mousetrap.bind(
                ['shift+e'],
                function(e) {
                    this.toggleSourceEditMode();
                    return false;
                }.bind(this),
            );

            // Focus on search
            Mousetrap.bind(['a+f'], function(e) {
                document.getElementById('codalab-search-bar').focus();
                return false; //prevent keypress to bubble
            });

            Mousetrap.bind(
                ['up', 'k'],
                function(e) {
                    e.preventDefault(); // Prevent automatic scrolling from up/down arrow keys
                    var focusIndex = this.state.focusIndex;
                    var subFocusIndex = this.state.subFocusIndex;
                    var wsItems = this.state.ws.info.blocks;
                    if (focusIndex === 0 && subFocusIndex === 0) {
                        // Deselect all item when selecting up above the first item.
                        this.setFocus(-1, 0);
                    } else if (
                        focusIndex >= 0 &&
                        (wsItems[focusIndex].mode === 'table_block' ||
                            wsItems[focusIndex].mode === 'subworksheets_block')
                    ) {
                        if (subFocusIndex - 1 < 0) {
                            // focusIndex must > 0
                            this.setFocus(focusIndex - 1, 'end'); // Move out of this table to the item above the current table
                        } else {
                            this.setFocus(focusIndex, subFocusIndex - 1);
                        }
                    } else if (focusIndex > 0) {
                        // worksheet_items.jsx
                        this.setFocus(focusIndex - 1, 'end');
                    }
                }.bind(this),
            );

            Mousetrap.bind(
                ['down', 'j'],
                function(e) {
                    e.preventDefault(); // Prevent automatic scrolling from up/down arrow keys
                    var focusIndex = this.state.focusIndex;
                    var subFocusIndex = this.state.subFocusIndex;
                    var wsItems = this.state.ws.info.blocks;
                    if (
                        focusIndex >= 0 &&
                        (wsItems[focusIndex].mode === 'table_block' ||
                            wsItems[focusIndex].mode === 'subworksheets_block')
                    ) {
                        if (
                            focusIndex < wsItems.length - 1 &&
                            subFocusIndex + 1 >= this._numTableRows(wsItems[focusIndex])
                        ) {
                            this.setFocus(focusIndex + 1, 0);
                        } else if (subFocusIndex + 1 < this._numTableRows(wsItems[focusIndex])) {
                            this.setFocus(focusIndex, subFocusIndex + 1);
                        }
                    } else {
                        if (focusIndex < wsItems.length - 1) this.setFocus(focusIndex + 1, 0);
                    }
                }.bind(this),
            );
            if (!this.state.showBundleOperationButtons && editPermission) {
                // insert text after current cell
                Mousetrap.bind(
                    ['a t'],
                    function(e) {
                        // if no active focus, scroll to the bottom position
                        if (this.state.focusIndex < 0) {
                            $('html, body').animate({ scrollTop: 0 }, 'fast');
                        }
                        this.setState({ showNewText: true });
                    }.bind(this),
                    'keyup',
                );

                // upload after current cell
                Mousetrap.bind(
                    ['a u'],
                    function(e) {
                        // if no active focus, scroll to the bottom position
                        if (this.state.focusIndex < 0) {
                            $('html, body').animate({ scrollTop: 0 }, 'fast');
                        }
                        document.querySelector('#upload-button').click();
                    }.bind(this),
                    'keyup',
                );
                // run after current cell
                Mousetrap.bind(
                    ['a r'],
                    function(e) {
                        // if no active focus, scroll to the bottom position
                        if (this.state.focusIndex < 0) {
                            $('html, body').animate({ scrollTop: 0 }, 'fast');
                        }
                        this.setState({ showNewRun: true });
                    }.bind(this),
                    'keyup',
                );
                // edit and rerun current bundle
                Mousetrap.bind(
                    ['a n'],
                    function(e) {
                        if (this.state.focusIndex < 0) return;
                        this.setState({ showNewRerun: true });
                    }.bind(this),
                    'keyup',
                );
            }
        }
        Mousetrap.bind(['escape'], this.handleBackButtonClick);
        Mousetrap.bind(['?'], (e) => {
            this.setState({
                showInformationModal: true,
            });
        });
        Mousetrap.bind(['+'], (e) => {
            this.toggleWorksheetSize();
        });

        if (
            this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_MARKDOWN ||
            this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_SCHEMA
        ) {
            Mousetrap.bind(
                ['enter'],
                function(e) {
                    e.preventDefault();
                    if (this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_MARKDOWN) {
                        this.state.deleteItemCallback();
                        this.toggleCmdDialogNoEvent('deleteMarkdown');
                    } else if (this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_SCHEMA) {
                        this.state.deleteItemCallback();
                        this.toggleCmdDialogNoEvent('deleteSchema');
                    }
                }.bind(this),
            );
        }
        // paste after current focused cell
        if (this.state.ws.info.edit_permission) {
            Mousetrap.bind(
                ['a v'],
                function(e) {
                    this.pasteBundlesToWorksheet();
                }.bind(this),
                'keyup',
            );
        }

        if (this.state.showBundleOperationButtons) {
            // Below are allowed shortcut even when a dialog is opened===================
            // The following three are bulk bundle operation shortcuts
            Mousetrap.bind(['a c'], () => {
                if (this.state.openedDialog) {
                    return;
                }
                this.toggleCmdDialogNoEvent('copy');
                Mousetrap.reset();
            });
            if (this.state.ws.info.edit_permission) {
                Mousetrap.bind(['backspace', 'del'], () => {
                    if (
                        this.state.openedDialog &&
                        this.state.openedDialog !== DIALOG_TYPES.OPEN_DELETE_BUNDLE
                    ) {
                        return;
                    }
                    this.toggleCmdDialogNoEvent('rm');
                });
                Mousetrap.bind(['a k'], () => {
                    if (
                        this.state.openedDialog &&
                        this.state.openedDialog !== DIALOG_TYPES.OPEN_KILL
                    ) {
                        return;
                    }
                    this.toggleCmdDialogNoEvent('kill');
                    Mousetrap.reset();
                });
                Mousetrap.bind(['a d'], () => {
                    if (this.state.openedDialog) {
                        return;
                    }
                    this.toggleCmdDialogNoEvent('cut');
                    Mousetrap.reset();
                });

                // Confirm bulk bundle operation
                if (this.state.openedDialog) {
                    Mousetrap.bind(
                        ['enter'],
                        function(e) {
                            if (this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_BUNDLE) {
                                this.executeBundleCommandNoEvent('rm');
                            } else if (this.state.openedDialog === DIALOG_TYPES.OPEN_KILL) {
                                this.executeBundleCommandNoEvent('kill');
                            }
                        }.bind(this),
                    );

                    // Select/Deselect to force delete during deletion dialog
                    Mousetrap.bind(
                        ['f'],
                        function() {
                            //force deletion through f
                            if (this.state.openedDialog === DIALOG_TYPES.OPEN_DELETE_BUNDLE) {
                                this.setState({ forceDelete: !this.state.forceDelete });
                            }
                        }.bind(this),
                    );
                }
            }
        }
        //====================Bulk bundle operations===================
    }

    toggleSourceEditMode(openSourceEditMode, saveChanges) {
        // openSourceEditMode: whether to open or close source mode, open if true, close if false, toggles to opposite if undefined
        // saveChanges: whether to save or discard changes
        if (openSourceEditMode === undefined) openSourceEditMode = !this.state.inSourceEditMode; // toggle by default

        if (saveChanges === undefined) saveChanges = true;

        if (!openSourceEditMode) {
            // Going out of raw mode - save the worksheet.
            if (this.hasEditPermission()) {
                var editor = ace.edit('worksheet-editor');
                if (saveChanges) {
                    // Use callback function to ensure the worksheet info will not be sent to the backend until the frontend state has finished updating
                    this.setState(
                        {
                            ws: {
                                ...this.state.ws,
                                info: {
                                    ...this.state.ws.info,
                                    source: editor.getValue().split('\n'),
                                },
                            },
                        },
                        () => {
                            var rawIndex = editor.getCursorPosition().row;
                            this.setState({
                                inSourceEditMode: false,
                                editorEnabled: false,
                            }); // Needs to be after getting the raw contents
                            this.saveAndUpdateWorksheet(saveChanges, rawIndex);
                        },
                    );
                } else {
                    var rawIndex = editor.getCursorPosition().row;
                    this.setState({
                        inSourceEditMode: false,
                        editorEnabled: false,
                    });
                    this.reloadWorksheet(undefined, rawIndex);
                }
            } else {
                // Not allowed to edit the worksheet.
                this.setState({
                    inSourceEditMode: false,
                    editorEnabled: false,
                });
            }
        } else {
            // Go into edit mode.
            this.setState({ inSourceEditMode: true });
            this.clearCheckedBundles();
            $('#worksheet-editor').focus(); // Needs to be before focusing
        }
    }

    // updateRunBundles fetch all the "unfinished" bundles in the worksheet, and recursively call itself until all the bundles in the worksheet are finished.
    updateRunBundles = (worksheetUuid, numTrials, updatingBundleUuids) => {
        var bundleUuids = updatingBundleUuids
            ? updatingBundleUuids
            : this.state.updatingBundleUuids;
        var startTime = new Date().getTime();
        var self = this;
        var queryParams = Object.keys(bundleUuids)
            .map(function(bundle_uuid) {
                return 'bundle_uuid=' + bundle_uuid;
            })
            .join('&');
        const url = '/rest/interpret/worksheet/' + worksheetUuid + '?' + queryParams;
        apiWrapper
            .get(url)
            .then((worksheet_content) => {
                if (this.state.isUpdatingBundles && worksheet_content.uuid === this.state.ws.uuid) {
                    if (worksheet_content.blocks) {
                        self.reloadWorksheet(worksheet_content.blocks);
                    }
                    var endTime = new Date().getTime();
                    var guaranteedDelayTime = Math.min(3000, numTrials * 1000);
                    // Since we don't want to flood the server with too many requests, we enforce a guaranteedDelayTime.
                    // guaranteedDelayTime is usually 3 seconds, except that we make the first two delays 1 second and 2 seconds respectively in case of really quick jobs.
                    // delayTime is also at least five times the amount of time it takes for the last request to complete
                    var delayTime = Math.max(guaranteedDelayTime, (endTime - startTime) * 5);
                    setTimeout(function() {
                        self.updateRunBundles(worksheetUuid, numTrials + 1);
                    }, delayTime);
                    startTime = endTime;
                }
            })
            .catch((e) => {
                console.error(e);
                this.setState({
                    openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
                    errorDialogMessage: 'Failed to update run bundles.',
                    isValid: false,
                });
            });
    };

    // Everytime the worksheet is updated, checkRunBundle will loop through all the bundles and find the "unfinished" ones (not ready or failed).
    // If there are unfinished bundles and we are not updating bundles now, call updateRunBundles, which will recursively call itself until all the bundles in the worksheet are finished.
    // this.state.updatingBundleUuids keeps track of the "unfinished" bundles in the worksheet at every moment.
    checkRunBundle(info) {
        var updatingBundleUuids = _.clone(this.state.updatingBundleUuids);
        if (info && info.blocks.length > 0) {
            var items = info.blocks;
            for (var i = 0; i < items.length; i++) {
                if (items[i].bundles_spec) {
                    for (var j = 0; j < items[i].bundles_spec.bundle_infos.length; j++) {
                        var bundle_info = items[i].bundles_spec.bundle_infos[j];
                        const inFinalState = FINAL_BUNDLE_STATES.includes(bundle_info.state);
                        if (!inFinalState) {
                            updatingBundleUuids[bundle_info.uuid] = true;
                        } else {
                            if (bundle_info.uuid in updatingBundleUuids)
                                delete updatingBundleUuids[bundle_info.uuid];
                        }
                    }
                }
            }
            if (Object.keys(updatingBundleUuids).length > 0 && !this.state.isUpdatingBundles) {
                this.setState({ isUpdatingBundles: true });
                this.updateRunBundles(info.uuid, 1, updatingBundleUuids);
            } else if (
                Object.keys(updatingBundleUuids).length === 0 &&
                this.state.isUpdatingBundles
            ) {
                this.setState({ isUpdatingBundles: false });
            }
            this.setState({ updatingBundleUuids: updatingBundleUuids });
        }
    }

    componentDidUpdate(prevProps, prevState) {
        if (this.state.inSourceEditMode && !this.state.editorEnabled) {
            this.setState({ editorEnabled: true });
            var editor = ace.edit('worksheet-editor');
            editor.$blockScrolling = Infinity;
            editor.session.setUseWrapMode(false);
            editor.setShowPrintMargin(false);
            editor.session.setMode('ace/mode/markdown', function() {
                editor.session.$mode.blockComment = { start: '//', end: '' };
            });
            if (!this.hasEditPermission()) {
                editor.setOptions({
                    readOnly: true,
                    highlightActiveLine: false,
                    highlightGutterLine: false,
                });
                editor.renderer.$cursorLayer.element.style.opacity = 0;
            } else {
                editor.commands.addCommand({
                    name: 'save',
                    bindKey: { win: 'Ctrl-Enter', mac: 'Ctrl-Enter' },
                    exec: function() {
                        this.toggleSourceEditMode();
                    }.bind(this),
                    readOnly: true,
                });
                editor.commands.addCommand({
                    name: 'exit',
                    bindKey: { win: 'Esc', mac: 'Esc' },
                    exec: function() {
                        // discard changes in source mode
                        this.toggleSourceEditMode(false, false);
                    }.bind(this),
                });
                editor.focus();

                var rawIndex;
                var cursorColumnPosition;
                if (this.state.focusIndex === -1) {
                    // Above the first item
                    rawIndex = 0;
                    cursorColumnPosition = 0;
                } else {
                    var item = this.state.ws.info.blocks[this.state.focusIndex];
                    // For non-tables such as search and wsearch, we have subFocusIndex, but not backed by raw items, so use 0.
                    var focusIndexPair =
                        this.state.focusIndex +
                        ',' +
                        (item.mode === 'table_block' || item.mode === 'subworksheets_block'
                            ? this.state.subFocusIndex
                            : 0);
                    rawIndex = this.state.ws.info.block_to_raw[focusIndexPair];
                }

                // When clicking "Edit Source" from one of the rows in a search results block, go to the line of the corresponding search directive.
                if (rawIndex === undefined) {
                    focusIndexPair = [this.state.focusIndex, 0].join(',');
                    rawIndex = this.state.ws.info.block_to_raw[focusIndexPair];
                }

                if (rawIndex === undefined) {
                    console.error(
                        "Can't map %s (focusIndex %d, subFocusIndex %d) to raw index",
                        focusIndexPair,
                        this.state.focusIndex,
                        this.state.subFocusIndex,
                    );
                    return;
                }
                if (cursorColumnPosition === undefined)
                    cursorColumnPosition = editor.session.getLine(rawIndex).length; // End of line
                editor.gotoLine(rawIndex + 1, cursorColumnPosition);
                editor.renderer.scrollToRow(rawIndex);
            }
        }
        if (prevState.showTerminal !== this.state.showTerminal) {
            // Hack to make sure that the <Sticky> component in WorksheetHeader.js updates.
            // This is needed because otherwise the header doesn't move up or down as needed
            // when the terminal is shown / hidden.
            window.scrollTo(window.scrollX, window.scrollY + 1);
        }
    }

    toggleTerminal() {
        this.setState({ showTerminal: !this.state.showTerminal });
    }

    focusTerminal() {
        this.setState({ activeComponent: 'terminal' });
        this.setState({ showTerminal: true });
        $('#command_line')
            .terminal()
            .focus();
    }

    ensureIsArray(bundle_info) {
        if (!bundle_info) return null;
        if (!Array.isArray(bundle_info)) {
            bundle_info = [bundle_info];
        }
        return bundle_info;
    }

    getNumOfBundles() {
        var items = this.state.ws.info && this.state.ws.info.blocks;
        if (!items) return 0;
        var count = 0;
        for (var i = 0; i < items.length; i++) {
            if (items[i].bundles_spec) {
                count += items[i].bundles_spec.bundle_infos.length;
            }
        }
        return count;
    }

    // If partialUpdateItems is undefined, we will fetch the whole worksheet.
    // Otherwise, partialUpdateItems is a list of item parallel to ws.info.blocks that contain only items that need updating.
    // More specifically, all items that don't contain run bundles that need updating are null.
    // Also, a non-null item could contain a list of bundle_infos, which represent a list of bundles. Usually not all of them need updating.
    // The bundle_infos for bundles that don't need updating are also null.
    // If rawIndexAfterEditMode is defined, this reloadWorksheet is called right after toggling editMode. It should resolve rawIndex to (focusIndex, subFocusIndex) pair.
    reloadWorksheet = (
        partialUpdateItems,
        rawIndexAfterEditMode,
        {
            moveIndex = false,
            textDeleted = false,
            fromDeleteCommand = false,
            uploadFiles = false,
            addImage = false, // whether the reload is caused by adding an image
        } = {},
        afterReload,
    ) => {
        // Don't reload if we're in the middle of executing a command.
        // The worksheet will get reloaded once the command has executed.
        if (this.state.executingCommand) {
            return;
        }

        if (partialUpdateItems === undefined) {
            this.setState({ updating: true, showUpdateProgress: true });
            this.fetch({
                brief: true,
                success: function(data) {
                    if (this.state.ws.uuid !== data.uuid) {
                        this.setState({
                            updating: false,
                            version: this.state.version + 1,
                        });
                        return false;
                    }
                    this.setState({ showUpdateProgress: false, showWorksheetContent: true });
                    var items = this.state.ws.info.blocks;
                    var numOfBundles = this.getNumOfBundles();
                    var focus = this.state.focusIndex;
                    if (rawIndexAfterEditMode !== undefined) {
                        var focusIndexPair = this.state.ws.info.raw_to_block[rawIndexAfterEditMode];
                        if (focusIndexPair === undefined) {
                            console.error(
                                "Can't map raw index " +
                                    rawIndexAfterEditMode +
                                    ' to item index pair',
                            );
                            focusIndexPair = [0, 0]; // Fall back to default
                        }

                        if (focusIndexPair === null) {
                            // happens in the case of an empty worksheet
                            this.setFocus(-1, 0);
                        } else {
                            this.setFocus(focusIndexPair[0], focusIndexPair[1]);
                        }
                    } else if (
                        (this.state.numOfBundles !== -1 &&
                            numOfBundles > this.state.numOfBundles) ||
                        uploadFiles
                    ) {
                        // If the number of bundles increases then the focus should be on the new bundle.
                        // if the current focus is not on a table
                        if (
                            items[focus] &&
                            items[focus].mode &&
                            items[focus].mode !== 'table_block'
                        ) {
                            this.setFocus(focus >= 0 ? focus + 1 : 'end', 0);
                        } else if (this.state.focusIndex === -1) {
                            // If currently the top of the worksheet is focused and a new bundle has been uploaded to the worksheet,
                            // the first item on the worksheet should be focused.
                            this.setFocus(0, 0);
                        } else if (this.state.subFocusIndex !== undefined) {
                            // Focus on the next bundle row
                            this.setFocus(focus >= 0 ? focus : 'end', this.state.subFocusIndex + 1);
                        } else {
                            this.setFocus(focus >= 0 ? focus : 'end', 'end');
                        }
                    } else if (numOfBundles < this.state.numOfBundles) {
                        // Bundles are deleted
                        // When delete something, cursor should be after the thing that was deleted
                        // The method also works when deleting multiple (non-consecutive) bundles.
                        if (focus === -1) {
                            // No focus has been set
                            // Move focus to the virtual item
                            this.setFocus(focus, 0);
                        } else {
                            // Move focus to the next available item
                            // First, create an id list containing the ids of all the items on the current page
                            const wsItemsIdDict = this._createWsItemsIdDict(this.state.ws.info);
                            let hasSetFocus = false;
                            for (let k = 0; k < this.state.focusedBundleUuidList.length; k++) {
                                // Find the first current available item by searching for its id
                                if (this.state.focusedBundleUuidList[k] in wsItemsIdDict) {
                                    this.setFocus(
                                        wsItemsIdDict[this.state.focusedBundleUuidList[k]][0],
                                        wsItemsIdDict[this.state.focusedBundleUuidList[k]][1],
                                    );
                                    hasSetFocus = true;
                                    break;
                                }
                            }
                            // If all the following items of the previous focused item have been delete
                            // Move focus to the last item on the page.
                            if (!hasSetFocus) {
                                this.setFocus(items.length - 1, 'end');
                            }
                        }
                    } else {
                        if (moveIndex) {
                            // for adding a new cell, we want the focus to be the one below the current focus
                            this.setFocus(focus >= 0 ? focus + 1 : items.length - 1, 0);
                        }
                        if (textDeleted) {
                            // When deleting text, we want the focus to stay at the same index,
                            // unless it is the last item in the worksheet, at which point the
                            // focus goes to the last item in the worksheet.
                            this.setFocus(items.length === focus ? items.length - 1 : focus, 0);
                        }
                        if (fromDeleteCommand) {
                            // Executed 'rm' command but no bundle deleted
                            // So bundles are deleted through a search directive
                            this.setFocus(focus, this.state.subFocusIndex);
                        }
                    }
                    this.setState(
                        {
                            updating: false,
                            version: this.state.version + 1,
                            numOfBundles: numOfBundles,
                        },
                        () => {
                            if (addImage) {
                                const subFocusIndex = this.state.subFocusIndex || 0;
                                let focusIndexPair = this.state.focusIndex + ',' + subFocusIndex;
                                let index = this.state.ws.info.block_to_raw[focusIndexPair];
                                if (index === undefined) {
                                    // the newly uploaded image currently does not create a new separate block (e.g. be added as a bundle row to a table)
                                    // retry to get the raw index
                                    focusIndexPair =
                                        this.state.focusIndex - 1 + ',' + (subFocusIndex + 1);
                                    index = this.state.ws.info.block_to_raw[focusIndexPair];
                                }
                                // index is the raw index of the new uploaded image
                                this.addImageDisplay(index);
                            }
                        },
                    );
                    this.checkRunBundle(this.state.ws.info);
                }.bind(this),
                error: function(xhr, status, err) {
                    this.setState({
                        updating: false,
                        openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
                        errorDialogMessage: xhr.responseText,
                    });
                    this.setState({ showUpdateProgress: false, isValid: false });
                }.bind(this),
            }).then(() => {
                afterReload && afterReload();
            });
        } else {
            var ws = _.clone(this.state.ws);
            for (var i = 0; i < partialUpdateItems.length; i++) {
                if (
                    !partialUpdateItems[i] ||
                    !(
                        'bundles_spec' in partialUpdateItems[i] &&
                        'bundle_infos' in partialUpdateItems[i]['bundles_spec']
                    )
                ) {
                    // Partial Update mechanism only designs for the blocks consisting of bundles
                    // Check whether the block contains the field of 'bundle_infos' to determine whether it is a non-None block containing a list of bundle_infos, which represent a list of bundles
                    continue;
                }
                // update interpreted items
                for (
                    let j = 0;
                    j < partialUpdateItems[i]['bundles_spec']['bundle_infos'].length;
                    j++
                ) {
                    if (partialUpdateItems[i]['bundles_spec']['bundle_infos'][j]) {
                        ws.info.blocks[i]['bundles_spec']['bundle_infos'][j] =
                            partialUpdateItems[i]['bundles_spec']['bundle_infos'][j];
                        ws.info.blocks[i]['rows'][j] = partialUpdateItems[i]['rows'][j];
                    }
                }
            }
            this.setState({ ws: ws, version: this.state.version + 1 });
            this.checkRunBundle(ws.info);
        }
    };

    openWorksheet = (uuid) => {
        // Change to a different worksheet. This does not call reloadWorksheet().
        this.setState({
            ws: {
                uuid,
                info: null,
            },
        });

        // Note: this is redundant if we're doing 'cl work' from the terminal,
        // but is necessary if triggered in other ways.
        this.reloadWorksheet();

        // Create a new entry in the browser history with new URL.
        window.history.pushState({ uuid: this.state.ws.uuid }, '', '/worksheets/' + uuid + '/');
    };

    openBundle = (bundleUUID, afterSortKey) => {
        const wsUUID = this.state.ws.uuid;
        const url = `/worksheets/${wsUUID}/${bundleUUID}`;

        window.history.pushState(
            {
                uuid: wsUUID,
                bundle_uuid: bundleUUID,
            },
            '',
            url,
        );

        this.setState({
            openBundleUUID: bundleUUID,
            openBundleAfterSortKey: afterSortKey,
            bundleIsOpen: true,
            showBundleOperationButtons: false,
        });
    };

    closeBundle = () => {
        const wsUUID = this.state.ws.uuid;
        const url = `/worksheets/${wsUUID}`;

        window.history.pushState({ uuid: wsUUID }, '', url);

        this.reloadWorksheet();
        this.setState({
            openBundleUUID: null,
            openBundleAfterSortKey: null,
            bundleIsOpen: false,
        });
    };

    handleBackButtonClick = () => {
        if (this.state.bundleIsOpen) {
            this.closeBundle();
            return;
        }
        this.props.history.push('/users'); // back to "My Worksheets"
    };

    saveAndUpdateWorksheet = (fromRaw, rawIndex) => {
        this.saveWorksheet({
            success: function(data) {
                this.setState({ updating: false });
                this.reloadWorksheet(undefined, rawIndex);
            }.bind(this),
            error: function() {
                this.setState({ updating: false, showUpdateProgress: false });
                this.setState({
                    openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
                    errorDialogMessage:
                        'Failed to save and update the worksheet, please check the syntax and try again.',
                });
                if (fromRaw) {
                    this.toggleSourceEditMode(true);
                }
            }.bind(this),
        });
    };

    deleteWorksheetAction = () => {
        this.deleteWorksheet({
            success: function(data) {
                this.setState({ updating: false });
                window.location = '/users';
            }.bind(this),
            error: function(xhr, status, err) {
                this.setState({ updating: false, showUpdateProgress: false });
                this.setState({
                    openedDialog: DIALOG_TYPES.OPEN_ERROR_DIALOG,
                    errorDialogMessage: xhr.responseText,
                });
            }.bind(this),
        });
    };

    showUploadMenu = (e) => {
        // pause mousetrap events such as up, down, and enter
        Mousetrap.pause();
        let form = document.querySelector('#upload-menu');

        Mousetrap(form).bind(['enter'], function(e) {
            e.stopImmediatePropagation();
            e.preventDefault();
            document.querySelector('label[for=' + e.target.firstElementChild.htmlFor + ']').click();
            Mousetrap(form).unbind(['enter']);
        });

        this.setState({ uploadAnchor: e.currentTarget });
    };

    /**
     * @param index index of new image's source line
     */
    addImageDisplay = (index: number) => {
        // add %display line to the worksheet source right before the newly uploaded image
        this.setState(
            (prevState) => {
                const items = ['% display image / width=250'];
                return {
                    ws: {
                        ...prevState.ws,
                        info: {
                            ...prevState.ws.info,
                            source: [
                                ...prevState.ws.info.source.slice(0, index),
                                ...items,
                                ...prevState.ws.info.source.slice(index),
                            ],
                        },
                    },
                };
            },
            () => {
                // since one line has been added before the newly uploaded image, now the rawIndex should be added by 1
                this.saveAndUpdateWorksheet(false, index + 1);
            },
        );
    };

    showBundleContent = () => {
        this.setState({ openedDialog: DIALOG_TYPES.OPEN_CREATE_CONTENT });
    };
    showBundleContentCallback = (path) => () => {
        // Default path for content block is '/'
        path = path || '/';
        let validBundles = [];
        let showContentCounts = 0;
        let tableIDs = Object.keys(this.showContentCallbacks).sort();
        tableIDs.forEach((tableID) => {
            let showContentBundleCallback = this.showContentCallbacks[tableID];
            let bundlesChecked = showContentBundleCallback();
            bundlesChecked.forEach((bundle) => {
                if (bundle.name === '<invalid>') {
                    return;
                }
                validBundles.push(bundle.rawIndex);
                showContentCounts += 1;
            });
        });

        let newSource = [...this.state.ws.info.source];

        validBundles.forEach((index, i) => {
            newSource.splice(
                index + 3 * i + 1,
                0,
                '% display contents ' + path,
                newSource[index],
                '',
            );
        });
        const toastString =
            showContentCounts > 0
                ? `Show contents for ${showContentCounts} bundle...` +
                  (showContentCounts > 1 ? 's' : '')
                : 'No bundle(s) selected';
        this.clearCheckedBundles(() => {
            toast.info(toastString, {
                position: 'top-right',
                autoClose: 1300,
                hideProgressBar: true,
                closeOnClick: true,
                pauseOnHover: false,
                draggable: true,
            });
        });
        this.setState(
            (prevState) => {
                return {
                    ws: {
                        ...prevState.ws,
                        info: {
                            ...prevState.ws.info,
                            source: newSource,
                        },
                    },
                    openedDialog: null,
                };
            },
            () => {
                this.saveAndUpdateWorksheet(false, validBundles ? validBundles[0] + 1 : undefined);
            },
        );
    };

    render() {
        const { classes } = this.props;
        const {
            anchorEl,
            uploadAnchor,
            showUpdateProgress,
            showInformationModal,
            showWorksheetContent,
            bundleIsOpen,
            openBundleUUID,
            openBundleAfterSortKey,
        } = this.state;

        this.setupEventHandlers();
        let info = this.state.ws.info;
        let rawWorksheet = info && info.source.join('\n');
        const editPermission = this.hasEditPermission();
        let editableClassName = editPermission && this.state.openSourceEditMode ? 'editable' : '';
        let disableWorksheetEditing = editPermission ? '' : 'disabled';
        let sourceStr = editPermission ? 'Edit Source' : 'View Source';

        let blockViewButtons = (
            <div style={{ display: 'inline-block' }}>
                <Button
                    onClick={() => {
                        this.toggleSourceEditMode(true);
                    }}
                    size='small'
                    color='inherit'
                    aria-label='Edit Source'
                    disabled={!info || bundleIsOpen}
                >
                    <EditIcon className={classes.buttonIcon} />
                    {sourceStr}
                </Button>
                <Button
                    onClick={(e) => this.toggleTerminal()}
                    size='small'
                    color='inherit'
                    aria-label='Expand CLI'
                    id='terminal-button'
                    disabled={!info || bundleIsOpen}
                >
                    {this.state.showTerminal ? (
                        <ContractIcon className={classes.buttonIcon} />
                    ) : (
                        <ExpandIcon className={classes.buttonIcon} />
                    )}
                    {this.state.showTerminal ? 'HIDE TERMINAL' : 'SHOW TERMINAL'}
                </Button>
                <Button
                    onClick={(e) =>
                        this.setState({ openedDialog: DIALOG_TYPES.OPEN_DELETE_WORKSHEET })
                    }
                    size='small'
                    color='inherit'
                    aria-label='Delete Worksheet'
                    disabled={!editPermission || bundleIsOpen}
                >
                    <Tooltip
                        disableFocusListener
                        disableTouchListener
                        title='Delete this worksheet.'
                    >
                        <DeleteIcon />
                    </Tooltip>
                </Button>
            </div>
        );

        let sourceModeButtons = (
            <div
                onMouseMove={(ev) => {
                    ev.stopPropagation();
                }}
                style={{ display: 'inline-block' }}
            >
                <Button
                    onClick={() => {
                        // save changes and exit source mode
                        this.toggleSourceEditMode(false, true);
                    }}
                    disabled={disableWorksheetEditing}
                    size='small'
                    color='inherit'
                    aria-label='Save Edit'
                >
                    <SaveIcon className={classes.buttonIcon} />
                    Save
                </Button>
                <Button
                    onClick={() => {
                        // discard changes
                        this.toggleSourceEditMode(false, false);
                    }}
                    size='small'
                    color='inherit'
                    aria-label='Discard Edit'
                >
                    <UndoIcon className={classes.buttonIcon} />
                    Discard
                </Button>
            </div>
        );

        if (info && info.blocks.length) {
            // Non-empty worksheet
        } else {
            $('.empty-worksheet').fadeIn();
        }

        let rawDisplay = (
            <div className={classes.rawDisplayContainer}>
                <div id='worksheet-editor'>{rawWorksheet}</div>
                <div className={classes.rawDisplayInfo}>
                    Press ctrl-enter to save. See{' '}
                    <a
                        target='_blank'
                        rel='noopener noreferrer'
                        href='https://codalab-worksheets.readthedocs.io/en/latest/Worksheet-Markdown'
                    >
                        markdown syntax
                    </a>
                    .
                </div>
            </div>
        );

        let itemsDisplay = (
            <WorksheetItemList
                active={this.state.activeComponent === 'itemList'}
                ws={this.state.ws}
                version={this.state.version}
                focusIndex={this.state.focusIndex}
                subFocusIndex={this.state.subFocusIndex}
                setFocus={this.setFocus}
                reloadWorksheet={this.reloadWorksheet}
                saveAndUpdateWorksheet={this.saveAndUpdateWorksheet}
                openWorksheet={this.openWorksheet}
                openBundle={this.openBundle}
                focusTerminal={this.focusTerminal}
                ensureIsArray={this.ensureIsArray}
                showNewRun={this.state.showNewRun}
                showNewText={this.state.showNewText}
                showNewRerun={this.state.showNewRerun}
                showNewSchema={this.state.showNewSchema}
                onError={this.onError}
                onHideNewRun={() => this.setState({ showNewRun: false })}
                onHideNewText={() => this.setState({ showNewText: false })}
                onHideNewRerun={() => this.setState({ showNewRerun: false })}
                onHideNewSchema={() => this.setState({ showNewSchema: false })}
                handleCheckBundle={this.handleCheckBundle}
                confirmBundleRowAction={this.confirmBundleRowAction}
                setDeleteItemCallback={this.setDeleteItemCallback}
                addCopyBundleRowsCallback={this.addCopyBundleRowsCallback}
                addShowContentBundleRowsCallback={this.addShowContentBundleRowsCallback}
                onAsyncItemLoad={this.onAsyncItemLoad}
                updateBundleBlockSchema={this.updateBundleBlockSchema}
                updateSchemaItem={this.updateSchemaItem}
                setDeleteSchemaItemCallback={this.setDeleteSchemaItemCallback}
                addImageDisplay={this.addImageDisplay}
            />
        );

        let worksheetDisplay = this.state.inSourceEditMode ? rawDisplay : itemsDisplay;
        let editButtons = this.state.inSourceEditMode ? sourceModeButtons : blockViewButtons;

        if (info && info.title) {
            document.title = info.title;
        }

        if (!this.state.isValid) {
            return <ErrorMessage message={"Not found: '/worksheets/" + this.state.ws.uuid + "'"} />;
        }

        return (
            <React.Fragment>
                <WorksheetHeader
                    showTerminal={this.state.showTerminal}
                    editPermission={editPermission}
                    info={info}
                    classes={classes}
                    renderPermissions={renderPermissions}
                    reloadWorksheet={this.reloadWorksheet}
                    editButtons={editButtons}
                    anchorEl={anchorEl}
                    setAnchorEl={(e) => this.setState({ anchorEl: e })}
                    onShowNewRun={() => this.setState({ showNewRun: true })}
                    onShowNewText={() => this.setState({ showNewText: true })}
                    onShowNewSchema={() => this.setState({ showNewSchema: true })}
                    onBackButtonClick={this.handleBackButtonClick}
                    bundleIsOpen={bundleIsOpen}
                    uploadAnchor={uploadAnchor}
                    showUploadMenu={this.showUploadMenu}
                    closeUploadMenu={() => {
                        this.setState({ uploadAnchor: null });
                        Mousetrap.unpause();
                    }}
                    handleSelectedBundleCommand={this.handleSelectedBundleCommand}
                    showBundleOperationButtons={this.state.showBundleOperationButtons}
                    toggleCmdDialog={this.toggleCmdDialog}
                    toggleInformationModal={this.toggleInformationModal}
                    toggleCmdDialogNoEvent={this.toggleCmdDialogNoEvent}
                    onError={this.onError}
                    copiedBundleIds={this.state.copiedBundleIds}
                    showPasteButton={this.state.showPasteButton}
                    toggleWorksheetSize={this.toggleWorksheetSize}
                    showBundleContent={this.showBundleContent}
                />
                <WorksheetTerminal
                    ws={this.state.ws}
                    handleFocus={this.handleTerminalFocus}
                    handleBlur={this.handleTerminalBlur}
                    active={this.state.activeComponent === 'terminal'}
                    reloadWorksheet={this.reloadWorksheet}
                    openWorksheet={this.openWorksheet}
                    editMode={() => {
                        this.toggleSourceEditMode(true);
                    }}
                    setFocus={this.setFocus}
                    hidden={!this.state.showTerminal}
                />
                {openBundleUUID && (
                    <div className={classes.bundleContainer}>
                        <BundleDetail
                            uuid={openBundleUUID}
                            wsUUID={this.state.ws.info?.uuid}
                            after_sort_key={openBundleAfterSortKey}
                            editPermission={editPermission}
                            onUpdate={() => {}}
                            contentExpanded
                            sidebarExpanded
                            fullMinHeight
                        />
                    </div>
                )}
                <div className={classes.worksheetContainer}>
                    {!info && (
                        <div className={classes.loaderContainer}>
                            <Loading />
                        </div>
                    )}
                    {showWorksheetContent && (
                        <div
                            id='worksheet_content'
                            className={editableClassName + ' worksheet_content'}
                        >
                            {worksheetDisplay}
                            {/* Show error dialog if bulk bundle execution failed*/}
                            {this.state.BulkBundleDialog}
                        </div>
                    )}
                </div>
                <Button
                    className={classes.scrollToBottom}
                    onClick={this.moveFocusToBottom}
                    color='primary'
                    variant='contained'
                >
                    <ExpandMoreIcon size='medium' />
                </Button>
                <WorksheetDialogs
                    openedDialog={this.state.openedDialog}
                    closeDialog={() => {
                        this.setState({ openedDialog: null });
                    }}
                    errorDialogMessage={this.state.errorDialogMessage}
                    toggleCmdDialog={this.toggleCmdDialog}
                    toggleCmdDialogNoEvent={this.toggleCmdDialogNoEvent}
                    toggleErrorMessageDialog={this.toggleErrorMessageDialog}
                    deleteWorksheetAction={this.deleteWorksheetAction}
                    executeBundleCommand={this.executeBundleCommand}
                    forceDelete={this.state.forceDelete}
                    handleForceDelete={this.handleForceDelete}
                    deleteItemCallback={this.state.deleteItemCallback}
                    showBundleContentCallback={this.showBundleContentCallback}
                />
                <InformationModal
                    showUpdateProgress={showUpdateProgress}
                    showInformationModal={showInformationModal}
                    toggleInformationModal={this.toggleInformationModal}
                />
                <Popover
                    open={this.state.messagePopover.showMessage}
                    anchorEl={anchorEl}
                    anchorOrigin={{
                        vertical: 'bottom',
                        horizontal: 'center',
                    }}
                    transformOrigin={{
                        vertical: 'top',
                        horizontal: 'center',
                    }}
                    classes={{ paper: classes.noTransform }}
                >
                    <div style={{ padding: 10, backgroundColor: '#D9ECDB', color: '#537853' }}>
                        {this.state.messagePopover.messageContent}
                    </div>
                </Popover>
                <ToastContainer
                    newestOnTop={false}
                    transition={Zoom}
                    rtl={false}
                    pauseOnVisibilityChange
                />
            </React.Fragment>
        );
    }
}

const containerHeight = `calc(100vh - ${NAVBAR_HEIGHT}px - ${WORKSHEET_HEADER_HEIGHT}px - ${FOOTER_HEIGHT}px)`;

const styles = (theme) => ({
    bundleContainer: {
        height: containerHeight,
        overflowY: 'scroll',
        zIndex: 7, // in front of worksheet container
        position: 'fixed',
        backgroundColor: 'white',
        width: '100%',
    },
    worksheetContainer: {
        height: containerHeight,
        overflowY: 'scroll',
        zIndex: 5,
        backgroundColor: 'white',
        width: '100%',
    },
    scrollToBottom: {
        zIndex: 6,
        borderRadius: '400px',
        position: 'fixed',
        bottom: '50px',
        right: '30px',
        backgroundColor: '00BFFF',
    },
    uuid: {
        fontFamily: theme.typography.fontFamilyMonospace,
        fontSize: 10,
        textAlign: 'right',
    },
    label: {
        paddingRight: theme.spacing.unit,
        fontWeight: 500,
    },
    bottomButtons: {
        paddingTop: theme.spacing.largest,
        paddingBottom: theme.spacing.largest,
    },
    permissions: {
        cursor: 'pointer',
        '&:hover': {
            backgroundColor: theme.color.primary.lightest,
        },
    },
    noTransform: {
        transform: 'none !important',
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
    loaderContainer: {
        paddingTop: 35,
    },
    rawDisplayContainer: {
        width: '100%',
    },
    rawDisplayInfo: {
        padding: 10,
    },
});

Mousetrap.stopCallback = function(e, element, combo) {
    //if the element is a checkbox, don't stop
    if (element.type === 'checkbox') {
        return false;
    }
    // if the element has the class "mousetrap" then no need to stop
    if ((' ' + element.className + ' ').indexOf(' mousetrap ') > -1) {
        return false;
    }

    // stop for input, select, and textarea
    return (
        element.tagName === 'INPUT' ||
        element.tagName === 'SELECT' ||
        element.tagName === 'TEXTAREA' ||
        (element.contentEditable && element.contentEditable === 'true')
    );
};

export const getToastMsg = (command, state, count) => {
    // Creates a toast message for a given command.
    // count is the number of bundles on which this command was performed, if applicable.
    // state can take the value of 0 or 1
    // 0 represents the command is being executed
    // 1 represents the command has already been executed
    const cmdMsgMap = { rm: ['Deleting', 'deleted'] };
    const bundleCount = count + (count === 1 ? ' bundle' : ' bundles');
    const cmdMsg = cmdMsgMap[command] ?? ['Executing', 'executed'];
    let toastMsg;
    if (command in cmdMsgMap) {
        // We want the toasts to be "Deleting x bundles..." and "x bundles deleted!"
        toastMsg =
            state === 0 ? cmdMsg[state] + ' ' + bundleCount : bundleCount + ' ' + cmdMsg[state];
    } else {
        // Default text for unrecognized commands.
        toastMsg =
            state === 0
                ? cmdMsg[state] + ' ' + command + ' command'
                : command + ' command' + ' ' + cmdMsg[state];
    }
    toastMsg += state === 0 ? '...' : '!';
    return toastMsg;
};

export default withStyles(styles)(Worksheet);
