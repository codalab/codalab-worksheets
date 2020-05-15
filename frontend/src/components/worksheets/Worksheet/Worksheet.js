import * as React from 'react';
import $ from 'jquery';
import _ from 'underscore';
import { withStyles } from '@material-ui/core/styles';
import {
    keepPosInView,
    renderPermissions,
    getAfterSortKey,
    createAlertText,
    getIds,
} from '../../../util/worksheet_utils';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import WorksheetItemList from '../WorksheetItemList';
import ReactDOM from 'react-dom';
import ExtraWorksheetHTML from '../ExtraWorksheetHTML/ExtraWorksheetHTML';
import 'jquery-ui-bundle';
import WorksheetHeader from './WorksheetHeader';
import {
    NAVBAR_HEIGHT,
    EXPANDED_WORKSHEET_WIDTH,
    DEFAULT_WORKSHEET_WIDTH,
    LOCAL_STORAGE_WORKSHEET_WIDTH,
} from '../../../constants';
import WorksheetActionBar from '../WorksheetActionBar';
import Loading from '../../Loading';
import Button from '@material-ui/core/Button';
import Icon from '@material-ui/core/Icon';
import EditIcon from '@material-ui/icons/EditOutlined';
import SaveIcon from '@material-ui/icons/SaveOutlined';
import DeleteIcon from '@material-ui/icons/DeleteOutline';
import UndoIcon from '@material-ui/icons/UndoOutlined';
import ContractIcon from '@material-ui/icons/ExpandLessOutlined';
import ExpandIcon from '@material-ui/icons/ExpandMoreOutlined';
import './Worksheet.scss';
import ErrorMessage from '../ErrorMessage';
import { ContextMenuMixin, default as ContextMenu } from '../ContextMenu';
import { buildTerminalCommand } from '../../../util/worksheet_utils';
import { executeCommand } from '../../../util/cli_utils';
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogActions from '@material-ui/core/DialogActions';
import Tooltip from '@material-ui/core/Tooltip';
import CloseIcon from '@material-ui/icons/Close';
import Grid from '@material-ui/core/Grid';
import WorksheetDialogs from '../WorksheetDialogs';
import { ToastContainer, toast, Zoom } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import queryString from 'query-string';
import { setPriority } from 'os';

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
        this.state = {
            ws: {
                uuid: this.props.match.params['uuid'],
                info: null,
            },
            version: 0, // Increment when we refresh
            escCount: 0, // Increment when the user presses esc keyboard shortcut, a hack to allow esc shortcut to work
            activeComponent: 'list', // Where the focus is (action, list, or side_panel)
            editMode: false, // Whether we're editing the worksheet
            editorEnabled: false, // Whether the editor is actually showing (sometimes lags behind editMode)
            showActionBar: false, // Whether the action bar is shown
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
            uploadAnchor: null,
            showRerun: false,
            isValid: true,
            checkedBundles: {},
            BulkBundleDialog: null,
            showBundleOperationButtons: false,
            uuidBundlesCheckedCount: {},
            openDelete: false,
            openDetach: false,
            openKill: false,
            openDeleteItem: false,
            forceDelete: false,
            showGlossaryModal: false,
            errorMessage: '',
            deleteWorksheetConfirmation: false,
            deleteItemCallback: null,
            copiedBundleIds: '',
            showPasteButton: window.localStorage.getItem('CopiedBundles') !== '',
            worksheetWidthPercentage: localWorksheetWidthPreference || DEFAULT_WORKSHEET_WIDTH,
        };
        this.copyCallbacks = [];
        this.bundleTableID = new Set();
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

        $.ajax({
            type: 'GET',
            url:
                '/rest/interpret/worksheet/' +
                this.state.ws.uuid +
                '?' +
                queryString.stringify(queryParams),
            // TODO: migrate to using main API
            // url: '/rest/worksheets/' + ws.uuid,
            async: props.async,
            dataType: 'json',
            cache: false,
            success: function(info) {
                this.setState({
                    ws: {
                        ...this.state.ws,
                        info,
                    },
                });
                props.success(info);
            }.bind(this),
            error: function(xhr, status, err) {
                props.error(xhr, status, err);
            },
        });
    }

    saveWorksheet(props) {
        if (this.state.ws.info === undefined) return;
        $('#update_progress').show();
        props = props || {};
        props.success = props.success || function(data) {};
        props.error = props.error || function(xhr, status, err) {};
        $('#save_error').hide();
        $.ajax({
            type: 'POST',
            cache: false,
            url: '/rest/worksheets/' + this.state.ws.uuid + '/raw',
            dataType: 'json',
            data: this.state.ws.info.source.join('\n'),
            success: function(data) {
                console.log('Saved worksheet ' + this.state.ws.uuid);
                props.success(data);
            }.bind(this),
            error: function(xhr, status, err) {
                props.error(xhr, status, err);
            },
        });
    }

    deleteWorksheet(props) {
        if (this.state.ws.info === undefined) return;
        $('#update_progress').show();
        $('#save_error').hide();
        $.ajax({
            type: 'DELETE',
            cache: false,
            url: '/rest/worksheets?force=1',
            contentType: 'application/json',
            data: JSON.stringify({ data: [{ id: this.state.ws.info.uuid, type: 'worksheets' }] }),
            success: function(data) {
                console.log('Deleted worksheet ' + this.state.ws.info.uuid);
                props.success && props.success(data);
            }.bind(this),
            error: function(xhr, status, err) {
                props.error && props.error(xhr, status, err);
            },
        });
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
        if (check) {
            //A bundle is checked
            if (
                uuid in this.state.checkedBundles &&
                identifier in this.state.checkedBundles[uuid]
            ) {
                return;
            }
            let bundlesCount = this.state.uuidBundlesCheckedCount;
            if (!(uuid in bundlesCount)) {
                bundlesCount[uuid] = 0;
            }
            bundlesCount[uuid] += 1;
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
                this.state.uuidBundlesCheckedCount[uuid] -= 1;
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
        this.setState({ updating: true });
        executeCommand(
            buildTerminalCommand([
                cmd,
                force_delete,
                ...Object.keys(this.state.uuidBundlesCheckedCount),
            ]),
            worksheet_uuid,
        )
            .done(() => {
                this.clearCheckedBundles(() => {
                    toast.info('Executing ' + cmd + ' command', {
                        position: 'top-right',
                        autoClose: 2000,
                        hideProgressBar: true,
                        closeOnClick: true,
                        pauseOnHover: false,
                        draggable: true,
                    });
                });
                this.reloadWorksheet();
            })
            .fail((e) => {
                let bundle_error_dialog = (
                    <Dialog
                        open={true}
                        onClose={this.toggleBundleBulkMessageDialog}
                        aria-labelledby='bundle-error-confirmation-title'
                        aria-describedby='bundle-error-confirmation-description'
                    >
                        <DialogTitle id='bundle-error-confirmation-title'>
                            <Grid container direction='row'>
                                <Grid item xs={10}>
                                    {'Failed to perform this action'}
                                </Grid>
                                <Grid item xs={2}>
                                    <Button
                                        variant='outlined'
                                        size='small'
                                        onClick={(e) => {
                                            this.setState({ BulkBundleDialog: null });
                                        }}
                                    >
                                        <CloseIcon size='small' />
                                    </Button>
                                </Grid>
                            </Grid>
                        </DialogTitle>
                        <DialogContent>
                            <DialogContentText
                                id='alert-dialog-description'
                                style={{ color: 'grey' }}
                            >
                                {e.responseText}
                            </DialogContentText>
                        </DialogContent>
                    </Dialog>
                );
                this.setState({ BulkBundleDialog: bundle_error_dialog, updating: false });
            });
    };

    handleForceDelete = (e) => {
        this.setState({ forceDelete: e.target.checked });
    };

    toggleBundleBulkMessageDialog = () => {
        this.setState({ BulkBundleDialog: null });
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

    // Helper functions to deal with commands
    toggleCmdDialog = (cmd_type) => () => {
        this.handleCommand(cmd_type);
    };

    toggleCmdDialogNoEvent = (cmd_type) => {
        this.handleCommand(cmd_type);
    };

    handleCommand = (cmd_type) => {
        if (cmd_type === 'deleteItem') {
            // This is used to delete markdown blocks
            this.setState({ openDeleteItem: !this.state.openDeleteItem });
        }
        const { openKill, openDelete, openDetach } = this.state;
        if (cmd_type === 'rm') {
            this.setState({ openDelete: !openDelete });
        } else if (cmd_type === 'detach') {
            this.setState({ openDetach: !openDetach });
        } else if (cmd_type === 'kill') {
            this.setState({ openKill: !openKill });
        } else if (cmd_type === 'copy') {
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
            let toastString =
                actualCopiedCounts > 0
                    ? 'Copied ' + actualCopiedCounts + ' bundle'
                    : 'No valid bundle to copy';
            if (actualCopiedCounts > 1) {
                toastString += 's';
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

    confirmBundleRowAction = (code) => {
        if (
            !(
                this.state.openDelete ||
                this.state.openDetach ||
                this.state.openKill ||
                this.state.openCopy ||
                this.state.BulkBundleDialog
            )
        ) {
            // no dialog is opened, open bundle row detail
            return false;
        } else if (code === 'KeyX' || code === 'Space') {
            return true;
        } else if (this.state.openDelete) {
            this.executeBundleCommandNoEvent('rm');
        } else if (this.state.openDetach) {
            this.executeBundleCommandNoEvent('detach');
        } else if (this.state.openKill) {
            this.executeBundleCommandNoEvent('kill');
        } else if (this.state.openCopy) {
            document.getElementById('copyBundleIdToClipBoard').click();
        }
        return true;
    };
    // BULK OPERATION RELATED CODE ABOVE======================================
    setDeleteItemCallback = (callback) => {
        this.setState({ deleteItemCallback: callback, openDeleteItem: true });
    };

    pasteBundlesToWorksheet = () => {
        // Unchecks all bundles after pasting
        const data = JSON.parse(window.localStorage.getItem('CopiedBundles'));
        let bundleString = '';
        let items = [];
        data.forEach((bundle) => {
            bundleString += '[]{' + bundle.uuid + '}\n';
            items.push(bundle.uuid);
        });
        // remove the last new line character
        bundleString = bundleString.substr(0, bundleString.length - 1);
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
        }
        actualData['item_type'] = 'bundle';
        $.ajax({
            url,
            data: JSON.stringify(actualData),
            contentType: 'application/json',
            type: 'POST',
            success: () => {
                const moveIndex = true;
                const param = { moveIndex };
                this.reloadWorksheet(undefined, undefined, param);
            },
            error: (jqHXR) => {
                alert(createAlertText(this.url, jqHXR.responseText));
            },
        });
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
            },
            clear_callback,
        );
        this.bundleTableID = new Set();
        this.copyCallbacks = {};
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

    setFocus = (index, subIndex, shouldScroll = true) => {
        var info = this.state.ws.info;
        // prevent multiple clicking from resetting the index
        if (index === this.state.focusIndex && subIndex === this.state.subFocusIndex) {
            return;
        }
        const item = this.refs.list.refs['item' + index];
        if (item && (!item.props || !item.props.item)) {
            // Skip "no search results" items and scroll past them.
            const offset = index - this.state.focusIndex;
            if (offset === 0) {
                return;
            }
            this.setFocus(index + offset, subIndex, shouldScroll);
            return;
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
        if (
            index < -1 ||
            index >= info.blocks.length ||
            subIndex < -1 ||
            subIndex >= (this._numTableRows(info.blocks[index]) || 1)
        ) {
            console.log('Out of bounds');
            return; // Out of bounds (note index = -1 is okay)
        }
        let focusedBundleUuidList = [];
        if (index !== -1) {
            // index !== -1 means something is selected.
            // focusedBundleUuidList is a list of uuids of all bundles after the selected bundle (itself included)
            // Say the selected bundle has focusIndex 1 and subFocusIndex 2, then focusedBundleUuidList will include the uuids of
            // all the bundles that have focusIndex 1 and subFocusIndex >= 2, and also all the bundles that have focusIndex > 1
            for (var i = index; i < info.blocks.length; i++) {
                if (info.blocks[i].bundles_spec) {
                    var j = i === index ? subIndex : 0;
                    for (; j < (this._numTableRows(info.blocks[i]) || 1); j++) {
                        focusedBundleUuidList.push(
                            info.blocks[i].bundles_spec.bundle_infos[j].uuid,
                        );
                    }
                }
            }
        }
        // Change the focus - triggers updating of all descendants.
        this.setState({
            focusIndex: index,
            subFocusIndex: subIndex,
            focusedBundleUuidList: focusedBundleUuidList,
            showNewRun: false,
            showNewText: false,
            uploadAnchor: null,
            showNewRerun: false,
        });
        if (shouldScroll) {
            this.scrollToItem(index, subIndex);
        }
    };

    scrollToItem = (index, subIndex) => {
        // scroll the window to keep the focused element in view if needed
        var __innerScrollToItem = function(index, subIndex) {
            // Compute the current position of the focused item.
            var pos;
            if (index === -1) {
                pos = -1000000; // Scroll all the way to the top
            } else {
                var item = this.refs.list.refs['item' + index];
                if (!item) {
                    // Don't scroll to an item if it doesn't exist.
                    return;
                }
                if (this._numTableRows(item.props.item)) {
                    item = item.refs['row' + subIndex]; // Specifically, the row
                }
                var node = ReactDOM.findDOMNode(item);
                pos = node.getBoundingClientRect().top;
            }
            keepPosInView(pos);
        };

        // Throttle so that if keys are held down, we don't suffer a huge lag.
        if (this.throttledScrollToItem === undefined)
            this.throttledScrollToItem = _.throttle(__innerScrollToItem, 50).bind(this);
        this.throttledScrollToItem(index, subIndex);
    };

    componentDidMount() {
        this.fetch({
            brief: true,
            success: function(data) {
                $('#worksheet_content').show();
                this.setState({
                    updating: false,
                    version: this.state.version + 1,
                    numOfBundles: this.getNumOfBundles(),
                    errorMessage: '',
                });
                // Fix out of bounds.
            }.bind(this),
            error: function(xhr, status, err) {
                this.setState({
                    errorMessage: xhr.responseText,
                    isValid: false,
                });
            }.bind(this),
        });

        // Initialize history stack
        window.history.replaceState({ uuid: this.state.ws.uuid }, '', window.location.pathname);
        $('body').addClass('ws-interface');
        $.ajax({
            url: '/rest/user',
            dataType: 'json',
            cache: false,
            type: 'GET',
            success: function(data) {
                var userInfo = data.data.attributes;
                userInfo.user_id = data.data.id;
                this.setState({
                    userInfo: userInfo,
                });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(xhr.responseText);
            },
        });
    }

    canEdit() {
        var info = this.state.ws.info;
        return info && info.edit_permission;
    }

    viewMode = () => {
        this.toggleEditMode(false, true);
    };
    discardChanges = () => {
        this.toggleEditMode(false, false);
    };
    editMode = () => {
        this.toggleEditMode(true);
    };
    handleActionBarFocus = (event) => {
        this.setState({ activeComponent: 'action' });
        // just scroll to the top of the page.
        // Add the stop() to keep animation events from building up in the queue
        $('#command_line').data('resizing', null);
        $('body')
            .stop(true)
            .animate({ scrollTop: 0 }, 250);
    };
    handleActionBarBlur = (event) => {
        // explicitly close terminal because we're leaving the action bar
        // $('#command_line').terminal().focus(false);
        this.setState({ activeComponent: 'list' });
        $('#command_line').data('resizing', null);
        $('#ws_search').removeAttr('style');
    };
    toggleGlossaryModal = () => {
        this.setState({ showGlossaryModal: !this.state.showGlossaryModal });
    };
    toggleWorksheetSize = () => {
        let newPercentage =
            this.state.worksheetWidthPercentage === DEFAULT_WORKSHEET_WIDTH
                ? EXPANDED_WORKSHEET_WIDTH
                : DEFAULT_WORKSHEET_WIDTH;
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

        if (this.state.activeComponent === 'action') {
            // no need for other keys, we have the action bar focused
            return;
        }

        if (!this.state.ws.info) {
            // disable all keyboard shortcuts when loading worksheet
            return;
        }

        if (
            !(
                this.state.openDelete ||
                this.state.openDetach ||
                this.state.openKill ||
                this.state.BulkBundleDialog
            )
        ) {
            // Only enable these shortcuts when no dialog is opened
            Mousetrap.bind(
                ['shift+r'],
                function(e) {
                    this.reloadWorksheet(undefined, undefined);
                    toast.info('🦄 Worksheet refreshed!', {
                        position: 'top-right',
                        autoClose: 1500,
                        hideProgressBar: false,
                        closeOnClick: true,
                        pauseOnHover: false,
                        draggable: true,
                    });
                }.bind(this),
            );

            // Show/hide web terminal (action bar)
            Mousetrap.bind(
                ['shift+c'],
                function(e) {
                    this.toggleActionBar();
                }.bind(this),
            );

            // Focus on web terminal (action bar)
            Mousetrap.bind(
                ['c c'],
                function(e) {
                    this.focusActionBar();
                }.bind(this),
            );

            // Toggle edit mode
            Mousetrap.bind(
                ['shift+e'],
                function(e) {
                    this.toggleEditMode();
                    return false;
                }.bind(this),
            );

            // Focus on search
            Mousetrap.bind(['a+f'], function(e) {
                document.getElementById('search-bar').focus();
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
                        // worksheet_item_interface and table_item_interface do the exact same thing anyway right now
                        if (focusIndex === 0 && subFocusIndex === 0) {
                            // stay on the first row
                            return;
                        }
                        if (subFocusIndex - 1 < 0) {
                            this.setFocus(focusIndex - 1 < 0 ? 0 : focusIndex - 1, 'end'); // Move out of this table to the item above the current table
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
                        if (subFocusIndex + 1 >= this._numTableRows(wsItems[focusIndex])) {
                            this.setFocus(focusIndex + 1, 0);
                        } else {
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
                            $('html, body').animate({ scrollTop: $(document).height() }, 'fast');
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
                            $('html, body').animate({ scrollTop: $(document).height() }, 'fast');
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
                            $('html, body').animate({ scrollTop: $(document).height() }, 'fast');
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
        Mousetrap.bind(['?'], (e) => {
            this.setState({
                showGlossaryModal: true,
            });
        });
        Mousetrap.bind(['+'], (e) => {
            this.toggleWorksheetSize();
        });

        Mousetrap.bind(['esc'], (e) => {
            ContextMenuMixin.closeContextMenu();
        });

        if (this.state.openDeleteItem) {
            Mousetrap.bind(
                ['enter'],
                function(e) {
                    e.preventDefault();
                    this.state.deleteItemCallback();
                    this.toggleCmdDialogNoEvent('deleteItem');
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
            Mousetrap.bind(['backspace', 'del'], () => {
                if (this.state.openDetach || this.state.openKill) {
                    return;
                }
                this.toggleCmdDialogNoEvent('rm');
            });
            Mousetrap.bind(['a d'], () => {
                if (this.state.openDelete || this.state.openKill) {
                    return;
                }
                this.toggleCmdDialogNoEvent('detach');
            });
            Mousetrap.bind(['a k'], () => {
                if (this.state.openDetach || this.state.openDelete) {
                    return;
                }
                this.toggleCmdDialogNoEvent('kill');
            });
            Mousetrap.bind(['a c'], () => {
                if (this.state.openDetach || this.state.openDelete || this.state.openKill) {
                    return;
                }
                this.toggleCmdDialogNoEvent('copy');
            });

            // Confirm bulk bundle operation
            if (this.state.openDelete || this.state.openKill || this.state.openDetach) {
                Mousetrap.bind(
                    ['enter'],
                    function(e) {
                        if (this.state.openDelete) {
                            this.executeBundleCommandNoEvent('rm');
                        } else if (this.state.openKill) {
                            this.executeBundleCommandNoEvent('kill');
                        } else if (this.state.openDetach) {
                            this.executeBundleCommandNoEvent('detach');
                        }
                    }.bind(this),
                );

                // Select/Deselect to force delete during deletion dialog
                Mousetrap.bind(
                    ['f'],
                    function() {
                        //force deletion through f
                        if (this.state.openDelete) {
                            this.setState({ forceDelete: !this.state.forceDelete });
                        }
                    }.bind(this),
                );
            }
        }
        //====================Bulk bundle operations===================
    }

    toggleEditMode(editMode, saveChanges) {
        if (editMode === undefined) editMode = !this.state.editMode; // Toggle by default

        if (saveChanges === undefined) saveChanges = true;

        if (!editMode) {
            // Going out of raw mode - save the worksheet.
            if (this.canEdit()) {
                var editor = ace.edit('worksheet-editor');
                if (saveChanges) {
                    this.state.ws.info.source = editor.getValue().split('\n');
                }
                var rawIndex = editor.getCursorPosition().row;
                this.setState({
                    editMode: editMode,
                    editorEnabled: false,
                }); // Needs to be after getting the raw contents
                if (saveChanges) {
                    this.saveAndUpdateWorksheet(saveChanges, rawIndex);
                } else {
                    this.reloadWorksheet(undefined, rawIndex);
                }
            } else {
                // Not allowed to edit the worksheet.
                this.setState({
                    editMode: editMode,
                    editorEnabled: false,
                });
            }
        } else {
            // Go into edit mode.
            this.setState({ editMode: editMode });
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
                return 'uuid=' + bundle_uuid;
            })
            .join('&');
        $.ajax({
            type: 'GET',
            url: '/rest/interpret/worksheet/' + worksheetUuid + '?' + queryParams,
            dataType: 'json',
            cache: false,
            success: function(worksheet_content) {
                if (this.state.isUpdatingBundles && worksheet_content.uuid === this.state.ws.uuid) {
                    if (worksheet_content.items) {
                        self.reloadWorksheet(worksheet_content.items);
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
            }.bind(this),
            error: function(xhr, status, err) {
                this.setState({ errorMessage: xhr.responseText });
                $('#worksheet_container').hide();
            },
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
                        if (bundle_info.bundle_type === 'run') {
                            if (bundle_info.state !== 'ready' && bundle_info.state !== 'failed') {
                                updatingBundleUuids[bundle_info.uuid] = true;
                            } else {
                                if (bundle_info.uuid in updatingBundleUuids)
                                    delete updatingBundleUuids[bundle_info.uuid];
                            }
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
        if (this.state.editMode && !this.state.editorEnabled) {
            this.setState({ editorEnabled: true });
            var editor = ace.edit('worksheet-editor');
            editor.$blockScrolling = Infinity;
            editor.session.setUseWrapMode(false);
            editor.setShowPrintMargin(false);
            editor.session.setMode('ace/mode/markdown', function() {
                editor.session.$mode.blockComment = { start: '//', end: '' };
            });
            if (!this.canEdit()) {
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
                        this.toggleEditMode();
                    }.bind(this),
                    readOnly: true,
                });
                editor.commands.addCommand({
                    name: 'exit',
                    bindKey: { win: 'Esc', mac: 'Esc' },
                    exec: function() {
                        this.discardChanges();
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
        if (prevState.showActionBar !== this.state.showActionBar) {
            // Hack to make sure that the <Sticky> component in WorksheetHeader.js updates.
            // This is needed because otherwise the header doesn't move up or down as needed
            // when the action bar is shown / hidden.
            window.scrollTo(window.scrollX, window.scrollY + 1);
        }
    }

    toggleActionBar() {
        this.setState({ showActionBar: !this.state.showActionBar });
    }

    focusActionBar() {
        this.setState({ activeComponent: 'action' });
        this.setState({ showActionBar: true });
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
    // More spefically, all items that don't contain run bundles that need updating are null.
    // Also, a non-null item could contain a list of bundle_infos, which represent a list of bundles. Usually not all of them need updating.
    // The bundle_infos for bundles that don't need updating are also null.
    // If rawIndexAfterEditMode is defined, this reloadWorksheet is called right after toggling editMode. It should resolve rawIndex to (focusIndex, subFocusIndex) pair.
    reloadWorksheet = (
        partialUpdateItems,
        rawIndexAfterEditMode,
        { moveIndex = false, textDeleted = false } = {},
    ) => {
        let itemHeights = {};
        if (this.refs.list && this.refs.list.refs) {
            for (let refName in this.refs.list.refs) {
                itemHeights[refName] = ReactDOM.findDOMNode(
                    this.refs.list.refs[refName],
                ).clientHeight;
            }
        }
        this.setState({ itemHeights });
        if (partialUpdateItems === undefined) {
            $('#update_progress').show();
            this.setState({ updating: true });
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
                    this.setState({ errorMessage: '' });
                    $('#update_progress').hide();
                    $('#worksheet_content').show();
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
                        this.state.numOfBundles !== -1 &&
                        numOfBundles > this.state.numOfBundles
                    ) {
                        // If the number of bundles increases then the focus should be on the new bundle.
                        // if the current focus is not on a table
                        if (
                            items[focus] &&
                            items[focus].mode &&
                            items[focus].mode !== 'table_block'
                        ) {
                            this.setFocus(focus >= 0 ? focus + 1 : 'end', 0);
                        } else if (this.state.subFocusIndex !== undefined) {
                            // Focus on the next bundle row
                            this.setFocus(focus >= 0 ? focus : 'end', this.state.subFocusIndex + 1);
                        } else {
                            this.setFocus(focus >= 0 ? focus : 'end', 'end');
                        }
                    } else if (numOfBundles < this.state.numOfBundles) {
                        // If the number of bundles decreases, then focus should be on the same bundle as before
                        // unless that bundle doesn't exist anymore, in which case we select the one above it.
                        // the deleted bundle is the only item of the table
                        if (this.state.subFocusIndex === 0) {
                            // the deleted item is the last item of the worksheet
                            if (items.length === focus + 1) {
                                this.setFocus(focus - 1, 0);
                            } else {
                                this.setFocus(focus, 0);
                            }
                            // the deleted bundle is the last item of the table
                            // note that for some reason subFocusIndex begins with 1, not 0
                        } else if (this._numTableRows(items[focus]) === this.state.subFocusIndex) {
                            this.setFocus(focus, this.state.subFocusIndex - 1);
                        } else {
                            this.setFocus(focus, this.state.subFocusIndex);
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
                            this.setFocus(items.length === focus ? items.length - 1 : focus, 'end');
                        }
                    }
                    this.setState({
                        updating: false,
                        version: this.state.version + 1,
                        numOfBundles: numOfBundles,
                    });
                    this.checkRunBundle(this.state.ws.info);
                }.bind(this),
                error: function(xhr, status, err) {
                    this.setState({
                        updating: false,
                        errorMessage: xhr.responseText,
                    });
                    $('#update_progress').hide();
                    $('#worksheet_container').hide();
                }.bind(this),
            });
        } else {
            var ws = _.clone(this.state.ws);
            for (var i = 0; i < partialUpdateItems.length; i++) {
                if (!partialUpdateItems[i]) continue;
                // update interpreted items
                ws.info.blocks[i] = partialUpdateItems[i];
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

        // Note: this is redundant if we're doing 'cl work' from the action bar,
        // but is necessary if triggered in other ways.
        this.reloadWorksheet();

        // Create a new entry in the browser history with new URL.
        window.history.pushState({ uuid: this.state.ws.uuid }, '', '/worksheets/' + uuid + '/');
    };

    saveAndUpdateWorksheet = (fromRaw, rawIndex) => {
        this.setState({ updating: true, errorMessage: '' });
        this.saveWorksheet({
            success: function(data) {
                this.setState({ updating: false });
                this.reloadWorksheet(undefined, rawIndex);
            }.bind(this),
            error: function(xhr, status, err) {
                this.setState({ updating: false });
                $('#update_progress').hide();
                $('#save_error').show();
                this.setState({ errorMessage: xhr.responseText });
                if (fromRaw) {
                    this.toggleEditMode(true);
                }
            }.bind(this),
        });
    };

    deteleWorksheetAction = () => {
        this.setState({ updating: true, errorMessage: '' });
        this.deleteWorksheet({
            success: function(data) {
                this.setState({ updating: false });
                window.location = '/rest/worksheets/?name=dashboard';
            }.bind(this),
            error: function(xhr, status, err) {
                this.setState({ updating: false });
                $('#update_progress').hide();
                $('#save_error').show();
                this.setState({ errorMessage: xhr.responseText });
            }.bind(this),
        });
    };

    deleteThisWorksheet() {
        // TODO: put all worksheet dialogs into WorksheetDialogs.js if possible
        let deleteWorksheetDialog = (
            <Dialog
                open={true}
                onClose={this.toggleBundleBulkMessageDialog}
                aria-labelledby='delete-worksheet-confirmation-title'
                aria-describedby='delete-worksheet-confirmation-description'
            >
                <DialogTitle id='delete-worksheet-confirmation-title' style={{ color: 'red' }}>
                    Delete this worksheet permanently?
                </DialogTitle>
                <DialogContent>
                    <DialogContentText
                        id='alert-dialog-description'
                        style={{ color: 'red', marginBottom: '20px' }}
                    >
                        {'Warning: Deleted worksheets cannot be recovered.'}
                    </DialogContentText>
                    <DialogContentText id='alert-dialog-description' style={{ color: 'grey' }}>
                        {'Note: Deleting worksheets does not delete the bundles inside it.'}
                    </DialogContentText>
                    <DialogActions>
                        <Button color='primary' onClick={this.toggleBundleBulkMessageDialog}>
                            CANCEL
                        </Button>
                        <Button
                            color='primary'
                            variant='contained'
                            onClick={this.deteleWorksheetAction}
                        >
                            DELETE
                        </Button>
                    </DialogActions>
                </DialogContent>
            </Dialog>
        );
        this.setState({ BulkBundleDialog: deleteWorksheetDialog });
    }

    showUploadMenu = (e) => {
        // pause mousetrap events such as up, down, and enter
        Mousetrap.pause();
        let form = document.querySelector('#upload-menu');

        Mousetrap(form).bind(['enter'], function(e) {
            e.stopPropagation();
            document.querySelector('label[for=' + e.target.firstElementChild.htmlFor + ']').click();
        });

        this.setState({ uploadAnchor: e.currentTarget });
    };

    render() {
        const { classes } = this.props;
        const { anchorEl, uploadAnchor } = this.state;

        this.setupEventHandlers();
        var info = this.state.ws.info;
        var rawWorksheet = info && info.source.join('\n');
        var editPermission = info && info.edit_permission;
        var canEdit = this.canEdit() && this.state.editMode;

        var searchClassName = this.state.showActionBar ? '' : 'search-hidden';
        var editableClassName = canEdit ? 'editable' : '';
        var disableWorksheetEditing = this.canEdit() ? '' : 'disabled';
        var sourceStr = editPermission ? 'Edit Source' : 'View Source';
        var editFeatures = (
            <div style={{ display: 'inline-block' }}>
                <Button
                    onClick={this.editMode}
                    size='small'
                    color='inherit'
                    aria-label='Edit Source'
                    disabled={!info}
                >
                    <EditIcon className={classes.buttonIcon} />
                    {sourceStr}
                </Button>
                <Button
                    onClick={(e) => this.toggleActionBar()}
                    size='small'
                    color='inherit'
                    aria-label='Expand CLI'
                    id='terminal-button'
                    disabled={!info}
                >
                    {this.state.showActionBar ? (
                        <ContractIcon className={classes.buttonIcon} />
                    ) : (
                        <ExpandIcon className={classes.buttonIcon} />
                    )}
                    {this.state.showActionBar ? 'HIDE TERMINAL' : 'SHOW TERMINAL'}
                </Button>
                <Button
                    onClick={(e) => this.deleteThisWorksheet()}
                    size='small'
                    color='inherit'
                    aria-label='Delete Worksheet'
                    disabled={!editPermission}
                >
                    <Tooltip
                        disableFocusListener
                        disableTouchListener
                        title='Delete this worksheet'
                    >
                        <DeleteIcon />
                    </Tooltip>
                </Button>
            </div>
        );

        var editModeFeatures = (
            <div
                onMouseMove={(ev) => {
                    ev.stopPropagation();
                }}
                style={{ display: 'inline-block' }}
            >
                <Button
                    onClick={this.viewMode}
                    disabled={disableWorksheetEditing}
                    size='small'
                    color='inherit'
                    aria-label='Save Edit'
                >
                    <SaveIcon className={classes.buttonIcon} />
                    Save
                </Button>
                <Button
                    onClick={this.discardChanges}
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

        var raw_display = (
            <div>
                Press ctrl-enter to save. See{' '}
                <a
                    target='_blank'
                    href='https://codalab-worksheets.readthedocs.io/en/latest/Worksheet-Markdown'
                >
                    markdown syntax
                </a>
                .<div id='worksheet-editor'>{rawWorksheet}</div>
            </div>
        );

        var action_bar_display = (
            <WorksheetActionBar
                ref={'action'}
                ws={this.state.ws}
                handleFocus={this.handleActionBarFocus}
                handleBlur={this.handleActionBarBlur}
                active={this.state.activeComponent === 'action'}
                reloadWorksheet={this.reloadWorksheet}
                openWorksheet={this.openWorksheet}
                editMode={this.editMode}
                setFocus={this.setFocus}
                hidden={!this.state.showActionBar}
            />
        );

        var items_display = (
            <WorksheetItemList
                ref={'list'}
                active={this.state.activeComponent === 'list'}
                ws={this.state.ws}
                version={this.state.version}
                canEdit={canEdit}
                focusIndex={this.state.focusIndex}
                subFocusIndex={this.state.subFocusIndex}
                setFocus={this.setFocus}
                reloadWorksheet={this.reloadWorksheet}
                saveAndUpdateWorksheet={this.saveAndUpdateWorksheet}
                openWorksheet={this.openWorksheet}
                focusActionBar={this.focusActionBar}
                ensureIsArray={this.ensureIsArray}
                showNewRun={this.state.showNewRun}
                showNewText={this.state.showNewText}
                showNewRerun={this.state.showNewRerun}
                onHideNewRun={() => this.setState({ showNewRun: false })}
                onHideNewText={() => this.setState({ showNewText: false })}
                onHideNewRerun={() => this.setState({ showNewRerun: false })}
                handleCheckBundle={this.handleCheckBundle}
                confirmBundleRowAction={this.confirmBundleRowAction}
                setDeleteItemCallback={this.setDeleteItemCallback}
                addCopyBundleRowsCallback={this.addCopyBundleRowsCallback}
                onAsyncItemLoad={this.onAsyncItemLoad}
                itemHeights={this.state.itemHeights}
            />
        );

        const context_menu_display = (
            <ContextMenu userInfo={this.state.userInfo} ws={this.state.ws} />
        );

        var worksheet_display = this.state.editMode ? raw_display : items_display;
        var editButtons = this.state.editMode ? editModeFeatures : editFeatures;
        if (!this.state.isValid) {
            return <ErrorMessage message={"Not found: '/worksheets/" + this.state.ws.uuid + "'"} />;
        }

        var worksheet_dialogs = (
            <WorksheetDialogs
                openKill={this.state.openKill}
                openDelete={this.state.openDelete}
                openDetach={this.state.openDetach}
                openDeleteItem={this.state.openDeleteItem}
                toggleCmdDialog={this.toggleCmdDialog}
                toggleCmdDialogNoEvent={this.toggleCmdDialogNoEvent}
                executeBundleCommand={this.executeBundleCommand}
                forceDelete={this.state.forceDelete}
                handleForceDelete={this.handleForceDelete}
                deleteItemCallback={this.state.deleteItemCallback}
            />
        );
        if (info && info.title) {
            document.title = info.title;
        }

        return (
            <React.Fragment>
                {context_menu_display}
                <WorksheetHeader
                    showActionBar={this.state.showActionBar}
                    canEdit={this.canEdit()}
                    info={info}
                    classes={classes}
                    renderPermissions={renderPermissions}
                    reloadWorksheet={this.reloadWorksheet}
                    editButtons={editButtons}
                    anchorEl={anchorEl}
                    setAnchorEl={(e) => this.setState({ anchorEl: e })}
                    onShowNewRun={() => this.setState({ showNewRun: true })}
                    onShowNewText={() => this.setState({ showNewText: true })}
                    uploadAnchor={uploadAnchor}
                    showUploadMenu={this.showUploadMenu}
                    closeUploadMenu={() => {
                        this.setState({ uploadAnchor: null });
                        Mousetrap.unpause();
                    }}
                    handleSelectedBundleCommand={this.handleSelectedBundleCommand}
                    showBundleOperationButtons={this.state.showBundleOperationButtons}
                    toggleCmdDialog={this.toggleCmdDialog}
                    toggleGlossaryModal={this.toggleGlossaryModal}
                    toggleCmdDialogNoEvent={this.toggleCmdDialogNoEvent}
                    copiedBundleIds={this.state.copiedBundleIds}
                    showPasteButton={this.state.showPasteButton}
                    toggleWorksheetSize={this.toggleWorksheetSize}
                />
                {action_bar_display}
                <ToastContainer
                    newestOnTop={false}
                    transition={Zoom}
                    rtl={false}
                    pauseOnVisibilityChange
                />
                <div id='worksheet_container'>
                    <div id='worksheet' className={searchClassName}>
                        <div
                            className={classes.worksheetDesktop}
                            onClick={this.handleClickForDeselect}
                        >
                            <div
                                className={classes.worksheetOuter}
                                onClick={this.handleClickForDeselect}
                                style={{ width: this.state.worksheetWidthPercentage }}
                            >
                                <div
                                    className={classes.worksheetInner}
                                    onClick={this.handleClickForDeselect}
                                >
                                    <div
                                        id='worksheet_content'
                                        className={editableClassName + ' worksheet_content'}
                                    >
                                        {worksheet_display}
                                        {/* Show error dialog if bulk bundle execution failed*/}
                                        {this.state.BulkBundleDialog}
                                        <ExtraWorksheetHTML
                                            showGlossaryModal={this.state.showGlossaryModal}
                                            toggleGlossaryModal={this.toggleGlossaryModal}
                                            errorMessage={this.state.errorMessage}
                                            clearErrorMessage={() =>
                                                this.setState({ errorMessage: '' })
                                            }
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                {worksheet_dialogs}
                <ExtraWorksheetHTML
                    showGlossaryModal={this.state.showGlossaryModal}
                    toggleGlossaryModal={this.toggleGlossaryModal}
                />
                {this.state.updating && <Loading />}
                {!info && <Loading />}
            </React.Fragment>
        );
    }
}

const styles = (theme) => ({
    worksheetDesktop: {
        backgroundColor: theme.color.grey.lightest,
        marginTop: NAVBAR_HEIGHT,
    },
    worksheetOuter: {
        minHeight: 600, // Worksheet height
        margin: '32px auto', // Center page horizontally
        backgroundColor: 'white', // Paper color
        border: `2px solid ${theme.color.grey.light}`,
    },
    worksheetInner: {
        padding: '0px 30px', // Horizonal padding, no vertical
        height: '100%',
        position: 'relative',
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

export default withStyles(styles)(Worksheet);
