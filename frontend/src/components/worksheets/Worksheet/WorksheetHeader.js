import React from 'react';
import Grid from '@material-ui/core/Grid';
import Popover from '@material-ui/core/Popover';
import { WorksheetEditableField } from '../../EditableField';
import PermissionDialog from '../PermissionDialog';
import Sticky from 'react-stickynode';
import ActionButtons from '../items/ActionButtons';
import Tooltip from '@material-ui/core/Tooltip';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/InfoOutlined';
import OpenWithIcon from '@material-ui/icons/OpenWith';
import './Worksheet.scss';

const styles = {
    backgroundColor: 'white',
    paddingLeft: 28,
    paddingRight: 28,
    position: 'relative',
    borderBottom: '1px solid #ddd',
    // overflow: 'hidden',
};

export default ({
    onShowNewRun,
    onShowNewText,
    onShowNewSchema,
    showUploadMenu,
    closeUploadMenu,
    uploadAnchor,
    editPermission,
    info,
    classes,
    renderPermissions,
    reloadWorksheet,
    editButtons,
    anchorEl,
    setAnchorEl,
    handleSelectedBundleCommand,
    showBundleOperationButtons,
    toggleCmdDialog,
    toggleCmdDialogNoEvent,
    toggleInformationModal,
    onError,
    copiedBundleIds,
    showPasteButton,
    toggleWorksheetSize,
    showBundleContent,
    syntaxHighlight,
}) => (
    <Sticky top='#codalab-app-bar' innerZ={1059}>
        <div className='worksheet_content' style={styles}>
            <div className='header-row'>
                <Grid container direction='column'>
                    <Grid
                        container
                        item
                        xs={12}
                        spacing={0}
                        alignItems='flex-start'
                        justify='space-between'
                    >
                        <h5 className='worksheet-title' style={{ marginBottom: 0 }}>
                            {/*TODO: use html contenteditable*/}
                            <WorksheetEditableField
                                key={'title' + editPermission}
                                canEdit={editPermission}
                                fieldName='title'
                                value={info ? info.title : 'Loading...'}
                                uuid={info && info.uuid}
                                onChange={() => reloadWorksheet()}
                                allowASCII={true}
                            />
                        </h5>
                        <Grid item style={{ paddingTop: '10px' }}>
                            {info && (
                                <React.Fragment>
                                    <WorksheetEditableField
                                        canEdit={editPermission}
                                        fieldName='name'
                                        value={info && info.name}
                                        uuid={info && info.uuid}
                                        onChange={() => reloadWorksheet()}
                                        onError={onError}
                                    />
                                    &nbsp;by&nbsp;
                                    {info.owner_name ? info.owner_name : '<anonymous>'}
                                    <div
                                        onClick={(ev) => {
                                            setAnchorEl(ev.currentTarget);
                                        }}
                                        className={classes.permissions}
                                        style={{ display: 'inline-block', margin: '0px 5px' }}
                                    >
                                        {renderPermissions(info)}
                                    </div>
                                    <Popover
                                        open={Boolean(anchorEl)}
                                        anchorEl={anchorEl}
                                        onClose={() => {
                                            setAnchorEl(null);
                                        }}
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
                                        <div style={{ padding: 16 }}>
                                            <PermissionDialog
                                                uuid={info.uuid}
                                                permission_spec={info.permission_spec}
                                                group_permissions={info.group_permissions}
                                                onChange={reloadWorksheet}
                                                wperm
                                            />
                                        </div>
                                    </Popover>
                                    &nbsp;tags:&nbsp;
                                    <div style={{ display: 'inline-block' }}>
                                        <WorksheetEditableField
                                            canEdit={editPermission}
                                            dataType='list'
                                            fieldName='tags'
                                            value={info.tags.join(' ')}
                                            uuid={info && info.uuid}
                                            onChange={() => reloadWorksheet()}
                                        />
                                    </div>
                                </React.Fragment>
                            )}
                        </Grid>
                    </Grid>
                    <Grid
                        container
                        item
                        xs={12}
                        spacing={0}
                        alignItems='flex-end'
                        justify='space-between'
                        style={{ lineHeight: 2.5 }}
                    >
                        <Grid item>
                            <ActionButtons
                                info={info}
                                onShowNewRun={onShowNewRun}
                                onShowNewText={onShowNewText}
                                onShowNewSchema={onShowNewSchema}
                                showUploadMenu={showUploadMenu}
                                closeUploadMenu={closeUploadMenu}
                                uploadAnchor={uploadAnchor}
                                handleSelectedBundleCommand={handleSelectedBundleCommand}
                                showBundleOperationButtons={showBundleOperationButtons}
                                toggleCmdDialog={toggleCmdDialog}
                                toggleCmdDialogNoEvent={toggleCmdDialogNoEvent}
                                copiedBundleIds={copiedBundleIds}
                                showPasteButton={showPasteButton}
                                showBundleContent={showBundleContent}
                                syntaxHighlight={syntaxHighlight}
                            />
                        </Grid>
                        <Grid item>
                            {editButtons}
                            <Tooltip
                                disableFocusListener
                                disableTouchListener
                                title='Shortcuts'
                                aria-label='keyboard shortcuts'
                            >
                                <IconButton
                                    color='inherit'
                                    href='#'
                                    onClick={toggleInformationModal}
                                >
                                    <InfoIcon fontSize='small' />
                                </IconButton>
                            </Tooltip>
                            <Tooltip
                                disableFocusListener
                                disableTouchListener
                                title='Expand/Shrink'
                                aria-label='toggle worksheet width'
                            >
                                <IconButton color='inherit' href='#' onClick={toggleWorksheetSize}>
                                    <OpenWithIcon fontSize='small' />
                                </IconButton>
                            </Tooltip>
                        </Grid>
                    </Grid>
                </Grid>
            </div>
        </div>
    </Sticky>
);
