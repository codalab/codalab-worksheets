import React from 'react';
import Grid from '@material-ui/core/Grid';
import Popover from '@material-ui/core/Popover';
import { WorksheetEditableField } from '../EditableField';
import PermissionDialog from './PermissionDialog';
import Sticky from 'react-stickynode';
import { NAVBAR_HEIGHT } from '../../constants';
import ActionButtons from './items/ActionButtons';
import Tooltip from '@material-ui/core/Tooltip';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/InfoOutlined';

const styles = {
    backgroundColor: "white",
    paddingLeft: 28,
    paddingRight: 28,
    boxShadow: "0px 2px 4px -2px rgba(0,0,0,0.2), 0px 4px 5px -1px rgba(0,0,0,0.14), 0px 1px 10px -1px rgba(0,0,0,0.12)"
};

export default ({ showActionBar, onShowNewUpload, onShowNewRun, onShowNewText, canEdit, info, classes, renderPermissions, reloadWorksheet, editButtons, anchorEl, setAnchorEl }) =>
    <Sticky top={NAVBAR_HEIGHT - 6 + (showActionBar ? NAVBAR_HEIGHT : 0)} innerZ={1059}>
        <div className='worksheet_content' style={styles}>
            <div className='header-row'>
                <Grid container direction="column">
                    <Grid container item xs={12} spacing={2} alignItems="flex-start" justify="space-between">
                        <h5 className='worksheet-title'>
                            {/*TODO: use html contenteditable*/}
                            <WorksheetEditableField
                                key={'title' + canEdit}
                                canEdit={canEdit}
                                fieldName='title'
                                value={(info && decodeURI(info.title)) || "Untitled"}
                                uuid={info && info.uuid}
                                onChange={() => reloadWorksheet()}
                            />
                        </h5>
                        <Grid item style={{paddingTop: '10px'}}>
                        {info && (
                            <React.Fragment>
                                <WorksheetEditableField
                                    canEdit={canEdit}
                                    fieldName='name'
                                    value={info && info.name}
                                    uuid={info && info.uuid}
                                    onChange={() => reloadWorksheet()}
                                />
                                &nbsp;by&nbsp;
                                            {info.owner_name ? info.owner_name : '<anonymous>'}
                                <div
                                    onClick={(ev) => {
                                        setAnchorEl(ev.currentTarget);
                                    }}
                                    className={classes.permissions}
                                    style={{float: "right"}}
                                >
                                    {renderPermissions(info)}
                                </div>
                                <Popover
                                    open={Boolean(anchorEl)}
                                    anchorEl={anchorEl}
                                    onClose={() => { setAnchorEl(null) }}
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
                            </React.Fragment>
                            )}
                        </Grid>
                    </Grid>
                    <Grid container item xs={12} spacing={2} alignItems="flex-end" justify="space-between">
                        <Grid item>
                            <ActionButtons
                                onShowNewUpload={onShowNewUpload}
                                onShowNewRun={onShowNewRun}
                                onShowNewText={onShowNewText}
                            />
                        </Grid>
                        <Grid item>
                            {editButtons}
                            <Tooltip disableFocusListener disableTouchListener
                            title="Shortcuts"
                            aria-label='keyboard shortcuts'>
                                <IconButton
                                    color='inherit'
                                    href='#'
                                    data-toggle='modal'
                                    data-target='#glossaryModal'
                                    >
                                    <InfoIcon fontSize='small' />
                                </IconButton>
                                </Tooltip>
                        </Grid>
                    </Grid>
                </Grid>
            </div>
        </div>
    </Sticky>;