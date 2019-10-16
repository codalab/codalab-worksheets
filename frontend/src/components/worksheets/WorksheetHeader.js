import React from 'react';
import Grid from '@material-ui/core/Grid';
import Popover from '@material-ui/core/Popover';
import { WorksheetEditableField } from '../EditableField';
import PermissionDialog from './PermissionDialog';
import Sticky from 'react-stickynode';
import { NAVBAR_HEIGHT } from '../../constants';
import ItemWrapper from './items/ItemWrapper';
import ActionButtons from './items/ActionButtons';

const styles = {
    backgroundColor: "white",
    paddingLeft: 36,
    paddingRight: 36,
    paddingTop: 10,
    paddingBottom: 20,
    boxShadow: "0px 2px 4px -2px rgba(0,0,0,0.2), 0px 4px 5px -1px rgba(0,0,0,0.14), 0px 1px 10px -1px rgba(0,0,0,0.12)"
};

export default ({ showActionBar, onShowNewUpload, onShowNewRun, onShowNewText, canEdit, info, classes, renderPermissions, reloadWorksheet, editButtons, anchorEl, setAnchorEl }) =>
    <Sticky top={NAVBAR_HEIGHT - 6 + (showActionBar ? 60: 0)} innerZ={1059}>
        <div className='worksheet_content' style={styles}>
            <div className='header-row'>
                <Grid container alignItems="flex-end">
                    <Grid item sm={12} md={7}>
                        <h4 className='worksheet-title'>
                            {/*TODO: hack, take out ASAP*/}
                            <WorksheetEditableField
                                key={'title' + canEdit}
                                canEdit={canEdit}
                                fieldName='title'
                                value={(info && decodeURI(info.title)) || "Untitled"}
                                uuid={info && info.uuid}
                                onChange={() => reloadWorksheet()}
                            />
                        </h4>
                    </Grid>
                    <Grid item sm={12} md={5} container direction="column" justify="flex-end">
                        <Grid item sm={12}>
                            {info && <div className={classes.uuid}>{info.uuid}</div>}
                        </Grid>
                        <Grid item sm={12} container direction="row">
                            <Grid item container sm={6} direction="column" alignItems="flex-end" justify="flex-end">
                                {!info ? null : (
                                    <React.Fragment>
                                        <Grid item>
                                            <span className={classes.label}>name:</span>
                                            <WorksheetEditableField
                                                canEdit={canEdit}
                                                fieldName='name'
                                                value={info && decodeURI(info.name)}
                                                uuid={info && info.uuid}
                                                onChange={() => reloadWorksheet()}
                                            />
                                        </Grid>
                                        <Grid item>
                                            <span className={classes.label}>owner:</span>
                                            {info.owner_name ? info.owner_name : '<anonymous>'}
                                        </Grid>
                                        <Grid item>
                                            <div
                                                onClick={(ev) => {
                                                    setAnchorEl(ev.currentTarget);
                                                }}
                                                className={classes.permissions}
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
                                        </Grid>
                                    </React.Fragment>
                                )}
                            </Grid>
                            <Grid item container sm={6} direction="column" alignItems="flex-end">
                                <div className='controls'>
                                    <a
                                        href='#'
                                        data-toggle='modal'
                                        data-target='#glossaryModal'
                                        className='glossary-link'
                                    >
                                        <code>?</code> Keyboard Shortcuts
                            </a>
                                    {editButtons}
                                </div>
                            </Grid>
                        </Grid>
                    </Grid>
                    <Grid item sm={12}>
                        <ActionButtons
                            onShowNewUpload={onShowNewUpload}
                            onShowNewRun={onShowNewRun}
                            onShowNewText={onShowNewText}
                        />
                    </Grid>
                </Grid>
            </div>
        </div>
    </Sticky>;