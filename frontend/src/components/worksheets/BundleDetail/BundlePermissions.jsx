import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import { renderPermissions } from '../../../util/worksheet_utils';
import PermissionDialog from '../PermissionDialog';

/**
 * This component renders color-coded bundle permissions.
 * Users can toggle an arrow icon to see the full permission dialog.
 */
class BundlePermissions extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { bundleInfo, classes, showDialog, onClick, onChange } = this.props;
        const { uuid, permission_spec, group_permissions } = bundleInfo;
        const style = {
            maxWidth: 168,
            whiteSpace: 'nowrap',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
        };

        return (
            <>
                <div onClick={onClick} className={classes.permissionsContainer}>
                    <div className={classes.permissions}>
                        {renderPermissions(bundleInfo, style)}
                        {showDialog ? (
                            <KeyboardArrowDownIcon fontSize='small' />
                        ) : (
                            <ChevronRightIcon fontSize='small' />
                        )}
                    </div>
                </div>
                {showDialog && (
                    <div className={classes.dialogConatiner}>
                        <PermissionDialog
                            uuid={uuid}
                            permission_spec={permission_spec}
                            group_permissions={group_permissions}
                            onChange={onChange}
                            perm
                        />
                    </div>
                )}
            </>
        );
    }
}

const styles = (theme) => ({
    permissionsContainer: {
        cursor: 'pointer',
        '&:hover': {
            backgroundColor: theme.color.primary,
        },
    },
    permissions: {
        display: 'flex',
        height: 21,
    },
    dialogConatiner: {
        marginTop: 5,
    },
});

export default withStyles(styles)(BundlePermissions);
