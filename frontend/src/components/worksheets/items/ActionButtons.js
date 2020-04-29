// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleOutline';
import UploadIcon from '@material-ui/icons/CloudUploadOutlined';
import AddIcon from '@material-ui/icons/AddBoxOutlined';
import BundleBulkActionMenu from '../BundleBulkActionMenu';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';

const StyledMenuItem = withStyles((theme) => ({
    root: {
        '&:focus': {
            backgroundColor: theme.palette.primary.main,
            '& .MuiListItemIcon-root, & .MuiListItemText-primary': {
                color: theme.palette.common.white,
            },
        },
        border: '2px solid #d3d4d5',
    },
}))(MenuItem);

class ActionButtons extends React.Component<{
    classes: {},
    onShowNewRun: () => void,
    onShowNewText: () => void,
    showUploadMenu: () => void,
}> {
    handleClick = (event) => {
        this.setState({ anchorEl: event.currentTarget });
    };

    handleClose = () => {
        this.setState({ anchorEl: null });
    };

    render() {
        const {
            classes,
            onShowNewRun,
            onShowNewText,
            showUploadMenu,
            closeUploadMenu,
            uploadAnchor,
            handleSelectedBundleCommand,
            showBundleOperationButtons,
            togglePopup,
            info,
        } = this.props;
        let editPermission = info && info.edit_permission;

        return (
            <div
                onMouseMove={(ev) => {
                    ev.stopPropagation();
                }}
            >
                {' '}
                {!showBundleOperationButtons ? (
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='Add Text'
                        onClick={onShowNewText}
                        disabled={!editPermission}
                    >
                        <AddIcon className={classes.buttonIcon} />
                        Text
                    </Button>
                ) : null}
                {!showBundleOperationButtons ? (
                    <span>
                        <Button
                            size='small'
                            color='inherit'
                            id='upload-button'
                            aria-label='Add New Upload'
                            onClick={showUploadMenu}
                            disabled={!editPermission}
                        >
                            <UploadIcon className={classes.buttonIcon} />
                            Upload
                        </Button>
                        <Menu
                            id='upload-menu'
                            elevation={0}
                            getContentAnchorEl={null}
                            anchorOrigin={{
                                vertical: 'bottom',
                                horizontal: 'center',
                            }}
                            transformOrigin={{
                                vertical: 'top',
                                horizontal: 'center',
                            }}
                            anchorEl={uploadAnchor}
                            keepMounted
                            open={Boolean(uploadAnchor)}
                            onClose={closeUploadMenu}
                        >
                            <StyledMenuItem onClick={closeUploadMenu}>
                                <label
                                    className={classes.uploadLabel}
                                    htmlFor='codalab-file-upload-input'
                                >
                                    File(s) Upload
                                </label>
                            </StyledMenuItem>
                            <StyledMenuItem onClick={closeUploadMenu}>
                                <label
                                    className={classes.uploadLabel}
                                    htmlFor='codalab-dir-upload-input'
                                >
                                    Folder Upload
                                </label>
                            </StyledMenuItem>
                        </Menu>
                    </span>
                ) : null}
                {!showBundleOperationButtons ? (
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='Add New Run'
                        onClick={onShowNewRun}
                        disabled={!editPermission}
                    >
                        <RunIcon className={classes.buttonIcon} />
                        Run
                    </Button>
                ) : null}
                {showBundleOperationButtons ? (
                    <BundleBulkActionMenu
                        handleSelectedBundleCommand={handleSelectedBundleCommand}
                        togglePopup={togglePopup}
                    />
                ) : null}
            </div>
        );
    }
}

const styles = (theme) => ({
    container: {
        position: 'relative',
        marginBottom: 20,
        zIndex: 5,
    },
    main: {
        zIndex: 10,
        border: `2px solid transparent`,
        '&:hover': {
            backgroundColor: theme.color.grey.lightest,
            border: `2px solid ${theme.color.grey.base}`,
        },
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
    uploadButton: {
        padding: 0,
    },
    uploadLabel: {
        width: '100%',
        display: 'inherit',
        padding: '4px 8px',
        marginBottom: 0,
        fontWeight: 'inherit',
        cursor: 'inherit',
    },
});

export default withStyles(styles)(ActionButtons);
