// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleOutline';
import UploadIcon from '@material-ui/icons/CloudUploadOutlined';
import AddIcon from '@material-ui/icons/AddBoxOutlined';
import NoteAddIcon from '@material-ui/icons/NoteAdd';
import BundleBulkActionMenu from '../BundleBulkActionMenu';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import Tooltip from '@material-ui/core/Tooltip';
import PlaylistAddIcon from '@material-ui/icons/PlaylistAdd';
import AddPhotoAlternateIcon from '@material-ui/icons/AddPhotoAlternate';
import ToggleOnIcon from '@material-ui/icons/ToggleOn';
import ToggleOffIcon from '@material-ui/icons/ToggleOff';

const StyledMenuItem = withStyles((theme) => ({
    root: {
        border: '2px solid #d3d4d5',
    },
}))(MenuItem);

class ActionButtons extends React.Component<{
    classes: {},
    onShowNewRun: () => void,
    onShowNewText: () => void,
    showUploadMenu: () => void,
}> {
    constructor(props) {
        super(props);
        this.state = {toggleon: true};
    }
    handleClick = (event) => {
        this.setState({ anchorEl: event.currentTarget });
    };

    handleClose = () => {
        this.setState({ anchorEl: null });
    };

    uploadNewImage = (e) => {
        document.querySelector('label[for=codalab-image-upload-input]').click();
    };

    handleHighlight = () => {
        this.setState({toggleon: !this.state.toggleon})
        this.props.syntaxHighlight(this.state.toggleon);
    }

    render() {
        const {
            classes,
            onShowNewRun,
            onShowNewText,
            onShowNewSchema,
            showUploadMenu,
            closeUploadMenu,
            uploadAnchor,
            handleSelectedBundleCommand,
            showBundleOperationButtons,
            toggleCmdDialog,
            toggleCmdDialogNoEvent,
            info,
            showPasteButton,
            showBundleContent,
            syntaxHighlight,
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
                            aria-controls='upload-menu'
                            aria-haspopup='true'
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
                            <StyledMenuItem key='file-upload-item' onClick={closeUploadMenu}>
                                <label
                                    className={classes.uploadLabel}
                                    htmlFor='codalab-file-upload-input'
                                >
                                    File(s) Upload
                                </label>
                            </StyledMenuItem>
                            <StyledMenuItem key='folder-upload-item' onClick={closeUploadMenu}>
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
                        editPermission={editPermission}
                        handleSelectedBundleCommand={handleSelectedBundleCommand}
                        toggleCmdDialog={toggleCmdDialog}
                        toggleCmdDialogNoEvent={toggleCmdDialogNoEvent}
                        showBundleContent={showBundleContent}
                    />
                ) : null}
                <Tooltip title='Paste cut/copied bundles to this worksheet'>
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='Paste'
                        onClick={toggleCmdDialog('paste')}
                        disabled={!editPermission || !showPasteButton}
                        id='paste-button'
                    >
                        <NoteAddIcon className={classes.buttonIcon} />
                        Paste bundles
                    </Button>
                </Tooltip>
                <Tooltip title='Add a new schema'>
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='schema'
                        onClick={onShowNewSchema}
                        disabled={!editPermission}
                        id='add-schema-button'
                    >
                        <PlaylistAddIcon className={classes.buttonIcon} />
                        Schema
                    </Button>
                </Tooltip>
                <Tooltip title='Add an image'>
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='image'
                        onClick={this.uploadNewImage}
                        disabled={!editPermission}
                        id='add-image-button'
                    >
                        <label htmlFor='codalab-image-upload-input'></label>
                        <AddPhotoAlternateIcon className={classes.buttonIcon} />
                        Image
                    </Button>
                </Tooltip>
                <Tooltip title='Syntax Highlighting'>
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='image'
                        onClick={this.handleHighlight}
                        disabled={!editPermission}
                        id='syntax-highlighting-button'
                    >
                        {this.state.toggleon ?
                        <ToggleOnIcon className={classes.buttonIcon} />  
                        :
                        <ToggleOffIcon className={classes.buttonIcon} />
                        }  
                        Syntax Highlighting
                    </Button>
                </Tooltip>
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
