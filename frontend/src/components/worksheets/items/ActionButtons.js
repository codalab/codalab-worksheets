// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleOutline';
import UploadIcon from '@material-ui/icons/CloudUploadOutlined';
import AddIcon from '@material-ui/icons/AddBoxOutlined';
import BundleBulkActionMenu from '../BundleBulkActionMenu';

class ActionButtons extends React.Component<{
    classes: {},
    onShowNewUpload: () => void,
    onShowNewRun: () => void,
    onShowNewText: () => void,
}> {
    render() {
        const {
            classes,
            onShowNewUpload,
            onShowNewRun,
            onShowNewText,
            handleSelectedBundleCommand,
            showBundleOperationButtons,
            togglePopup,
            togglePopupNoEvent,
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
                    <Button
                        size='small'
                        color='inherit'
                        aria-label='Add New Upload'
                        onClick={onShowNewUpload}
                        disabled={!editPermission}
                    >
                        <UploadIcon className={classes.buttonIcon} />
                        Upload
                    </Button>
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
                        togglePopupNoEvent={togglePopupNoEvent}
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
});

export default withStyles(styles)(ActionButtons);
