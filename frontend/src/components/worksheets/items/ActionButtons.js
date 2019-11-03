// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleOutline';
import UploadIcon from '@material-ui/icons/CloudUploadOutlined';
import AddIcon from '@material-ui/icons/AddBoxOutlined';
import Popover from '@material-ui/core/Popover';
import MenuIcon from '@material-ui/icons/Menu';
import BundleBulkActionMenu from '../BundleBulkActionMenu';

class ActionButtons extends React.Component<{
    classes: {},
    onShowNewUpload: () => void,
    onShowNewRun: () => void,
    onShowNewText: () => void,
}> {
    render() {
        const { classes, onShowNewUpload, onShowNewRun, onShowNewText, anchorElBundle, setShowBundleOperation, handleSelectedBundleCommand } = this.props;
        return (
            <div
                onMouseMove={(ev) => {
                    ev.stopPropagation();
                }}
            >
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Add Text'
                    onClick={onShowNewText}
                >
                    <AddIcon className={classes.buttonIcon} />
                    Cell
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Add New Upload'
                    onClick={onShowNewUpload}
                >
                    <UploadIcon className={classes.buttonIcon} />
                    Upload
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Add New Run'
                    onClick={onShowNewRun}
                >
                    <RunIcon className={classes.buttonIcon} />
                    Run
                </Button>
                <BundleBulkActionMenu 
                    handleSelectedBundleCommand={handleSelectedBundleCommand} 
                    closeMenu={() => { setShowBundleOperation(null)}}/>
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
        }
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
});

export default withStyles(styles)(ActionButtons);
