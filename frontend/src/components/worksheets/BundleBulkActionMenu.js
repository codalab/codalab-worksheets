import React from 'react';
import MenuList from '@material-ui/core/MenuList';
import MenuItem from '@material-ui/core/MenuItem';
import Paper from '@material-ui/core/Paper';
import { withStyles } from '@material-ui/core';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import Typography from '@material-ui/core/Typography';
import ExitToAppIcon from '@material-ui/icons/ExitToApp';
import DeleteForeverIcon from '@material-ui/icons/DeleteForever';
import LibraryAddIcon from '@material-ui/icons/LibraryAdd';
import HighlightOffIcon from '@material-ui/icons/HighlightOff';
import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';

class BundleBulkActionMenu extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            openDelete: false,
            openAttach: false,
            openDetach: false,
            openKill: false,
        };
    }

    executeDeleteCommand = (ev) => {
        this.props.handleSelectedBundleCommand('rm');
        this.toggleDeletePopup();
        this.props.closeMenu();
    };

    executeDetachCommand = (ev) => {
        // this.props.handleSelectedBundleCommand('detach');
        // this.toggleDeletePopup();
        // Not fully implemented yet
        this.props.closeMenu();
    };

    executeAttachCommand = (ev) => {
        // this.props.handleSelectedBundleCommand('rm');
        // this.toggleAttachPopup();
        // Not fully implemented yet
        this.props.closeMenu();
    };

    executeKillCommand = (ev) => {
        //buggy
        this.props.handleSelectedBundleCommand('kill');
        this.toggleKillPopup();
        this.props.closeMenu();
    };

    toggleDeletePopup = () => {
        const { openDelete } = this.state;
        this.setState({
            openDelete: !openDelete,
        });
    }

    toggleAttachPopup = () => {
        const { openAttach } = this.state;
        this.setState({
            openAttach: !openAttach,
        });
    }

    toggleDetachPopup = () => {
        const { openDetach } = this.state;
        this.setState({
            openDetach: !openDetach,
        });
    }

    toggleKillPopup = () => {
        const { openKill } = this.state;
        this.setState({
            openKill: !openKill,
        });
    }

    render() {
        const {classes} = this.props;
        const {openDelete, openDetach, openAttach, openKill} = this.state;
        return <Paper className={classes.root}>
                <MenuList>
                    <MenuItem onClick={this.toggleDeletePopup}>
                        <ListItemIcon>
                            <DeleteForeverIcon fontSize="small" />
                        </ListItemIcon>
                        <Typography variant="inherit">Delete selected</Typography>
                    </MenuItem>
                    <MenuItem onClick={this.toggleDetachPopup}>
                        <ListItemIcon>
                            <ExitToAppIcon fontSize="small" />
                        </ListItemIcon>
                        <Typography variant="inherit">Detach selected</Typography>
                    </MenuItem>
                    <MenuItem onClick={this.toggleAttachPopup}>
                        <ListItemIcon>
                            <LibraryAddIcon fontSize="small" />
                        </ListItemIcon>
                        <Typography variant="inherit">Attach selected</Typography>
                    </MenuItem>
                    <MenuItem onClick={this.toggleKillPopup}>
                        <ListItemIcon>
                            <HighlightOffIcon fontSize="small" />
                        </ListItemIcon>
                        <Typography variant="inherit">Kill selected</Typography>
                    </MenuItem>
                </MenuList>
                <Dialog
                    open={openDelete}
                    onClose={this.toggleDeletePopup}
                    aria-labelledby="deletion-confirmation-title"
                    aria-describedby="deletion-confirmation-description"
                    >
                    <DialogTitle id="deletion-confirmation-title">{"Delete all selected bundle?"}</DialogTitle>
                    <DialogContent>
                        <DialogContentText id="alert-dialog-description">
                            Deletion cannot be undone.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.toggleDeletePopup}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.executeDeleteCommand} autoFocus>
                            DELETE
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={openDetach}
                    onClose={this.toggleDetachPopup}
                    aria-labelledby="detach-confirmation-title"
                    aria-describedby="detach-confirmation-description"
                    >
                    <DialogTitle id="detach-confirmation-title">{"Detach all selected bundle from this worksheet?"}</DialogTitle>
                    <DialogActions>
                        <Button color='primary' onClick={this.toggleDetachPopup}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.executeDetachCommand} autoFocus>
                            DETACH
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={openAttach}
                    onClose={this.toggleAttachPopup}
                    aria-labelledby="attach-confirmation-title"
                    aria-describedby="attach-confirmation-description"
                    >
                    <DialogTitle id="attach-confirmation-title">{"Attach all selected bundle to home worksheet?"}</DialogTitle>
                    <DialogActions>
                        <Button color='primary' onClick={this.toggleAttachPopup}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.executeAttachCommand} autoFocus>
                            ATTACH
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={openKill}
                    onClose={this.toggleKillPopup}
                    aria-labelledby="kill-confirmation-title"
                    aria-describedby="kill-confirmation-description"
                    >
                    <DialogTitle id="kill-confirmation-title">{"Kill all selected bundles if running?"}</DialogTitle>
                    <DialogContent>
                        <DialogContentText id="alert-dialog-description">
                            Only running bundles can be killed
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.toggleKillPopup}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.executeKillCommand} autoFocus>
                            KILL
                        </Button>
                    </DialogActions>
                </Dialog>
            </Paper>
    }
}

const styles = (theme) => ({
    root: {
        width: 200,
    },
});

export default withStyles(styles)(BundleBulkActionMenu);