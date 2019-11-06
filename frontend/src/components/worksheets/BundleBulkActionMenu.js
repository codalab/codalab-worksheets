import React from 'react';
import { withStyles } from '@material-ui/core';
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
import Checkbox from '@material-ui/core/Checkbox';
import * as Mousetrap from '../../util/ws_mousetrap_fork';
import Tooltip from '@material-ui/core/Tooltip';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/InfoOutlined';

class BundleBulkActionMenu extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            openDelete: false,
            openAttach: false,
            openDetach: false,
            openKill: false,
            forceDelete: false,
        };
    }

    handleCheckboxChange = event => {
        this.setState({ forceDelete: event.target.checked });
    };


    executeDeleteCommand = () => {
        this.props.handleSelectedBundleCommand('rm', this.state.forceDelete);
        this.toggleDeletePopup();
    };

    executeDetachCommand = () => {
        // Not fully implemented
        this.props.handleSelectedBundleCommand('detach');
        this.toggleDetachPopup();
    };

    executeAttachCommand = () => {
        // Not fully implemented
        this.props.handleSelectedBundleCommand('add');
        this.toggleAttachPopup();
    };

    executeKillCommand = () => {
        //buggy
        this.props.handleSelectedBundleCommand('kill');
        this.toggleKillPopup();
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
        Mousetrap.bind(
            ['enter'],
            function(e) {
                //TODO: don't sue stopPropagation
                e.stopPropagation();
                if(this.state.openDelete){
                    this.executeDeleteCommand();
                }
                else if(this.state.openAttach){
                    this.executeAttachCommand();
                }
                else if(this.state.openDetach){
                    this.executeDetachCommand();
                }
                else if(this.state.openKill){
                    this.executeKillCommand();
                }
            }.bind(this),
        );
        const {classes} = this.props;
        const {openDelete, openDetach, openAttach, openKill} = this.state;
        return <div className={classes.root}>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Delete'
                    onClick={this.toggleDeletePopup}
                >
                    <DeleteForeverIcon fontSize="small" />
                    <Typography variant="inherit">Delete</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Detach'
                    onClick={this.toggleDetachPopup}
                >
                    <ExitToAppIcon fontSize="small" />
                    <Typography variant="inherit">Detach</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Attach'
                    onClick={this.toggleAttachPopup}
                    disabled
                >
                    <LibraryAddIcon fontSize="small" />
                    <Typography variant="inherit">Attach</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Kill'
                    onClick={this.toggleKillPopup}
                >
                    <HighlightOffIcon fontSize="small" />
                    <Typography variant="inherit">Kill</Typography>
                </Button>
                <Dialog
                    open={openDelete}
                    onClose={this.toggleDeletePopup}
                    aria-labelledby="deletion-confirmation-title"
                    aria-describedby="deletion-confirmation-description"
                    >
                    <DialogTitle id="deletion-confirmation-title">{"Deletion cannot be undone"}</DialogTitle>
                    <DialogContent className={classes.dialog}>
                        {/* <DialogContentText id="alert-dialog-description" style={{ color:'red' }}>
                            Deletion cannot be undone.
                        </DialogContentText> */}
                        <DialogContentText id="alert-dialog-description">
                            Force delete?
                            <Checkbox
                            checked={this.state.forceDelete}
                            onChange={this.handleCheckboxChange}
                            value="checkedA"
                            inputProps={{
                            'aria-label': 'primary checkbox',
                            }}
                            />
                            <Tooltip disableFocusListener disableTouchListener
                            title="Force deletion will ignore all bundle dependencies">
                                <IconButton
                                    color='inherit'
                                    >
                                    <InfoIcon fontSize='small' />
                                </IconButton>
                            </Tooltip>
                        </DialogContentText>
                        {this.state.forceDelete? <DialogContentText id="alert-dialog-description" style={{ color:'red' }}>
                            The deletion will ignore all bundle dependencies
                        </DialogContentText>:null}
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
                        It may take a few seconds to finish killing. <br/> Only running bundles can be killed. 
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
            </div>
    }
}

const styles = (theme) => ({
    root: {
        width: 120,
        display: 'inline',
        border: '1px solid',
        padding: 2,
    },
    dialog:{
        width: 400,
        height: 100,
    }
});

export default withStyles(styles)(BundleBulkActionMenu);