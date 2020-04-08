import React from 'react';
import { withStyles } from '@material-ui/core';
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import DialogActions from '@material-ui/core/DialogActions';
import Checkbox from '@material-ui/core/Checkbox';
import Tooltip from '@material-ui/core/Tooltip';
import IconButton from '@material-ui/core/IconButton';
import InfoIcon from '@material-ui/icons/InfoOutlined';
import Button from '@material-ui/core/Button';
import { CopyToClipboard } from 'react-copy-to-clipboard';
import TextField from '@material-ui/core/TextField';

class WorksheetDialogs extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            pasteInputValue: '',
        };
    }
    render() {
        const { classes } = this.props;
        return (
            <div>
                <Dialog
                    open={this.props.openDelete}
                    onClose={this.props.toggleCmdDialog('rm')} //{this.props.toggleDeletePopup}
                    aria-labelledby='deletion-confirmation-title'
                    aria-describedby='deletion-confirmation-description'
                >
                    <DialogTitle id='deletion-confirmation-title'>
                        {'Delect selected bundles permanently?'}
                    </DialogTitle>
                    <DialogContent className={classes.dialog}>
                        <DialogContentText id='alert-dialog-description'>
                            Deletion cannot be undone.
                        </DialogContentText>
                        <DialogContentText id='alert-dialog-description'>
                            Force delete?
                            <Checkbox
                                checked={this.props.forceDelete}
                                onChange={this.props.handleForceDelete}
                                value='checkedA'
                                inputProps={{
                                    'aria-label': 'primary checkbox',
                                }}
                            />
                            <Tooltip
                                disableFocusListener
                                disableTouchListener
                                title='Force deletion will ignore all bundle dependencies'
                            >
                                <IconButton color='inherit'>
                                    <InfoIcon fontSize='small' />
                                </IconButton>
                            </Tooltip>
                        </DialogContentText>
                        {this.props.forceDelete ? (
                            <DialogContentText
                                id='alert-dialog-description'
                                style={{ color: 'red' }}
                            >
                                The deletion will ignore all bundle dependencies
                            </DialogContentText>
                        ) : null}
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.toggleCmdDialog('rm')}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.props.executeBundleCommand('rm')}>
                            DELETE
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openDetach}
                    onClose={this.props.toggleCmdDialog('detach')}
                    aria-labelledby='detach-confirmation-title'
                    aria-describedby='detach-confirmation-description'
                >
                    <DialogTitle id='detach-confirmation-title'>
                        {'Detach all selected bundle from this worksheet?'}
                    </DialogTitle>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.toggleCmdDialog('detach')}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.props.executeBundleCommand('detach')}>
                            DETACH
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openKill}
                    onClose={this.props.toggleCmdDialog('kill')}
                    aria-labelledby='kill-confirmation-title'
                    aria-describedby='kill-confirmation-description'
                >
                    <DialogTitle id='kill-confirmation-title'>
                        {'Kill all selected bundles if running?'}
                    </DialogTitle>
                    <DialogContent>
                        <DialogContentText id='alert-dialog-description'>
                            It may take a few seconds to finish killing. <br /> Only running bundles
                            can be killed.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.toggleCmdDialog('kill')}>
                            CANCEL
                        </Button>
                        <Button color='primary' onClick={this.props.executeBundleCommand('kill')}>
                            KILL
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openDeleteItem}
                    onClose={this.props.toggleCmdDialog('deleteItem')}
                    aria-labelledby='deletion-confirmation-title'
                    aria-describedby='deletion-confirmation-description'
                >
                    <DialogTitle id='deletion-confirmation-title'>
                        {'Delect selected markdown block?'}
                    </DialogTitle>
                    <DialogContent className={classes.dialog}>
                        <DialogContentText id='alert-dialog-description' style={{ color: 'red' }}>
                            Deletion cannot be undone.
                        </DialogContentText>
                        <DialogContentText id='alert-dialog-description'>
                            You can modify the source to delete multiple blocks at once.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.toggleCmdDialog('deleteItem')}>
                            CANCEL
                        </Button>
                        <Button
                            color='primary'
                            onClick={() => {
                                this.props.deleteItemCallback();
                                this.props.toggleCmdDialogNoEvent('deleteItem');
                            }}
                        >
                            DELETE
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openCopy}
                    onClose={this.props.toggleCmdDialog('copy')}
                    aria-labelledby='copy-title'
                    aria-describedby='deletion-description'
                >
                    <DialogContent className={classes.copyDialog}>
                        <DialogContentText id='alert-dialog-description'>
                            The following bundle ids (excluding invalid ones) will be copied to
                            clipboard:
                            <div style={{ whiteSpace: 'pre-wrap' }}>
                                {this.props.copiedBundleIds.display}
                            </div>
                            You can use "paste" to move the copied bundles.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.toggleCmdDialog('copy')}>
                            CANCEL
                        </Button>
                        <CopyToClipboard
                            color='primary'
                            text={this.props.copiedBundleIds.actualContent}
                            id='copyBundleIdToClipBoard'
                            onCopy={() => {
                                window.localStorage.setItem(
                                    'CopiedBundles',
                                    this.props.copiedBundleIds.actualContent.substr(
                                        0,
                                        this.props.copiedBundleIds.actualContent.length - 1,
                                    ),
                                );
                            }}
                        >
                            <Button color='primary' onClick={this.props.toggleCmdDialog('copy')}>
                                Copy
                            </Button>
                        </CopyToClipboard>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openPaste}
                    onClose={this.props.toggleCmdDialog('paste')}
                    aria-labelledby='paste-title'
                    aria-describedby='paste-description'
                >
                    <DialogContent className={classes.copyDialog}>
                        <DialogContentText id='alert-dialog-description'>
                            {
                                'Paste the bundle ids (in form of []{uuid}) or other contents to the box below'
                            }
                        </DialogContentText>
                        <TextField
                            autoFocus
                            margin='dense'
                            id='paste-box'
                            onChange={(e) => {
                                this.setState({ pasteInputValue: e.target.value });
                            }}
                            multiline
                        />
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.toggleCmdDialog('paste')}>
                            CANCEL
                        </Button>
                        <Button
                            color='primary'
                            onClick={() => {
                                this.props.pasteToWorksheet(this.state.pasteInputValue);
                                this.props.toggleCmdDialogNoEvent('paste');
                            }}
                        >
                            Paste
                        </Button>
                    </DialogActions>
                </Dialog>
                }
            </div>
        );
    }
}

const styles = () => ({
    root: {
        width: 120,
        display: 'inline',
        padding: 2,
    },
    dialog: {
        width: 400,
        height: 120,
    },
    copyDialog: {
        width: 450,
        height: 200,
    },
});

export default withStyles(styles)(WorksheetDialogs);
