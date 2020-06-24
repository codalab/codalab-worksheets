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
import Grid from '@material-ui/core/Grid';
import CloseIcon from '@material-ui/icons/Close';
import { DIALOG_TYPES } from '../../constants';

class WorksheetDialogs extends React.Component {
    render() {
        const { classes } = this.props;
        return (
            <div>
                <Dialog
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_DELETE_BUNDLE}
                    onClose={this.props.closeDialog}
                    aria-labelledby='deletion-confirmation-title'
                    aria-describedby='deletion-confirmation-description'
                >
                    <DialogTitle id='deletion-confirmation-title'>
                        {'Delete selected bundles permanently?'}
                    </DialogTitle>
                    <DialogContent className={classes.dialog}>
                        <DialogContentText
                            id='alert-dialog-description'
                            className={classes.warning}
                        >
                            Bundle deletion cannot be undone.
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
                                title='Delete a bundle even if other bundles depend on it.'
                            >
                                <IconButton color='inherit'>
                                    <InfoIcon fontSize='small' />
                                </IconButton>
                            </Tooltip>
                        </DialogContentText>
                        {this.props.forceDelete ? (
                            <DialogContentText
                                id='alert-dialog-description'
                                className={classes.warning}
                            >
                                Delete a bundle even if other bundles depend on it.
                            </DialogContentText>
                        ) : null}
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.closeDialog}>
                            CANCEL
                        </Button>
                        <Button
                            color='primary'
                            variant='contained'
                            onClick={this.props.executeBundleCommand('rm')}
                        >
                            DELETE
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_KILL}
                    onClose={this.props.closeDialog}
                    aria-labelledby='kill-confirmation-title'
                    aria-describedby='kill-confirmation-description'
                >
                    <DialogTitle id='kill-confirmation-title'>
                        {'Kill selected bundles?'}
                    </DialogTitle>
                    <DialogContent>
                        <DialogContentText id='alert-dialog-description'>
                            Note: this might take a few seconds.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.closeDialog}>
                            CANCEL
                        </Button>
                        <Button
                            color='primary'
                            variant='contained'
                            onClick={this.props.executeBundleCommand('kill')}
                        >
                            KILL
                        </Button>
                    </DialogActions>
                </Dialog>
                <Dialog
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_DELETE_MARKDOWN}
                    onClose={this.props.toggleCmdDialog('deleteItem')}
                    aria-labelledby='deletion-confirmation-title'
                    aria-describedby='deletion-confirmation-description'
                >
                    <DialogTitle id='deletion-confirmation-title'>
                        {'Delete selected markdown?'}
                    </DialogTitle>
                    <DialogContent className={classes.dialog}>
                        <DialogContentText
                            id='alert-dialog-description'
                            className={classes.warning}
                        >
                            Deletion cannot be undone.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button color='primary' onClick={this.props.closeDialog}>
                            CANCEL
                        </Button>
                        <Button
                            color='primary'
                            variant='contained'
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
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_DELETE_WORKSHEET}
                    onClose={this.props.closeDialog}
                    aria-labelledby='delete-worksheet-confirmation-title'
                    aria-describedby='delete-worksheet-confirmation-description'
                >
                    <DialogTitle id='delete-worksheet-confirmation-title' style={{ color: 'red' }}>
                        Delete this worksheet permanently?
                    </DialogTitle>
                    <DialogContent>
                        <DialogContentText
                            id='alert-dialog-description'
                            style={{ color: 'red', marginBottom: '20px' }}
                        >
                            {'Worksheet deletion cannot be undone.'}
                        </DialogContentText>
                        <DialogContentText id='alert-dialog-description' style={{ color: 'grey' }}>
                            {'Note: Deleting a worksheet does not delete its bundles.'}
                        </DialogContentText>
                        <DialogActions>
                            <Button color='primary' onClick={this.props.closeDialog}>
                                CANCEL
                            </Button>
                            <Button
                                color='primary'
                                variant='contained'
                                onClick={this.props.deleteWorksheetAction}
                            >
                                DELETE
                            </Button>
                        </DialogActions>
                    </DialogContent>
                </Dialog>
                {/* Error message dialog */}
                <Dialog
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_ERROR_DIALOG}
                    onClose={this.props.toggleErrorMessageDialog}
                    aria-labelledby='error-title'
                    aria-describedby='error-description'
                >
                    <DialogTitle id='error-title'>
                        <Grid container direction='row'>
                            <Grid item xs={10}>
                                {'Error occurred'}
                            </Grid>
                            <Grid item xs={2}>
                                <Button
                                    variant='outlined'
                                    size='small'
                                    onClick={this.props.toggleErrorMessageDialog}
                                >
                                    <CloseIcon size='small' />
                                </Button>
                            </Grid>
                        </Grid>
                    </DialogTitle>
                    <DialogContent>
                        <DialogContentText id='alert-dialog-description' style={{ color: 'grey' }}>
                            {this.props.errorDialogMessage}
                        </DialogContentText>
                    </DialogContent>
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
        minHeight: 50,
    },
    warning: {
        color: 'red',
        marginBottom: 20,
    },
});

export default withStyles(styles)(WorksheetDialogs);
