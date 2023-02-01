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
import TextField from '@material-ui/core/TextField';

class WorksheetDialogs extends React.Component {
    constructor(props) {
        super(props);
        this.state = { deleteWorksheetCheck: false, pathValue: null };
    }

    toggleDeleteWorksheet = () => {
        this.setState({ deleteWorksheetCheck: !this.state.deleteWorksheetCheck });
    };

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
                        Delete this WORKSHEET permanently?
                    </DialogTitle>
                    <DialogContent>
                        <DialogContentText id='alert-dialog-check'>
                            <Checkbox
                                checked={this.state.deleteWorksheetCheck}
                                onChange={this.toggleDeleteWorksheet}
                                inputProps={{
                                    'aria-label': 'primary checkbox',
                                }}
                            />
                            {'Yes, I want to delete this worksheet permanently.'}
                        </DialogContentText>
                        <DialogContentText
                            id='alert-dialog-description'
                            style={{ color: 'red', marginLeft: '35px', marginBottom: '20px' }}
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
                                disabled={!this.state.deleteWorksheetCheck}
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
                                {'Error Occurred'}
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
                {/* Delete schema dialog */}
                <Dialog
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_DELETE_SCHEMA}
                    onClose={this.props.closeDialog}
                    aria-labelledby='delete-schema-confirmation-title'
                    aria-describedby='delete-schema-confirmation-description'
                >
                    <DialogTitle id='delete-schema-confirmation-title' style={{ color: 'red' }}>
                        Delete this schema permanently?
                    </DialogTitle>
                    <DialogContent>
                        <DialogContentText
                            id='alert-dialog-description'
                            style={{ color: 'red', marginBottom: '20px' }}
                        >
                            {'Schema deletion cannot be undone.'}
                        </DialogContentText>
                        <DialogContentText id='alert-dialog-description' style={{ color: 'grey' }}>
                            {'Note: Deleting a schema does not delete its bundles.'}
                        </DialogContentText>
                        <DialogActions>
                            <Button color='primary' onClick={this.props.closeDialog}>
                                CANCEL
                            </Button>
                            <Button
                                color='primary'
                                variant='contained'
                                onClick={this.props.deleteItemCallback}
                            >
                                DELETE
                            </Button>
                        </DialogActions>
                    </DialogContent>
                </Dialog>
                {/* Specify path for content block */}
                <Dialog
                    open={this.props.openedDialog === DIALOG_TYPES.OPEN_CREATE_CONTENT}
                    onClose={this.props.closeDialog}
                    aria-labelledby='create-content-block-title'
                    aria-describedby='create-content-block-description'
                >
                    <DialogTitle id='create-content-block-title' style={{ color: 'red' }}>
                        Specify the subpath inside the bundle you want to display content for
                    </DialogTitle>
                    <DialogContent>
                        <TextField
                            autoFocus
                            onChange={(event) => {
                                this.setState({ pathValue: event.target.value });
                            }}
                            margin='dense'
                            id='content-block-path'
                            label='Default Path: /'
                            fullWidth
                        />
                        <DialogActions>
                            <Button color='primary' onClick={this.props.closeDialog}>
                                CANCEL
                            </Button>
                            <Button
                                color='primary'
                                variant='contained'
                                onClick={this.props.showBundleContentCallback(this.state.pathValue)}
                            >
                                SUBMIT
                            </Button>
                        </DialogActions>
                    </DialogContent>
                </Dialog>
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
