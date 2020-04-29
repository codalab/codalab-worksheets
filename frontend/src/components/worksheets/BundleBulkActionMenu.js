import React from 'react';
import { withStyles } from '@material-ui/core';
import Typography from '@material-ui/core/Typography';
import ExitToAppIcon from '@material-ui/icons/ExitToApp';
import DeleteForeverIcon from '@material-ui/icons/DeleteForever';
import HighlightOffIcon from '@material-ui/icons/HighlightOff';
import Button from '@material-ui/core/Button';

class BundleBulkActionMenu extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            openDelete: false,
            openDetach: false,
            openKill: false,
            forceDelete: false,
        };
    }

    handleCheckboxChange = (event) => {
        this.setState({ forceDelete: event.target.checked });
    };

    render() {
        const { classes } = this.props;
        return (
            <div className={classes.root}>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Delete'
                    onClick={this.props.togglePopup('rm')}
                >
                    <DeleteForeverIcon fontSize='small' />
                    <Typography variant='inherit'>Delete</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Detach'
                    onClick={this.props.togglePopup('detach')}
                >
                    <ExitToAppIcon fontSize='small' />
                    <Typography variant='inherit'>Detach</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Kill'
                    onClick={this.props.togglePopup('kill')}
                >
                    <HighlightOffIcon fontSize='small' />
                    <Typography variant='inherit'>Kill</Typography>
                </Button>
            </div>
        );
    }
}

const styles = (theme) => ({
    root: {
        width: 120,
        display: 'inline',
        padding: 2,
    },
    dialog: {
        width: 400,
        height: 120,
    },
});

export default withStyles(styles)(BundleBulkActionMenu);
