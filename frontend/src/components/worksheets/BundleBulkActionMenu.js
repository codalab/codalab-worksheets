import React from 'react';
import { withStyles } from '@material-ui/core';
import Typography from '@material-ui/core/Typography';
import DeleteForeverIcon from '@material-ui/icons/DeleteForever';
import HighlightOffIcon from '@material-ui/icons/HighlightOff';
import FileCopyOutlinedIcon from '@material-ui/icons/FileCopyOutlined';
import FileCopyIcon from '@material-ui/icons/FileCopy';
import DescriptionIcon from '@material-ui/icons/Description';
import Button from '@material-ui/core/Button';

class BundleBulkActionMenu extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            openDelete: false,
            openDetach: false,
            openKill: false,
            forceDelete: false,
            copyValue: '',
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
                    disabled={!this.props.editPermission}
                    onClick={this.props.toggleCmdDialog('rm')}
                >
                    <DeleteForeverIcon fontSize='small' />
                    <Typography variant='inherit'>Delete</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Kill'
                    disabled={!this.props.editPermission}
                    onClick={this.props.toggleCmdDialog('kill')}
                >
                    <HighlightOffIcon fontSize='small' />
                    <Typography variant='inherit'>Kill</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Copy'
                    onClick={this.props.toggleCmdDialog('copy')}
                    id='copy-button'
                >
                    <FileCopyIcon className={classes.buttonIcon} />
                    <Typography variant='inherit'>Copy</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Cut'
                    disabled={!this.props.editPermission}
                    onClick={this.props.toggleCmdDialog('cut')}
                >
                    <FileCopyOutlinedIcon className={classes.buttonIcon} />
                    <Typography variant='inherit'>Cut</Typography>
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Content'
                    disabled={!this.props.editPermission}
                    onClick={this.props.showBundleContent}
                >
                    <DescriptionIcon className={classes.buttonIcon} />
                    <Typography variant='inherit'>Content</Typography>
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
