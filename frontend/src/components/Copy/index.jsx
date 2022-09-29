import React from 'react';
import { CopyToClipboard } from 'react-copy-to-clipboard';
import { withStyles } from '@material-ui/core/styles';
import IconButton from '@material-ui/core/IconButton';
import CloseIcon from '@material-ui/icons/Close';
import Snackbar from '@material-ui/core/Snackbar';
import CopyIcon from './CopyIcon';

/**
 * This component renders a copy icon that, when clicked, will copy specified
 * contentes to the user's clipboard.
 */
class Copy extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            snackbarIsOpen: false,
        };
    }

    handleOpenSnackbar = () => {
        this.setState({ snackbarIsOpen: true });
    };

    handleCloseSnackbar = () => {
        this.setState({ snackbarIsOpen: false });
    };

    snackbarAction = (
        <IconButton
            size='small'
            aria-label='close'
            color='inherit'
            onClick={this.handleCloseSnackbar}
        >
            <CloseIcon fontSize='small' />
        </IconButton>
    );

    render() {
        const { classes, message, text, style } = this.props;
        if (!message || !text) {
            return null;
        }

        return (
            <>
                <CopyToClipboard text={text}>
                    <div
                        className={classes.copyIconContainer}
                        style={style}
                        onClick={this.handleOpenSnackbar}
                    >
                        <CopyIcon />
                    </div>
                </CopyToClipboard>
                <Snackbar
                    classes={{ root: classes.snackbar }}
                    open={this.state.snackbarIsOpen}
                    autoHideDuration={6000}
                    onClose={this.handleCloseSnackbar}
                    message={message}
                    action={this.snackbarAction}
                />
            </>
        );
    }
}

const styles = () => ({
    copyIconContainer: {
        width: 12,
        minWidth: 12,
        cursor: 'pointer',
    },
    snackbar: {
        marginBottom: 40,
    },
});

export default withStyles(styles)(Copy);
