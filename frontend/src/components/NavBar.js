import * as React from 'react';
import classNames from 'classnames';
import $ from 'jquery';

import { Link } from 'react-router-dom';
import { withStyles } from '@material-ui/core/styles';
import { MuiThemeProvider } from '@material-ui/core/styles';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import ListSubheader from '@material-ui/core/ListSubheader';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import TextField from '@material-ui/core/TextField';
import Dialog from '@material-ui/core/Dialog';
import DialogActions from '@material-ui/core/DialogActions';
import DialogContent from '@material-ui/core/DialogContent';
import DialogContentText from '@material-ui/core/DialogContentText';
import DialogTitle from '@material-ui/core/DialogTitle';
import Snackbar from '@material-ui/core/Snackbar';
import SnackbarContent from '@material-ui/core/SnackbarContent';

import NewWorksheetIcon from '@material-ui/icons/NoteAdd';
import GalleryIcon from '@material-ui/icons/Public'; // FindInPage
import HowToIcon from '@material-ui/icons/Help'; // Info
import ContactIcon from '@material-ui/icons/Feedback';
import AccountIcon from '@material-ui/icons/AccountCircle';
import CloseIcon from '@material-ui/icons/Close';
import SuccessIcon from '@material-ui/icons/CheckCircle';
import ErrorIcon from '@material-ui/icons/Error';
import InfoIcon from '@material-ui/icons/Info';
import WarningIcon from '@material-ui/icons/Warning';

import { executeCommand } from '../util/cli_utils';

const kDefaultWorksheetName = 'unnamed';

class NavBar extends React.Component<{
    auth: {
        isAuthenticated: boolean,
        signout: () => void,
    },
}> {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            accountEl: null,
            newWorksheetShowDialog: false,
            newWorksheetName: '',
            userInfo: {},
            snackbarShow: false,
            snackbarMessage: '',
            snackbarVariant: '',
        };
    }

    componentDidMount() {
        // Initialize history stack
        this.fetchName();
    }

    fetchName() {
        $.ajax({
            url: '/rest/user',
            dataType: 'json',
            cache: false,
            type: 'GET',
            success: (data) => {
                const userInfo = data.data.attributes;
                userInfo.user_id = data.data.id;
                this.setState({ userInfo: userInfo, newWorksheetName: `${userInfo.user_name}-` });
            },
            error: (xhr, status, err) => {
                console.error(xhr.responseText);
            },
        });
    }

    resetDialog() {
        this.setState({
            newWorksheetShowDialog: false,
            newWorksheetName: `${this.state.userInfo.user_name}-`,
        });
    }

    createNewWorksheet() {
        this.resetDialog();
        executeCommand(`new ${this.state.newWorksheetName || kDefaultWorksheetName}`)
            .then((data) => {
                if (data.structured_result && data.structured_result.ui_actions) {
                    data.structured_result.ui_actions.forEach(([action, param]) => {
                        if (action === 'openWorksheet') {
                            window.location.href = `/worksheets/${param}`;
                        }
                    });
                }
            })
            .fail((error) => {
                this.setState({
                    snackbarShow: true,
                    snackbarMessage: error.responseText,
                    snackbarVariant: 'error',
                });
            });
    }

    /** Renderer. */
    render() {
        const { classes } = this.props;
        const { accountEl } = this.state;

        if (this.props.auth.isAuthenticated && this.state.userInfo === undefined) {
            this.fetchName();
        }

        let SnackbarIcon = {
            error: ErrorIcon,
            success: SuccessIcon,
            info: InfoIcon,
            warning: WarningIcon,
        }[this.state.snackbarVariant];

        return (
            <MuiThemeProvider
                theme={{
                    overrides: {
                        MuiIconButton: {
                            root: {
                                padding: 12,
                            },
                        },
                    },
                }}
            >
                <AppBar id='codalab-app-bar' color='default'>
                    <Toolbar>
                        <div className={classes.logoContainer}>
                            <Link to='/home'>
                                <img
                                    src={`${process.env.PUBLIC_URL}/img/codalab-logo.png`}
                                    className={classes.logo}
                                    alt='CodaLab'
                                />
                            </Link>
                        </div>
                        {!this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <Link to='/account/signup'>
                                    <Button color='inherit'>Sign Up</Button>
                                </Link>
                                <Link to='/account/login'>
                                    <Button color='inherit'>Login</Button>
                                </Link>
                            </React.Fragment>
                        )}
                        {this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <Link to='/worksheets?name=dashboard'>
                                    <Button color='primary'>Dashboard</Button>
                                </Link>
                                <Tooltip title='New Worksheet'>
                                    <IconButton
                                        onClick={() =>
                                            this.setState({ newWorksheetShowDialog: true })
                                        }
                                    >
                                        <NewWorksheetIcon />
                                    </IconButton>
                                </Tooltip>
                            </React.Fragment>
                        )}
                        <Tooltip title='Gallery'>
                            <Link to='/worksheets?name=home'>
                                <IconButton>
                                    <GalleryIcon />
                                </IconButton>
                            </Link>
                        </Tooltip>
                        <Tooltip title='How-To Guides'>
                            <IconButton href='https://github.com/codalab/codalab-worksheets/wiki'>
                                <HowToIcon />
                            </IconButton>
                        </Tooltip>
                        <Tooltip title='Bugs/Issues'>
                            <IconButton
                                href='https://github.com/codalab/codalab-worksheets/issues'
                                target='_blank'
                            >
                                <ContactIcon />
                            </IconButton>
                        </Tooltip>
                        {this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <Tooltip title='Account'>
                                    <IconButton
                                        aria-owns={accountEl ? 'account-menu' : undefined}
                                        aria-haspopup='true'
                                        onClick={(e) =>
                                            this.setState({ accountEl: e.currentTarget })
                                        }
                                    >
                                        <AccountIcon />
                                    </IconButton>
                                </Tooltip>
                                <Menu
                                    id='account-menu'
                                    anchorEl={accountEl}
                                    open={Boolean(accountEl)}
                                    onClose={() => this.setState({ accountEl: null })}
                                >
                                    <ListSubheader>
                                        {this.state.userInfo && this.state.userInfo.user_name}
                                    </ListSubheader>
                                    <MenuItem
                                        onClick={() => (window.location.href = '/account/profile')}
                                    >
                                        My Account
                                    </MenuItem>
                                    <MenuItem onClick={this.props.auth.signout}>Logout</MenuItem>
                                </Menu>
                            </React.Fragment>
                        )}
                    </Toolbar>
                </AppBar>
                {/** =============================================================================================== */}
                <Dialog
                    open={this.state.newWorksheetShowDialog}
                    onClose={() => this.resetDialog()}
                    aria-labelledby='form-dialog-title'
                >
                    <DialogTitle id='form-dialog-title'>New Worksheet</DialogTitle>
                    <DialogContent>
                        <DialogContentText>
                            To create a new worksheet, give it a name.
                        </DialogContentText>
                        <TextField
                            autoFocus
                            margin='dense'
                            id='name'
                            label='Name'
                            fullWidth
                            value={this.state.newWorksheetName}
                            placeholder={this.state.newWorksheetName}
                            onChange={(e) => this.setState({ newWorksheetName: e.target.value })}
                            onKeyDown={(e) => {
                                if (e.keyCode === 13) {
                                    // ENTER shortcut
                                    e.preventDefault();
                                    this.createNewWorksheet();
                                } else if (e.keyCode === 27) {
                                    // ESC shortcut
                                    e.preventDefault();
                                    this.resetDialog();
                                }
                            }}
                        />
                    </DialogContent>
                    <DialogActions>
                        <Button onClick={() => this.resetDialog()} color='primary'>
                            Cancel
                        </Button>
                        <Button onClick={() => this.createNewWorksheet()} color='primary'>
                            Confirm
                        </Button>
                    </DialogActions>
                </Dialog>
                {/** =============================================================================================== */}
                <Snackbar
                    anchorOrigin={{
                        vertical: 'bottom',
                        horizontal: 'left',
                    }}
                    open={this.state.snackbarShow}
                    autoHideDuration={5000}
                    onClose={(e, reason) => {
                        if (reason !== 'clickaway') this.setState({ snackbarShow: false });
                    }}
                >
                    <SnackbarContent
                        className={classNames({
                            [classes.snackbarError]: this.state.snackbarVariant === 'error',
                            [classes.snackbarWarning]: this.state.snackbarVariant === 'warning',
                            [classes.snackbarInfo]: this.state.snackbarVariant === 'info',
                            [classes.snackbarSuccess]: this.state.snackbarVariant === 'success',
                        })}
                        message={
                            <span className={classes.snackbarMessage}>
                                {SnackbarIcon && <SnackbarIcon className={classes.snackbarIcon} />}
                                {this.state.snackbarMessage}
                            </span>
                        }
                        action={[
                            <IconButton
                                key='close'
                                aria-label='Close'
                                color='inherit'
                                className={classes.close}
                                onClick={() => this.setState({ snackbarShow: false })}
                            >
                                <CloseIcon />
                            </IconButton>,
                        ]}
                    />
                </Snackbar>
            </MuiThemeProvider>
        );
    }
}

const styles = (theme) => ({
    logoContainer: {
        flexGrow: 1,
    },
    logo: {
        maxHeight: 64,
    },
    snackbarMessage: {
        display: 'flex',
        alignItems: 'center',
    },
    snackbarIcon: {
        marginRight: theme.spacing.large,
    },
    snackbarError: {
        backgroundColor: theme.color.red.base,
    },
    snackbarWarning: {
        backgroundColor: theme.color.yellow.base,
    },
    snackbarInfo: {
        backgroundColor: theme.color.primary.base,
    },
    snackbarSuccess: {
        backgroundColor: theme.color.green.base,
    },
});

export default withStyles(styles)(NavBar);
