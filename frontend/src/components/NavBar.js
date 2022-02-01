import * as React from 'react';
import classNames from 'classnames';
import $ from 'jquery';

import { Link } from 'react-router-dom';
import { withStyles } from '@material-ui/core/styles';
import { MuiThemeProvider, createMuiTheme } from '@material-ui/core/styles';
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
import Search from 'semantic-ui-react/dist/commonjs/modules/Search';
import _ from 'lodash';
import { getUser, executeCommand, navBarSearch, defaultErrorHandler } from '../util/apiWrapper';
import DOMPurify from 'dompurify';
import { NAME_REGEX } from '../constants';

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
            userInfo: this.props.userInfo,
            snackbarShow: false,
            snackbarMessage: '',
            snackbarVariant: '',
            value: '',
            isLoading: false,
            results: [],
            avatar: '',
        };
    }

    componentDidMount() {
        // Initialize history stack
        this.fetchName();
    }

    fetchName() {
        const callback = (data) => {
            const userInfo = data.data.attributes;
            userInfo.user_id = data.data.id;
            this.fetchImg(userInfo.avatar_id);
            this.setState({ userInfo: userInfo, newWorksheetName: `${userInfo.user_name}-` });
        };
        getUser()
            .then(callback)
            .catch(defaultErrorHandler);
    }

    resetDialog() {
        this.setState({
            newWorksheetShowDialog: false,
            newWorksheetName: `${this.state.userInfo.user_name}-`,
        });
    }

    async createNewWorksheet() {
        this.resetDialog();
        if (!NAME_REGEX.test(this.state.newWorksheetName)) {
            this.setState({
                snackbarShow: true,
                snackbarMessage: `Names must match ${NAME_REGEX}, was ${this.state.newWorksheetName}`,
                snackbarVariant: 'error',
            });
            return;
        }

        try {
            const data = await executeCommand(
                `new ${this.state.newWorksheetName || kDefaultWorksheetName}`,
            );
            if (data.structured_result && data.structured_result.ui_actions) {
                data.structured_result.ui_actions.forEach(([action, param]) => {
                    if (action === 'openWorksheet') {
                        window.location.href = `/worksheets/${param}`;
                    }
                });
            }
        } catch (error) {
            this.setState({
                snackbarShow: true,
                snackbarMessage: error,
                snackbarVariant: 'error',
            });
        }
    }

    // Fetch the image file represented by the bundle
    fetchImg(bundleUuid) {
        if (bundleUuid == null) return;
        // Set defaults
        let url = '/rest/bundles/' + bundleUuid + '/contents/blob/';

        fetch(url)
            .then(function(response) {
                if (response.ok) {
                    return response.arrayBuffer();
                }
                throw new Error('Network response was not ok.');
            })
            .then(function(data) {
                let dataUrl =
                    'data:image/png;base64,' +
                    btoa(
                        new Uint8Array(data).reduce(
                            (data, byte) => data + String.fromCharCode(byte),
                            '',
                        ),
                    );
                return dataUrl;
            })
            .then((dataUrl) => {
                // Update avatar shown on the page
                this.setState({
                    avatar: dataUrl,
                });
            })
            .catch(function(error) {
                console.log(url, error.responseText);
            });
    }

    handleResultSelect = (e, { result }) => {
        this.setState({ value: result.plaintextTitle || result.plaintextDescription });
        window.open('/worksheets/' + result.uuid, '_self');
    };

    initialState = { isLoading: false, results: [], value: '' };

    resultRenderer = ({ title, description }) => (
        <div key='content' className='content'>
            {title && <div dangerouslySetInnerHTML={{ __html: title }} className='title' />}
            {description && (
                <div dangerouslySetInnerHTML={{ __html: description }} className='description' />
            )}
        </div>
    );

    categoryRenderer = ({ name }) => {
        return (
            <Link target='_blank' to={`/users/${name}`}>
                <div>{name}</div>
            </Link>
        );
    };

    handleSearchFocus = () => {
        // Disable the terminal to avoid the search bar text being mirrored in the terminal
        const $cmd = $('#command_line');
        if ($cmd.length > 0) {
            if ($cmd.terminal().enabled()) {
                $cmd.terminal().focus(false);
            }
        }
    };

    handleSearchChange = (e, { value }) => {
        this.setState({ isLoading: true, value });

        setTimeout(() => {
            if (this.state.value.length < 1) return this.setState(this.initialState);
            const keywords = this.state.value.split(' ');
            const regexKeywords = keywords.join('|');
            const re = new RegExp(regexKeywords, 'gi');
            const callback = (data) => {
                /*
                    Response body:
                    ```
                    {
                        "response": [
                            {id: 6,
                            uuid: "0x5505f540936f4d0d919f3186141192b0",
                            name: "codalab-a",
                            title: "CodaLab Dashboard",
                            frozen: null,
                            owner_id: "0"
                            owner_name: "codalab"
                            group_permissions: {
                                id: 8,
                                group_uuid: "0x41e95d8592de417cbb726085d6986137",
                                group_name: "public",
                                permission: 1}
                            }
                            ...
                        ]
                    }
                    ```

                    turn the above response into the following dict
                    ```
                    {
                        "name": "codalab",
                        "results": [
                            {
                                "title": "Brakus Group",
                                "description": "Cloned interactive Graphic Interface",
                            },
                        ]
                    }
                    ```
                    */
                let filteredResults = {};
                for (let item of data.response) {
                    // use DOMPurify to get rid of the XSS security risk
                    item.plaintextDescription = item.name;
                    item.description = DOMPurify.sanitize(
                        item.name.replace(re, "<span id='highlight'>$&</span>"),
                    );
                    item.plaintextTitle = item.title;
                    item.title = DOMPurify.sanitize(
                        (item.title || '').replace(re, "<span id='highlight'>$&</span>"),
                    );

                    if (!(item.owner_name in filteredResults)) {
                        filteredResults[item.owner_name] = {
                            name: item.owner_name,
                            results: [],
                        };
                    }
                    filteredResults[item.owner_name].results.push(item);
                }

                /*
                    turn the above dict into a a dict with a key of category name,
                    e.g., codalab
                    {
                        "codalab": {
                            "name": "codalab",
                            "results": [
                                {
                                    "title": "Brakus Group",
                                    "description": "Cloned interactive Graphic Interface",
                                },
                            ]
                        },
                    */

                const preRanking = _.reduce(
                    filteredResults,
                    (memo, data, name) => {
                        memo[name] = { name, results: data.results };
                        return memo;
                    },
                    {},
                );

                // the results are displayed using the map function, which remembers
                // order of insertion. We therefore put the owner's worksheets on top
                const currName = this.state.userInfo.user_name;
                if (currName in preRanking) {
                    let ownerResults = {};
                    ownerResults[currName] = preRanking[currName];
                    delete preRanking[currName];
                    let finalResults = { ...ownerResults, ...preRanking };
                    this.setState({
                        isLoading: false,
                        results: finalResults,
                    });
                } else {
                    this.setState({
                        isLoading: false,
                        results: preRanking,
                    });
                }
            };
            navBarSearch(keywords)
                .then(callback)
                .catch(defaultErrorHandler);
        }, 300);
    };

    /** Renderer. */
    render() {
        const { classes } = this.props;
        const { accountEl, isLoading, value, results } = this.state;

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
            <MuiThemeProvider theme={overrideMedia}>
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
                        {this.props.auth.isAuthenticated && (
                            <div className={classes.searchContainer}>
                                <Search
                                    fluid
                                    category
                                    loading={isLoading}
                                    input={{ icon: 'search', iconPosition: 'left', fluid: true }}
                                    onResultSelect={this.handleResultSelect}
                                    onSearchChange={_.debounce(this.handleSearchChange, 500, {
                                        leading: true,
                                    })}
                                    onFocus={this.handleSearchFocus}
                                    placeholder='search worksheets...'
                                    resultRenderer={this.resultRenderer}
                                    categoryRenderer={this.categoryRenderer}
                                    results={results}
                                    value={value}
                                    showNoResults={true}
                                    id='codalab-search-bar'
                                />
                            </div>
                        )}
                        {!this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <div className={classes.searchContainer} />
                                <Link to='/account/signup'>
                                    <Button color='inherit'>Sign Up</Button>
                                </Link>
                                <Link
                                    to={{
                                        pathname: '/account/login',
                                        state: { from: this.props.location },
                                    }}
                                >
                                    <Button color='inherit'>Login</Button>
                                </Link>
                            </React.Fragment>
                        )}
                        {this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <Link to='/users'>
                                    <Button color='primary'>My Worksheets</Button>
                                </Link>
                                <Tooltip title='New Worksheet'>
                                    <IconButton
                                        disabled={
                                            !this.state.userInfo ||
                                            this.state.userInfo.has_access == 'False'
                                        }
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
                                <IconButton aria-label='Gallery'>
                                    <GalleryIcon />
                                </IconButton>
                            </Link>
                        </Tooltip>
                        <Tooltip title='How-To Guides'>
                            <IconButton href='https://codalab-worksheets.readthedocs.io/en/latest'>
                                <HowToIcon />
                            </IconButton>
                        </Tooltip>
                        <Tooltip title='Bugs/Issues'>
                            <IconButton
                                href='https://github.com/codalab/codalab-worksheets/issues'
                                target='_blank'
                                rel='noopener noreferrer'
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
                                        {this.state.avatar ? (
                                            <div>
                                                <img
                                                    src={this.state.avatar}
                                                    className={classes.avatar}
                                                    alt='CodaLab'
                                                />
                                            </div>
                                        ) : (
                                            <AccountIcon />
                                        )}
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
                        <DialogContentText>
                            Note: worksheet names must be globally unique and not contain any
                            spaces.
                        </DialogContentText>
                    </DialogContent>
                    <DialogActions>
                        <Button onClick={() => this.resetDialog()} color='primary'>
                            Cancel
                        </Button>
                        <Button
                            onClick={() => this.createNewWorksheet()}
                            variant='contained'
                            color='primary'
                        >
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

const overrideMedia = createMuiTheme({
    overrides: {
        MuiToolbar: {
            regular: {
                '@media(min-width: 0px) and (orientation: landscape)': {
                    minHeight: '36px',
                },
                '@media(min-width:600px)': {
                    minHeight: '42px',
                },
                height: '32px',
                minHeight: '32px',
            },
        },
        MuiIconButton: {
            root: {
                padding: 12,
            },
        },
    },
});

const styles = (theme) => ({
    logoContainer: {
        marginRight: 40,
    },
    searchContainer: {
        flexGrow: 1,
        marginRight: 20,
    },
    logo: {
        maxHeight: 40,
    },
    avatar: {
        maxHeight: 30,
        maxWidth: 30,
        borderRadius: 15,
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
