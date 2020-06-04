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
import { executeCommand } from '../util/cli_utils';
import DOMPurify from 'dompurify';

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
            value: '',
            isLoading: false,
            results: [],
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

    search(keyword) {
        const url = '/rest/interpret/wsearch';

        $.ajax({
            url: url,
            dataType: 'json',
            type: 'POST',
            cache: false,
            data: JSON.stringify({ keywords: [keyword] }),
            contentType: 'application/json; charset=utf-8',
            success: (data) => {
                console.log(data);
            },
            error: (xhr, status, err) => {
                console.error(xhr.responseText);
            },
        });
    }

    handleChange = (e, { value }) => this.setState({ value });

    handleResultSelect = (e, { result }) => {
        this.setState({ value: result.plaintextTitle || result.plaintextDescription });
        window.open('/worksheets/' + result.uuid, '_self');
    };

    initialState = { isLoading: false, results: [], value: '' };

    resultRenderer = ({ title, description }) => (
        <div key='content' className='content'>
            {title && <div dangerouslySetInnerHTML={{ __html: title }} className='title'></div>}
            {description && (
                <div
                    dangerouslySetInnerHTML={{ __html: description }}
                    className='description'
                ></div>
            )}
        </div>
    );

    handleSearchChange = (e, { value }) => {
        this.setState({ isLoading: true, value });

        setTimeout(() => {
            if (this.state.value.length < 1) return this.setState(this.initialState);
            const keywords = this.state.value.split(' ');
            const regexKeywords = keywords.join('|');
            const re = new RegExp(regexKeywords, 'gi');

            const url = '/rest/interpret/wsearch';

            $.ajax({
                url: url,
                dataType: 'json',
                type: 'POST',
                cache: false,
                data: JSON.stringify({ keywords: keywords }),
                contentType: 'application/json; charset=utf-8',
                success: (data) => {
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
                },
                error: (xhr, status, err) => {
                    console.error(xhr.responseText);
                },
            });
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
                                    placeholder='search worksheets...'
                                    resultRenderer={this.resultRenderer}
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
                            <IconButton href='https://codalab-worksheets.readthedocs.io/en/latest'>
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
