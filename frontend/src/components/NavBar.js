import * as React from 'react';
import classNames from 'classnames';
import $ from 'jquery';
import Immutable from 'seamless-immutable';

import { withStyles } from '@material-ui/core/styles';
import { MuiThemeProvider } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import AppBar from '@material-ui/core/AppBar';
import Toolbar from '@material-ui/core/Toolbar';
import Button from '@material-ui/core/Button';
import Menu from '@material-ui/core/Menu';
import MenuItem from '@material-ui/core/MenuItem';
import ListSubheader from '@material-ui/core/ListSubheader';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import DashboardIcon from '@material-ui/icons/Dashboard'; // Home
import NewWorksheetIcon from '@material-ui/icons/NoteAdd';
import GalleryIcon from '@material-ui/icons/Public'; // FindInPage
import HowToIcon from '@material-ui/icons/Help'; // Info
import ContactIcon from '@material-ui/icons/Feedback';
import AccountIcon from '@material-ui/icons/AccountCircle';

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
            success: function(data) {
                var userInfo = data.data.attributes;
                userInfo.user_id = data.data.id;
                this.setState(
                    Immutable({
                        userInfo: userInfo,
                    }),
                );
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(xhr.responseText);
            },
        });
    }

    /** Renderer. */
    render() {
        const { classes } = this.props;
        const { accountEl } = this.state;

        if (this.props.auth.isAuthenticated && this.state.userInfo === undefined) {
            this.fetchName();
        }

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
                <AppBar color='default'>
                    <Toolbar>
                        <div className={classes.logoContainer}>
                            <a href='/' target='_self'>
                                <img
                                    src={`${process.env.PUBLIC_URL}/img/codalab-logo.png`}
                                    className={classes.logo}
                                    alt='CodaLab'
                                />
                            </a>
                        </div>
                        {!this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <Button color='inherit' href='/account/signup'>
                                    Sign Up
                                </Button>
                                <Button color='inherit' href='/account/login'>
                                    Login
                                </Button>
                            </React.Fragment>
                        )}
                        {this.props.auth.isAuthenticated && (
                            <React.Fragment>
                                <Tooltip title='Dashboard'>
                                    <IconButton href='/rest/worksheets/?name=dashboard'>
                                        <DashboardIcon />
                                    </IconButton>
                                </Tooltip>
                                <Tooltip title='New Worksheet'>
                                    <IconButton>
                                        <NewWorksheetIcon />
                                    </IconButton>
                                </Tooltip>
                            </React.Fragment>
                        )}
                        <Tooltip title='Gallery'>
                            <IconButton href='/rest/worksheets/?name=home'>
                                <GalleryIcon />
                            </IconButton>
                        </Tooltip>
                        <Tooltip title='How-To Guides'>
                            <IconButton href='https://github.com/codalab/codalab-worksheets/wiki'>
                                <HowToIcon />
                            </IconButton>
                        </Tooltip>
                        <Tooltip title='Contact'>
                            <IconButton href='mailto:codalab.worksheets@gmail.com'>
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
                                    <MenuItem href='/account/profile'>My Account</MenuItem>
                                    <MenuItem onClick={this.props.auth.signout} href='#'>
                                        Logout
                                    </MenuItem>
                                </Menu>
                            </React.Fragment>
                        )}
                    </Toolbar>
                </AppBar>
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
});

export default withStyles(styles)(NavBar);
