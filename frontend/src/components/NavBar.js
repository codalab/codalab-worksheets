import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';

class NavBar extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return (
            <nav class='navbar navbar-default navbar-fixed-top' role='navigation'>
                <div class='container-fluid'>
                    <div class='navbar-header'>
                        <button
                            type='button'
                            class='navbar-toggle collapsed'
                            data-toggle='collapse'
                            data-target='#navbar_collapse'
                        >
                            <span class='sr-only'>Toggle navigation</span>
                            <span class='icon-bar' />
                            <span class='icon-bar' />
                            <span class='icon-bar' />
                        </button>
                        <a class='navbar-brand' href='/' tabIndex={1} target='_self'>
                            <img
                                src={process.env.PUBLIC_URL + '/img/codalab-logo.png'}
                                alt='Home'
                            />
                        </a>
                    </div>
                    <div class='collapse navbar-collapse' id='navbar_collapse'>
                        <ul class='nav navbar-nav navbar-right'>
                            <li>
                                <a href='/rest/worksheets/?name=home' tabIndex={2} target='_self'>
                                    Public Home
                                </a>
                            </li>
                            {this.props.auth.isAuthenticated && (
                                <li class='user-authenticated'>
                                    <a
                                        href='/rest/worksheets/?name=%2F'
                                        tabIndex={2}
                                        target='_self'
                                    >
                                        My Home
                                    </a>
                                </li>
                            )}
                            {this.props.auth.isAuthenticated && (
                                <li class='user-authenticated'>
                                    <a
                                        href='/rest/worksheets/?name=dashboard'
                                        tabIndex={2}
                                        target='_self'
                                    >
                                        My Dashboard
                                    </a>
                                </li>
                            )}
                            <li>
                                <a
                                    href='https://github.com/codalab/codalab-worksheets/wiki'
                                    target='_blank'
                                >
                                    Help
                                </a>
                            </li>
                            {this.props.auth.isAuthenticated && (
                                <li class="user-authenticated dropdown {% active request '/accounts/' %}">
                                    <a>
                                        <img
                                            src={
                                                process.env.PUBLIC_URL + '/img/icon_mini_avatar.png'
                                            }
                                            class='mini-avatar'
                                        />{' '}
                                        <span class='user-name' /> <span class='caret' />
                                    </a>
                                    <ul class='dropdown-menu' role='menu'>
                                        <li>
                                            <a href='/account/profile' target='_self'>
                                                My Account
                                            </a>
                                        </li>
                                        <li>
                                            <a
                                                onClick={this.props.auth.signout}
                                                href={
                                                    '/rest/account/logout?redirect_uri=' +
                                                    encodeURIComponent(this.props.location.pathname)
                                                }
                                                target='_self'
                                            >
                                                Sign Out
                                            </a>
                                        </li>
                                    </ul>
                                </li>
                            )}
                            {!this.props.auth.isAuthenticated && (
                                <li class='user-not-authenticated'>
                                    <a href='/account/signup' target='_self'>
                                        Sign Up
                                    </a>
                                </li>
                            )}
                            {!this.props.auth.isAuthenticated && (
                                <li class='user-not-authenticated'>
                                    <a href='/account/login' target='_self'>
                                        Sign In
                                    </a>
                                </li>
                            )}
                        </ul>
                    </div>
                </div>
            </nav>
        );
    }
}

export default NavBar;
