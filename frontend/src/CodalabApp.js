import React from 'react';
import { Router, Route, Redirect, Switch } from 'react-router-dom';
import { CookiesProvider } from 'react-cookie';

import { MuiThemeProvider } from '@material-ui/core/styles';
import CodalabTheme from './theme';

// Components
import UserInfo from './components/UserInfo';
import NavBar from './components/NavBar';
import Footer from './components/Footer';
import Login from './components/Login';
import { SignUp, SignUpSuccess } from './components/SignUp';
import { ChangeEmail, ChangeEmailSuccess } from './components/ChangeEmail';
import VerifySuccess from './components/VerifySuccess';
import VerifyError from './components/VerifyError';
import Worksheet from './components/worksheets/Worksheet/Worksheet';
import WorksheetNameSearch from './components/worksheets/WorksheetNameSearch';
import {
    PasswordReset,
    PasswordResetSent,
    PasswordResetVerified,
    PasswordResetComplete,
} from './components/PasswordReset';
import NewDashboard from './components/Dashboard/NewDashboard';

// Routes
import HomePage from './routes/HomePage';
import BundleRoute from './routes/BundleRoute';
import StoreRoute from './routes/StoreRoute';

import history from './history';
import Cookies from 'universal-cookie';
import DashboardRoute from './routes/DashboardRoute';

function CodalabApp() {
    return (
        <CookiesProvider>
            <MuiThemeProvider theme={CodalabTheme}>
                <Router history={history}>
                    <React.Fragment>
                        {/*NavBar. Rendered as a route on all pages so it can access the navigation props.*/}
                        <Route path='/' render={(props) => <NavBar {...props} auth={fakeAuth} />} />

                        {/*Main Content.*/}
                        <Switch>
                            <Route
                                path='/'
                                exact
                                render={(props) => (
                                    <HomePage
                                        {...props}
                                        auth={fakeAuth}
                                        redirectAuthToProfile={true}
                                    />
                                )}
                            />
                            <Route
                                path='/home'
                                exact
                                render={(props) => (
                                    <HomePage
                                        {...props}
                                        auth={fakeAuth}
                                        redirectAuthToProfile={false}
                                    />
                                )}
                            />
                            <Route path='/account/signup/success' component={SignUpSuccess} />
                            <Route path='/account/verify/error' component={VerifyError} />
                            <Route
                                path='/account/verify/success'
                                render={(props) => <VerifySuccess {...props} auth={fakeAuth} />}
                            />
                            <Route
                                path='/account/reset/verified'
                                component={PasswordResetVerified}
                            />
                            <Route path='/account/reset/sent' component={PasswordResetSent} />
                            <Route
                                path='/account/reset/complete'
                                render={(props) => (
                                    <PasswordResetComplete {...props} auth={fakeAuth} />
                                )}
                            />
                            <Route
                                path='/account/reset'
                                component={(props) => <PasswordReset {...props} auth={fakeAuth} />}
                            />
                            <Route
                                path='/account/signup'
                                component={(props) => <SignUp {...props} auth={fakeAuth} />}
                            />
                            <Route
                                path='/account/changeemail/sent'
                                component={ChangeEmailSuccess}
                            />
                            <Route
                                path='/account/changeemail'
                                component={(props) => <ChangeEmail {...props} auth={fakeAuth} />}
                            />
                            <Route
                                path='/account/login'
                                render={(props) => <Login {...props} auth={fakeAuth} />}
                            />
                            <PrivateRoute path='/account/profile' component={UserInfo} />
                            <Route path='/worksheets/:uuid/:bundle_uuid?' component={Worksheet} />
                            <Route
                                path='/worksheets'
                                render={(props) => (
                                    <WorksheetNameSearch {...props} auth={fakeAuth} />
                                )}
                            />
                            <Route path='/bundles/:uuid' component={BundleRoute} />
                            <Route
                                path='/users/:username'
                                component={(props) => <DashboardRoute {...props} auth={fakeAuth} />}
                            />
                            <Route
                                path='/users'
                                render={(props) => <NewDashboard {...props} auth={fakeAuth} />}
                            />
                            <Route path='/stores/:uuid' component={StoreRoute} />
                            <Route component={PageNotFound} />
                        </Switch>
                        {/*Footer.*/}
                        <Footer />
                    </React.Fragment>
                </Router>
            </MuiThemeProvider>
        </CookiesProvider>
    );
}

function checkAuth() {
    let codalab_session = new Cookies().get('codalab_session');
    return codalab_session !== undefined;
}

const fakeAuth = {
    isAuthenticated: checkAuth(),
    signout: (event) => {
        fakeAuth.isAuthenticated = false;
        new Cookies().remove('codalab_session');
        window.location.href =
            '/rest/account/logout?redirect_uri=' + encodeURIComponent(history.location.pathname);
        event.preventDefault();
    },
};

const PrivateRoute = ({ component: Component, ...rest }) => (
    <Route
        {...rest}
        component={(props) =>
            fakeAuth.isAuthenticated ? (
                <Component {...props} />
            ) : (
                <Redirect
                    to={{
                        pathname: '/account/login',
                        state: { from: props.location },
                    }}
                />
            )
        }
    />
);

function PageNotFound() {
    return <div>404 Page Not Found</div>;
}

export default CodalabApp;
