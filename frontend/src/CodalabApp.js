import React from 'react';
import { Router, Route, Link, Redirect, withRouter, Switch } from 'react-router-dom';
import { CookiesProvider, withCookies } from 'react-cookie';
import UserInfo from './components/UserInfo';
import PublicHome from './components/PublicHome';
import $ from 'jquery';
import NavBar from './components/NavBar';
import Footer from './components/Footer';
import Login from './components/Login';
import history from './history';
import Cookies from 'universal-cookie';

////////////////////////////////////////////////////////////
// 1. Click the public page
// 2. Click the protected page
// 3. Log in
// 4. Click the back button, note the URL each time

function CodalabApp() {
    return (
        <CookiesProvider>
            <Router history={history}>
                <div>
                    <Route path='/' render={(props) => <NavBar {...props} auth={fakeAuth} />} />

                    <Switch>
                        <Route path='/' exact component={PublicHome} />
                        <Route path='/account/signup' component={Login} />
                        <Route
                            path='/account/login'
                            render={(props) => <Login {...props} auth={fakeAuth} />}
                        />
                        <PrivateRoute path='/account/profile' component={UserInfo} />
                        <Route component={NoPage} />
                    </Switch>

                    <Route path='/' render={(props) => <Footer {...props} />} />
                </div>
            </Router>
        </CookiesProvider>
    );
}

function checkAuth() {
    let codalab_session = new Cookies().get('codalab_session');
    console.log(codalab_session != undefined);
    return codalab_session != undefined;
}

const fakeAuth = {
    isAuthenticated: checkAuth(),
    authenticate(authObject, callback) {
        $.ajax({
            type: 'POST',
            url: '/rest/account/login',
            data: {
                username: authObject.username,
                password: authObject.password,
            },
            //or your custom data either as object {foo: "bar", ...} or foo=bar&...
            success: function(response, status, xhr) {
                fakeAuth.isAuthenticated = true;
                console.log(response);
                console.log(status);
                console.log(xhr);
                if (callback) {
                    callback();
                }
            },
        });
    },
    signout: (cb) => {
        this.isAuthenticated = false;
        new Cookies().remove('codalab_session');
        history.push(
            '/rest/account/logout?redirect_uri=' + encodeURIComponent(history.location.pathname),
        );
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

function NoPage() {
    return <div>404 No Page Exists</div>;
}

export default CodalabApp;
