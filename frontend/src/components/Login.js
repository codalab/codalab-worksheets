import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { Redirect } from 'react-router-dom';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';

class Login extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({ redirectToReferrer: false, username: '', password: '' });
    }

    loginRequest = (e) => {
        e.preventDefault();
        this.props.auth.authenticate(
            { username: this.state.username, password: this.state.password },
            () =>
                this.setState(() => ({
                    redirectToReferrer: true,
                })),
        );
    };

    handleInputChange = (event) => {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState({
            [name]: value,
        });
    };

    render() {
        let from = { pathname: '/' };
        if (this.props.location.pathname && this.props.location.pathname != '/account/login') {
            from.pathname = this.props.location.pathname;
        }

        let { redirectToReferrer } = this.state;

        if (redirectToReferrer) return <Redirect to={from} />;

        return (
            <React.Fragment>
                <SubHeader title='Sign In' />
                <ContentWrapper>
                    {from.pathname != '/' && (
                        <p>You must log in to view the page at {from.pathname}</p>
                    )}
                    <form className='login' method='POST' onSubmit={this.loginRequest}>
                        <div className='form-group'>
                            <label htmlFor='id_login'>Login:</label>
                            <input
                                id='id_login'
                                className='form-control'
                                name='username'
                                placeholder='Username or e-mail'
                                type='text'
                                autoFocus=''
                                autoComplete='off'
                                value={this.state.username}
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <div className='form-group'>
                            <label htmlFor='id_password'>Password:</label>
                            <input
                                id='id_password'
                                className='form-control'
                                name='password'
                                placeholder='Password'
                                type='password'
                                autoComplete='off'
                                value={this.state.password}
                                onChange={this.handleInputChange}
                            />
                        </div>
                        {/* the above is almost certainly wrong, not sure how to fix*/}
                        <button type='submit'>Sign In</button>
                    </form>
                </ContentWrapper>
            </React.Fragment>
        );
    }
}

export default Login;
