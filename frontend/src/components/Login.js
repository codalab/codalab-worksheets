import * as React from 'react';
import Immutable from 'seamless-immutable';
import { Redirect, NavLink } from 'react-router-dom';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import queryString from 'query-string';

class Login extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        let fromPathname = '/';
        if (this.props.location.state && this.props.location.state.from) {
            fromPathname = this.props.location.state.from.pathname;
        }

        this.state = Immutable({
            redirectToReferrer: false,
            username: '',
            password: '',
            from: fromPathname,
        });
    }

    handleInputChange = (event) => {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState({
            [name]: value,
        });
    };

    render() {
        const { error } = queryString.parse(this.props.location.search);
        const pathname = this.props.location.pathname;

        let { redirectToReferrer, from } = this.state;

        if (redirectToReferrer) return <Redirect to={from} />;

        return (
            <React.Fragment>
                <SubHeader title='Sign In' />
                <ContentWrapper>
                    {from !== '/' && <p>You must log in to view the page at {from}</p>}
                    {error && <div className='alert alert-error'>{error}</div>}
                    <form className='login' method='POST' action='/rest/account/login'>
                        <div className='form-group'>
                            <label htmlFor='id_login'>Login:</label>
                            <input
                                id='id_login'
                                className='form-control'
                                name='username'
                                placeholder='Username or e-mail'
                                type='text'
                                autoFocus={true}
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
                        <input type='hidden' name='success_uri' value={from} />
                        <input type='hidden' name='error_uri' value={pathname} />
                        <button className='btn btn-primary margin-top' type='submit'>
                            Sign In
                        </button>
                        {/* the above is almost certainly wrong, not sure how to fix*/}
                    </form>
                    <p>
                        <NavLink to='/account/signup'>Don't have an account? Sign up!</NavLink>
                    </p>
                    <p>
                        <NavLink to='/account/reset'>Forgot your password?</NavLink>
                    </p>
                    <button
                        className='link'
                        onClick={(event) => {
                            alert(
                                'Please log in and navigate to your dashboard to resend confirmation email.',
                            );
                            event.preventDefault();
                        }}
                    >
                        Resend confirmation email
                    </button>
                </ContentWrapper>
            </React.Fragment>
        );
    }
}

export default Login;
