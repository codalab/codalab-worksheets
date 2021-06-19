import * as React from 'react';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import Immutable from 'seamless-immutable';
import queryString from 'query-string';
import { NavLink } from 'react-router-dom';

export const PasswordResetSent = (props) => {
    return (
        <React.Fragment>
            <SubHeader title='Password Reset' />
            <ContentWrapper>
                <p>
                    We have sent you an e-mail. Please contact us if you do not receive it within a
                    few minutes.
                </p>
            </ContentWrapper>
        </React.Fragment>
    );
};

export const PasswordResetComplete = (props) => {
    return (
        <React.Fragment>
            <SubHeader title='Password Reset' />
            <ContentWrapper>
                <p>Your password has been updated.</p>
                {!props.auth.isAuthenticated && (
                    <p>
                        You may go ahead and <NavLink to='/account/login'>log in</NavLink> now.
                    </p>
                )}
            </ContentWrapper>
        </React.Fragment>
    );
};

export class PasswordReset extends React.Component {
    constructor(props) {
        super(props);
        this.state = Immutable({ form: {} });
    }

    handleInputChange = (event) => {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState({
            form: { [name]: value },
        });
    };

    render() {
        const { error } = queryString.parse(this.props.location.search);
        return (
            <React.Fragment>
                <SubHeader title='Password Reset' />
                <ContentWrapper>
                    {this.props.auth.isAuthenticated && (
                        <p className='user-authenticated'>You're already logged in!</p>
                    )}
                    {!this.props.auth.isAuthenticated && (
                        <React.Fragment>
                            <p className='user-not-authenticated'>
                                Forgot your password? Enter your e-mail address below, and we'll
                                send you an e-mail allowing you to reset it.
                            </p>
                            <div className='user-not-authenticated row'>
                                <div className='col-md-6'>
                                    {error && <div className='alert alert-error'>{error}</div>}
                                    <form method='POST' action='/rest/account/reset'>
                                        <div className='form-group'>
                                            <div className='form-group'>
                                                <label htmlFor='id_email'>Email:</label>
                                                <input
                                                    id='id_email'
                                                    className='form-control'
                                                    name='email'
                                                    placeholder='Email address'
                                                    type='email'
                                                    autoFocus
                                                    required
                                                    value={this.state.form.email}
                                                    onChange={this.handleInputChange}
                                                />
                                            </div>
                                        </div>
                                        <input
                                            className='btn btn-primary margin-top'
                                            type='submit'
                                            value='Request Password Reset'
                                        />
                                    </form>
                                </div>
                            </div>
                        </React.Fragment>
                    )}
                </ContentWrapper>
            </React.Fragment>
        );
    }
}

export class PasswordResetVerified extends React.Component {
    constructor(props) {
        super(props);
        this.state = Immutable({ form: {} });
    }

    handleInputChange = (event) => {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState(
            Immutable({
                form: { [name]: value },
            }),
        );
    };

    render() {
        const { error, code, code_valid } = queryString.parse(this.props.location.search);
        return (
            <React.Fragment>
                <SubHeader title='Password Reset' />
                <ContentWrapper>
                    {code_valid === 'False' && (
                        <p className='alert alert-error'>
                            {
                                'The password reset link was invalid, possibly because it has already been used. Please request a '
                            }
                            <NavLink to='/account/reset'>new password reset</NavLink>.
                        </p>
                    )}
                    <div className='row'>
                        <div className='col-sm-6'>
                            {error && <div className='alert alert-error'>{error}</div>}
                            <form method='POST' action='/rest/account/reset/finalize'>
                                <div className='form-group'>
                                    <label htmlFor='id_password1'>New Password:</label>
                                    <input
                                        id='id_password1'
                                        name='password'
                                        placeholder='Password'
                                        type='password'
                                        className='form-control'
                                        autoFocus
                                        value={this.state.form.password}
                                        onChange={this.handleInputChange}
                                    />
                                </div>
                                <div className='form-group'>
                                    <label htmlFor='id_password2'>Confirm New Password:</label>
                                    <input
                                        id='id_password2'
                                        name='confirm_password'
                                        placeholder='Password'
                                        type='password'
                                        className='form-control'
                                        value={this.state.form.passwordConfirm}
                                        onChange={this.handleInputChange}
                                    />
                                </div>
                                <input name='code' type='hidden' value={code} />
                                <input
                                    type='submit'
                                    className='btn btn-primary margin-top'
                                    name='action'
                                    value='Set New Password'
                                />
                            </form>
                        </div>
                    </div>
                </ContentWrapper>
            </React.Fragment>
        );
    }
}
