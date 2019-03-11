import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { Redirect } from 'react-router-dom';
import $ from 'jquery';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import queryString from 'query-string';

export const SignUpSuccess = (props) => {
    const { email } = queryString.parse(props.location.search);
    return (
        <React.Fragment>
            <SubHeader title='Signup Success' />
            <ContentWrapper>
                <div className='row'>
                    <div className='col-md-6'>
                        <h4>Thank you for signing up for a CodaLab account!</h4>
                        <p>A link to verify your account has been sent to {email}.</p>
                    </div>
                </div>
            </ContentWrapper>
        </React.Fragment>
    );
};

export class SignUp extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            form: {},
        });
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
        const { error } = queryString.parse(this.props.location.search);
        return (
            <React.Fragment>
                <SubHeader title='Sign Up' />
                <ContentWrapper>
                    <p>
                        Already have an account? Then please <a href='/account/login'>sign in</a>.
                    </p>
                    <form
                        className='signup'
                        id='signup_form'
                        method='post'
                        action='/rest/account/signup'
                    >
                        {error && <div class='alert alert-error'>{error}</div>}
                        <div className='form-group'>
                            <label htmlFor='id_email'>Email:</label>
                            <input
                                id='id_email'
                                className='form-control'
                                name='email'
                                placeholder='Email'
                                type='email'
                                value={this.state.form.email}
                                required
                                autoFocus
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <div className='form-group'>
                            <label htmlFor='id_login'>Username:</label>
                            <input
                                id='id_login'
                                className='form-control'
                                name='username'
                                placeholder='Username'
                                type='text'
                                value={this.state.form.username}
                                required
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <div className='form-group'>
                            <label htmlFor='id_login'>First Name:</label>
                            <input
                                id='id_firstname'
                                className='form-control'
                                name='first_name'
                                placeholder='First Name'
                                type='text'
                                value={this.state.form.first_name}
                                required
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <div className='form-group'>
                            <label htmlFor='id_login'>Last Name:</label>
                            <input
                                id='id_lastname'
                                className='form-control'
                                name='last_name'
                                placeholder='Last Name'
                                type='text'
                                value={this.state.form.last_name}
                                required
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <div className='form-group'>
                            <label htmlFor='id_login'>Affiliation:</label>
                            <input
                                id='id_affiliation'
                                className='form-control'
                                name='affiliation'
                                placeholder='Affiliation'
                                type='text'
                                value={this.state.form.affiliation}
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
                                value={this.state.form.password}
                                required
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <div className='form-group'>
                            <label htmlFor='id_password_confirm'>Confirm Password:</label>
                            <input
                                id='id_password_confirm'
                                className='form-control'
                                name='confirm_password'
                                placeholder='Password again'
                                type='password'
                                value={this.state.form.confirm_password}
                                required
                                onChange={this.handleInputChange}
                            />
                        </div>
                        <input type='hidden' name='success_uri' value='/account/signup/success' />
                        <input
                            type='hidden'
                            name='error_uri'
                            value={this.props.location.pathname}
                        />
                        <button type='submit'>Sign Up &raquo;</button>
                    </form>
                </ContentWrapper>
            </React.Fragment>
        );
    }
}
