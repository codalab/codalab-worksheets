import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { Redirect } from 'react-router-dom';
import $ from 'jquery';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import { Formik, Form, Field, ErrorMessage } from 'formik';

class SignUp extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            redirect: false,
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

    onSubmit = (values, { setSubmitting }) => {
        console.log(values);
        $.ajax({
            type: 'POST',
            url: '/rest/account/signup',
            data: values,
            success: (response, status, xhr) => {
                console.log(response);
                console.log(status);
                console.log(xhr);
                this.setState({ redirect: true });
            },
        });
    };

    render() {
        if (this.state.redirectToReferrer) return <Redirect to={'/'} />;

        return (
            <React.Fragment>
                <SubHeader title='Sign Up' />
                <ContentWrapper>
                    <p>
                        Already have an account? Then please <a href='/account/login'>sign in</a>.
                    </p>
                    <Formik
                        initialValues={{
                            email: '',
                            username: '',
                            first_name: '',
                            last_name: '',
                            affiliation: '',
                            password: '',
                            confirm_password: '',
                            success_uri: '/account/signup/success',
                            error_uri: '/account/signup',
                        }}
                        validate={(values) => {
                            // TODO: this is unused, integrate this?
                            let errors = {};
                            if (!values.email) {
                                errors.email = 'Required';
                            } else if (
                                !/^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(values.email)
                            ) {
                                errors.email = 'Invalid email address';
                            }
                            return errors;
                        }}
                        onSubmit={this.onSubmit}
                    >
                        {({
                            values,
                            errors,
                            handleChange,
                            handleBlur,
                            handleSubmit,
                            isSubmitting,
                        }) => (
                            <React.Fragment>
                                {this.state.redirect && (
                                    <Redirect
                                        to={
                                            '/account/signup/success?email=' +
                                            encodeURIComponent(values.email)
                                        }
                                    />
                                )}
                                <form className='signup' id='signup_form' onSubmit={handleSubmit}>
                                    {this.state.error && <div className='alert alert-error' />}
                                    <div className='form-group'>
                                        <label htmlFor='id_email'>Email:</label>
                                        <input
                                            id='id_email'
                                            className='form-control'
                                            name='email'
                                            placeholder='Email'
                                            type='email'
                                            value={values.email}
                                            required
                                            autoFocus
                                            onChange={handleChange}
                                            onBlur={handleBlur}
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
                                            value={values.username}
                                            required
                                            onChange={handleChange}
                                            onBlur={handleBlur}
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
                                            value={values.first_name}
                                            required
                                            onChange={handleChange}
                                            onBlur={handleBlur}
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
                                            value={values.last_name}
                                            required
                                            onChange={handleChange}
                                            onBlur={handleBlur}
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
                                            value={values.affiliation}
                                            onChange={handleChange}
                                            onBlur={handleBlur}
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
                                            value={values.password}
                                            required
                                            onChange={handleChange}
                                            onBlur={handleBlur}
                                        />
                                    </div>
                                    <div className='form-group'>
                                        <label htmlFor='id_password_confirm'>
                                            Confirm Password:
                                        </label>
                                        <input
                                            id='id_password_confirm'
                                            className='form-control'
                                            name='confirm_password'
                                            placeholder='Password again'
                                            type='password'
                                            value={values.confirm_password}
                                            required
                                            onChange={handleChange}
                                            onBlur={handleBlur}
                                        />
                                    </div>
                                    <input
                                        type='hidden'
                                        name='success_uri'
                                        value={values.success_uri}
                                        onChange={handleChange}
                                        onBlur={handleBlur}
                                    />
                                    <input
                                        type='hidden'
                                        name='error_uri'
                                        value={values.error_uri}
                                        onChange={handleChange}
                                        onBlur={handleBlur}
                                    />
                                    <button type='submit' disabled={isSubmitting}>
                                        Sign Up &raquo;
                                    </button>
                                </form>
                            </React.Fragment>
                        )}
                    </Formik>
                </ContentWrapper>
            </React.Fragment>
        );
    }
}

export default SignUp;
