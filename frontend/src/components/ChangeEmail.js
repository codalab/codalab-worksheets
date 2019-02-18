import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { Redirect } from 'react-router-dom';
import $ from 'jquery';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import { Formik } from 'formik';

class ChangeEmail extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            redirectToSent: false,
            error: {},
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
        $.ajax({
            type: 'POST',
            url: '/rest/account/changeemail',
            data: values,
            success: (response, status, xhr) => {
                this.setState(Immutable({ redirectToSent: true }));
            },
        });
    };

    render() {
        if (this.state.redirectToSent) return <Redirect to={'/account/changeemail/sent'} />;

        return (
            <React.Fragment>
                <SubHeader title='Change Email Address' />
                <ContentWrapper>
                    {!this.props.auth.isAuthenticated && (
                        <p>
                            Please <a href='/account/login'>sign in</a> first before updating your
                            email address.
                        </p>
                    )}
                    {this.props.auth.isAuthenticated && (
                        <React.Fragment>
                            <p class='user-authenticated'>
                                Use this form to update the email address that you would like
                                CodaLab to use for notifications and identification. Note that this
                                will temporarily deactivate your account (i.e. you will be unable to
                                work on your worksheets) until you verify your account again with a
                                link sent to your new email address.
                            </p>
                            <div className='user-authenticated row'>
                                <div className='col-md-6'>
                                    <Formik
                                        initialValues={{
                                            email: '',
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
                                                <form onSubmit={handleSubmit}>
                                                    <div className='form-group'>
                                                        <div className='form-group'>
                                                            <label for='id_email'>Email:</label>
                                                            <input
                                                                id='id_email'
                                                                className='form-control'
                                                                name='email'
                                                                placeholder='Email address'
                                                                type='email'
                                                                autofocus
                                                                required
                                                                value={values.email}
                                                                onChange={handleChange}
                                                                onBlur={handleBlur}
                                                            />
                                                        </div>
                                                    </div>
                                                    <input
                                                        className='btn btn-primary margin-top'
                                                        type='submit'
                                                        value='Request Email Change'
                                                        disabled={isSubmitting}
                                                    />
                                                </form>
                                            </React.Fragment>
                                        )}
                                    </Formik>
                                </div>
                            </div>
                        </React.Fragment>
                    )}
                </ContentWrapper>
            </React.Fragment>
        );
    }
}

export default ChangeEmail;
