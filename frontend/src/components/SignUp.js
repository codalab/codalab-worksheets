import * as React from 'react';
import { NavLink } from 'react-router-dom';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import queryString from 'query-string';
import ReCAPTCHA from 'react-google-recaptcha';

// Set global properties used by reCaptcha
// See: https://www.npmjs.com/package/react-google-recaptcha#global-properties-used-by-recaptcha
window.recaptchaOptions = {
    // Use recaptcha.net to avoid blocks on google.com
    useRecaptchaNet: true,
};

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
    state = {
        form: {},
        captchaPassed: false,
    };
    recaptchaRef = React.createRef();

    handleInputChange = (event) => {
        const target = event.target;
        const value = target.value;
        const name = target.name;

        this.setState({
            form: { ...this.state.form, [name]: value },
        });
    };

    componentDidMount() {
        const { error } = queryString.parse(this.props.location.search);
        if (!error) {
            return;
        }
        // Retrieve previous user input from the Http Request
        const { email, username, first_name, last_name, affiliation } = queryString.parse(
            this.props.location.search,
        );
        this.setState({
            form: {
                email: email,
                username: username,
                first_name: first_name,
                last_name: last_name,
                affiliation: affiliation,
            },
        });
    }

    onSubmit = async (e) => {
        e.preventDefault();
        try {
            const token = this.recaptchaRef.current.getValue();
            const response = await fetch('/rest/account/signup', {
                method: 'POST',
                body: new URLSearchParams({
                    ...this.state.form,
                    success_uri: '/account/signup/success',
                    error_uri: this.props.location.pathname,
                    token,
                }),
            });
            this.recaptchaRef.current.reset();
            if (response.redirected) {
                window.location.href = response.url;
            }
        } catch (error) {
            console.log(error);
        }
    };

    render() {
        const { error } = queryString.parse(this.props.location.search);
        return (
            <React.Fragment>
                <SubHeader title='Sign Up' />
                <ContentWrapper>
                    <p>
                        Already have an account? Then please{' '}
                        <NavLink to='/account/login'>sign in</NavLink>.
                    </p>
                    <form
                        className='signup'
                        id='signup_form'
                        method='post'
                        action='/rest/account/signup'
                        onSubmit={this.onSubmit}
                    >
                        {error && <div className='alert alert-error'>{error}</div>}
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
                        <ReCAPTCHA
                            ref={this.recaptchaRef}
                            sitekey={window.env.REACT_APP_CODALAB_RECAPTCHA_SITE_KEY}
                            onChange={() => {
                                this.setState({ captchaPassed: true });
                            }}
                            style={{ marginBottom: 10 }}
                        />
                        <button type='submit' disabled={!this.state.captchaPassed}>
                            Sign Up &raquo;
                        </button>
                    </form>
                </ContentWrapper>
            </React.Fragment>
        );
    }
}
