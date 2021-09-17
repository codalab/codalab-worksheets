import * as React from 'react';
import Immutable from 'seamless-immutable';
import { NavLink } from 'react-router-dom';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import queryString from 'query-string';

export const ChangeEmailSuccess = (props) => {
    return (
        <React.Fragment>
            <SubHeader title='Change Email Address' />
            <ContentWrapper>
                <p>
                    We have sent you an email to verify your new email address. Please contact us if
                    you do not receive it within a few minutes.
                </p>
            </ContentWrapper>
        </React.Fragment>
    );
};

export class ChangeEmail extends React.Component {
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

        this.setState({
            form: { [name]: value },
        });
    };

    render() {
        const { error } = queryString.parse(this.props.location.search);

        return (
            <React.Fragment>
                <SubHeader title='Change Email Address' />
                <ContentWrapper>
                    {!this.props.auth.isAuthenticated && (
                        <p>
                            Please <NavLink to='/account/login'>sign in</NavLink> first before
                            updating your email address.
                        </p>
                    )}
                    {this.props.auth.isAuthenticated && (
                        <React.Fragment>
                            <p className='user-authenticated'>
                                Use this form to update the email address that you would like
                                CodaLab to use for notifications and identification. Note that this
                                will temporarily deactivate your account (i.e. you will be unable to
                                work on your worksheets) until you verify your account again with a
                                link sent to your new email address.
                            </p>
                            <div className='user-authenticated row'>
                                <div className='col-md-6'>
                                    {error && <div className='alert alert-error'>{error}</div>}
                                    <form method='post' action='/rest/account/changeemail'>
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
                                            value='Request Email Change'
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
