import * as React from 'react';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import { NavLink } from 'react-router-dom';

const VerifySuccess = (props) => {
    return (
        <React.Fragment>
            <SubHeader title='Signup Success' />
            <ContentWrapper>
                <div className='row'>
                    <div className='col-md-6'>
                        <h4>Your account has been verified!</h4>
                        {props.auth.isAuthenticated && (
                            <p className='user-authenticated'>
                                Check out your{' '}
                                <NavLink
                                    to='/worksheets?name=dashboard'
                                    tabIndex={2}
                                >
                                    dashboard
                                </NavLink>{' '}
                                to get started.
                            </p>
                        )}
                        {!props.auth.isAuthenticated && (
                            <p className='user-not-authenticated'>
                                <a
                                    href={
                                        '/account/login?next=' +
                                        encodeURIComponent('/rest/worksheets/?name=dashboard')
                                    }
                                    target='_self'
                                >
                                    Sign In
                                </a>{' '}
                                to get started.
                            </p>
                        )}
                    </div>
                </div>
            </ContentWrapper>
        </React.Fragment>
    );
};

export default VerifySuccess;
