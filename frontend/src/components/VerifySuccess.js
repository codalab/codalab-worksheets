import * as React from 'react';
import classNames from 'classnames';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';

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
                                <a
                                    href='/rest/worksheets/?name=dashboard'
                                    tabIndex={2}
                                    target='_self'
                                >
                                    dashboard
                                </a>{' '}
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
