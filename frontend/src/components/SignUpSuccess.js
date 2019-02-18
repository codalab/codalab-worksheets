import * as React from 'react';
import classNames from 'classnames';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';
import { withRouter } from 'react-router-dom';
import queryString from 'query-string';

const SignUpSuccess = (props) => {
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

export default withRouter(SignUpSuccess);
