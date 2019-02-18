import * as React from 'react';
import classNames from 'classnames';
import SubHeader from './SubHeader';
import ContentWrapper from './ContentWrapper';

const VerifyError = (props) => {
    return (
        <React.Fragment>
            <SubHeader title='Signup Success' />
            <ContentWrapper>
                <div className='row'>
                    <div className='col-md-6'>
                        <div className='row'>
                            <div className='col-md-6'>
                                <p>Invalid or expired verification key. Please try again.</p>
                            </div>
                        </div>
                    </div>
                </div>
            </ContentWrapper>
        </React.Fragment>
    );
};

export default VerifyError;
