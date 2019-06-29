import * as React from 'react';

const ContentWrapper = ({ children }) => (
    <div className='container' style={{ paddingBottom: 40 }}>
        {children}
    </div>
);

export default ContentWrapper;
