import * as React from 'react';

const ContentWrapper = ({ children }) => (
    <div className='container' style={{ 'padding-bottom': 40 }}>
        {children}
    </div>
);

export default ContentWrapper;
