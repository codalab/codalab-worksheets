import * as React from 'react';

const SubHeader = (props: { title: string }) => (
    <div className='page-header'>
        <div className='container'>
            <h1>{props.title}</h1>
        </div>
    </div>
);

export default SubHeader;
