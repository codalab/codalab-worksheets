import * as React from 'react';

const SubHeader = (props: { title: string }) => (
    <div class='page-header'>
        <div class='container'>
            <h1>{props.title}</h1>
        </div>
    </div>
);

export default SubHeader;
