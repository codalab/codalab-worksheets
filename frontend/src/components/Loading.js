import React from 'react';
export default () => (
    <div
        style={{
            position: 'absolute',
            top: '50%',
            left: '50%',
        }}
    >
        <img src={`${process.env.PUBLIC_URL}/img/Preloader_Small.gif`} />
    </div>
);
