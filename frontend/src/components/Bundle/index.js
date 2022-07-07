import React from 'react';
import MakeBundle from './MakeBundle';
import RunBundle from './RunBundle';
import UploadedBundle from './UploadedBundle';

class Bundle extends React.Component {
    render() {
        // TODO: fetch bundle info here and pass into appropriate bundle
        // TODO: handle 'private' bundle type -- private_bundle.py
        // TODO: handle bundle error messaging

        const bundleType = 'run';

        if (bundleType == 'run') {
            return <RunBundle />;
        }
        if (bundleType == 'dataset') {
            return <UploadedBundle />;
        }
        if (bundleType == 'make') {
            return <MakeBundle />;
        }
        return <div>404 Bundle Not Found</div>;
    }
}

export default Bundle;
