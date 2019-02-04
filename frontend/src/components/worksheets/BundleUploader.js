import * as React from 'react';
import Immutable from 'seamless-immutable';

class BundleUploader extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>BundleUploader</div>;
    }
}

export default BundleUploader;
