import * as React from 'react';
import Immutable from 'seamless-immutable';

class RunBundleBuilder extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>RunBundleBuilder</div>;
    }
}

export default RunBundleBuilder;
