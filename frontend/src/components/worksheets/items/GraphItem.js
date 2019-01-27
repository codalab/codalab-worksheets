import * as React from 'react';
import Immutable from 'seamless-immutable';

class GraphItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>GraphItem</div>;
    }
}

export default GraphItem;
