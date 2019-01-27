import * as React from 'react';
import Immutable from 'seamless-immutable';

class RecordItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>RecordItem</div>;
    }
}

export default RecordItem;
