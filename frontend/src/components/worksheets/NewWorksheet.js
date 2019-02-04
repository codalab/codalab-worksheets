import * as React from 'react';
import Immutable from 'seamless-immutable';

class NewWorksheet extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>NewWorksheet</div>;
    }
}

export default NewWorksheet;
