import * as React from 'react';
import Immutable from 'seamless-immutable';

class WorksheetActionBar extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>WorksheetActionBar</div>;
    }
}

export default WorksheetActionBar;
