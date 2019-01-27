import * as React from 'react';
import Immutable from 'seamless-immutable';

class WorksheetSidePanel extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>WorksheetSidePanel</div>;
    }
}

export default WorksheetSidePanel;
