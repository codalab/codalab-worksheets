import * as React from 'react';
import Immutable from 'seamless-immutable';

class WorksheetEditableField extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>WorksheetEditableField</div>;
    }
}

export default WorksheetEditableField;
