import * as React from 'react';
import Immutable from 'seamless-immutable';

class WorksheetChatBox extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>WorksheetChatBox</div>;
    }
}

export default WorksheetChatBox;
