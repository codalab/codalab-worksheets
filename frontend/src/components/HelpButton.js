import * as React from 'react';
import Immutable from 'seamless-immutable';

class HelpButton extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>HelpButton</div>;
    }
}

export default HelpButton;
