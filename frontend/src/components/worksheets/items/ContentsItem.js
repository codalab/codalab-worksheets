import * as React from 'react';
import Immutable from 'seamless-immutable';

class ContentsItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>ContentsItem</div>;
    }
}

export default ContentsItem;
