import * as React from 'react';
import Immutable from 'seamless-immutable';

class TableItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>TableItem</div>;
    }
}

export default TableItem;
