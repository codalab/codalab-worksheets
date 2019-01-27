import * as React from 'react';
import Immutable from 'seamless-immutable';

class ImageItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>ImageItem</div>;
    }
}

export default ImageItem;
