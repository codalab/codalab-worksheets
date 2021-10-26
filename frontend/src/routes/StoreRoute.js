import * as React from 'react';
import Store from '../components/Store/Store';

/**
 * This route page displays a bundle's metadata and contents.
 */
class StoreRoute extends React.Component {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        const { uuid } = this.props.match.params;
        this.state = {
            storeInfo: null,
            uuid,
        };
    }

    /** Renderer. */
    render() {
        const { uuid } = this.props.match.params;
        return <Store uuid={uuid} isStandalonePage={true} />;
    }
}

export default StoreRoute;
