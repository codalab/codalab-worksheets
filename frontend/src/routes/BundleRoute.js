import * as React from 'react';
// import Bundle from '../components/Bundle/Bundle';

import Bundle from '../components/Bundle';

/**
 * This route page displays a bundle's metadata and contents.
 */
class BundleRoute extends React.Component<> {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        const { uuid } = this.props.match.params;
        this.state = {
            bundleInfo: null,
            uuid,
        };
    }

    /** Renderer. */
    render() {
        const { uuid } = this.props.match.params;
        return <Bundle uuid={uuid} isStandalonePage={true} />;
    }
}

export default BundleRoute;
