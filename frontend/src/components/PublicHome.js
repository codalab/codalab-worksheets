import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';

/**
 * This [pure / stateful] component ___.
 */
class PublicHome extends React.Component<
    {
        /** React components within opening & closing tags. */
        children: React.Node,
    },
    {
        // Optional: type declaration of this.state.
    },
> {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>Home</div>;
    }
}

export default PublicHome;
