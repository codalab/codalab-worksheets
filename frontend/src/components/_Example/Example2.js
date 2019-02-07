// @flow
import * as React from 'react';
import $ from 'jquery';
import classNames from 'classnames';

class Example extends React.Component<
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
        this.state = {};
    }

    /** Renderer. */
    render() {
        const { classes } = this.props;
        return (
            <div
                className={classNames({
                    [classes.container]: true,
                })}
            />
        );
    }
}

