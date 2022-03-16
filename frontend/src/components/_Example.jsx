// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';

/** This class is an template for how new React components should be structured. */
class Example extends React.Component<
    {
        /** CSS-in-JS styling object. */
        classes: {},

        /** React components within opening & closing tags. */
        children: React.Node,
    },
    {
        // Optional
    },
> {
    /** Prop default values. */
    static defaultProps = {
        // key: value,
    };

    /**
     * Constructor.
     * @param props
     */
    constructor(props) {
        super(props);
        this.state = {};
    }

    /**
     * Renderer.
     */
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

// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({
    // css-key: value,
});

export default withStyles(styles)(Example);
