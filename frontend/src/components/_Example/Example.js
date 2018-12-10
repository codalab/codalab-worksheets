import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { withStyles } from '@material-ui/core/styles';
import { createSelector } from 'reselect';

/**
 * This [pure / stateful] component ___.
 */
class Example extends React.Component<
    {
        /** CSS-in-JS styling object. */
        classes: {},

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

/** CSS-in-JS styling object. */
const styles = (theme) => ({
    // css-key: value,
});

export default withStyles(styles)(Example);
