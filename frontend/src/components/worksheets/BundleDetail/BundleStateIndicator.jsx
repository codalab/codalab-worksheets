import React from 'react';
import { withStyles } from '@material-ui/core';
import Tooltip from '@material-ui/core/Tooltip';

/**
 * This component takes in a bundle state and renders a colored circle that
 * indicates the state.
 */
class BundleStateIndicator extends React.Component {
    constructor(props) {
        super(props);
    }

    getStateClass() {
        const { classes, state } = this.props;
        if (state === 'ready') {
            return classes.ready;
        }
        if (state === 'failed' || state === 'killed') {
            return classes.failed;
        }
        if (state === 'worker_offline') {
            return classes.offline;
        }
        return classes.pending;
    }

    render() {
        const { classes, state } = this.props;
        return (
            <Tooltip classes={{ tooltip: classes.tooltip }} title={state}>
                <div className={`${classes.base} ${this.getStateClass()}`} />
            </Tooltip>
        );
    }
}

const styles = (theme) => ({
    base: {
        display: 'inline-block',
        height: 7,
        width: 7,
        borderRadius: '100%',
    },
    ready: {
        background: `radial-gradient(circle at 2px 2px, ${theme.color.green.light}, ${theme.color.green.darkest})`,
    },
    pending: {
        background: `radial-gradient(circle at 2px 2px, ${theme.color.yellow.base}, ${theme.color.yellow.darkest})`,
    },
    failed: {
        background: `radial-gradient(circle at 2px 2px, ${theme.color.red.light}, ${theme.color.red.darkest})`,
    },
    offline: {
        background: `radial-gradient(circle at 2px 2px, ${theme.color.grey.dark}, ${theme.color.grey.darkest})`,
    },
    tooltip: {
        fontSize: 14,
    },
});

export default withStyles(styles)(BundleStateIndicator);
