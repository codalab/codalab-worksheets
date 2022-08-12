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
        if (!state || state === 'failed' || state === 'killed') {
            return classes.failed;
        }
        if (state === 'worker_offline') {
            return classes.offline;
        }
        if (state === 'ready') {
            return classes.ready;
        }
        return classes.pending;
    }

    render() {
        const { classes, state } = this.props;
        const title = state || 'not found';
        return (
            <Tooltip classes={{ tooltip: classes.tooltip }} title={title}>
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
        backgroundColor: theme.color.green.base,
    },
    pending: {
        backgroundColor: theme.color.yellow.base,
    },
    failed: {
        backgroundColor: theme.color.red.base,
    },
    offline: {
        backgroundColor: theme.color.grey.darker,
    },
    tooltip: {
        fontSize: 14,
    },
});

export default withStyles(styles)(BundleStateIndicator);
