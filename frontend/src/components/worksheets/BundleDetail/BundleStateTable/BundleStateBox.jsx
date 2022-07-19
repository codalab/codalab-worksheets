import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import { Typography } from '@material-ui/core';

/**
 * This component is a color-coded box that displays a given state.
 * It is optimized to be used in the BundleStateTable.
 *
 * If the state is active, a flashing animation is applied to it.
 */
class BundleStateBox extends React.Component {
    constructor(props) {
        super(props);
    }

    getActiveClass() {
        const { classes, state, isActive } = this.props;
        if (!isActive) {
            return '';
        }
        if (state == 'ready') {
            return classes.readyState;
        }
        if (state == 'failed' || state == 'killed') {
            return classes.failedState;
        }
        if (state == 'worker_offline') {
            return classes.offlineState;
        }
        return classes.activeState;
    }

    render() {
        const { classes, state } = this.props;
        const activeClass = this.getActiveClass(state);

        return (
            <div className={`${classes.baseState} ${activeClass}`}>
                <Typography inline color='inherit'>
                    {state}
                </Typography>
            </div>
        );
    }
}

const styles = (theme) => ({
    baseState: {
        display: 'inline-block',
        borderRadius: '5px',
        textAlign: 'center',
        padding: '0px 5px',
        verticalAlign: 'middle',
    },
    readyState: {
        color: 'white',
        backgroundColor: theme.color.green.base,
    },
    failedState: {
        color: 'white',
        backgroundColor: theme.color.red.base,
    },
    activeState: {
        color: 'white',
        backgroundColor: theme.color.yellow.base,
        animation: 'flashing 1100ms linear infinite',
    },
    offlineState: {
        color: 'white',
        backgroundColor: theme.color.grey.darker,
    },
    '@keyframes flashing': {
        '50%': {
            opacity: 0,
        },
    },
});

export default withStyles(styles)(BundleStateBox);
