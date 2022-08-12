import React from 'react';
import Tooltip from '@material-ui/core/Tooltip';
import { withStyles } from '@material-ui/core/styles';
import { Typography } from '@material-ui/core';

/**
 * This component is a color-coded box that displays a given state.
 * If the state is active, a pulsing animation is applied to it.
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
        if (state === 'ready') {
            return classes.readyState;
        }
        if (state === 'failed' || state === 'killed') {
            return classes.failedState;
        }
        if (state === 'worker_offline') {
            return classes.offlineState;
        }
        return classes.activeState;
    }

    render() {
        const { classes, title, state } = this.props;
        const activeClass = this.getActiveClass(state);

        return (
            <Tooltip
                classes={{ tooltip: classes.tooltip }}
                disableHoverListener={!title}
                title={title}
            >
                <div className={`${classes.baseState} ${activeClass}`}>
                    <Typography inline color='inherit'>
                        {state}
                    </Typography>
                </div>
            </Tooltip>
        );
    }
}

const styles = (theme) => ({
    tooltip: {
        fontSize: 14,
    },
    baseState: {
        display: 'inline-block',
        borderRadius: '5px',
        textAlign: 'center',
        padding: '0px 5px',
        verticalAlign: 'middle',
        cursor: 'default',
    },
    readyState: {
        color: 'white',
        backgroundColor: theme.color.green.base,
    },
    failedState: {
        color: 'white',
        backgroundColor: theme.color.red.base,
    },
    offlineState: {
        color: 'white',
        backgroundColor: theme.color.grey.darker,
    },
    activeState: {
        color: 'white',
        backgroundColor: theme.color.yellow.base,
        animation: 'pulsing 3000ms ease-in-out infinite',
    },
    '@keyframes pulsing': {
        '0%': {
            backgroundColor: theme.color.yellow.base,
        },
        '50%': {
            backgroundColor: '#D79B0F',
        },
        '100%': {
            backgroundColor: theme.color.yellow.base,
        },
    },
});

export default withStyles(styles)(BundleStateBox);
