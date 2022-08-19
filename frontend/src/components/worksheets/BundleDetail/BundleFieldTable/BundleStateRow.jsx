import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import { FINAL_BUNDLE_STATES } from '../../../../constants';
import BundleStateBox from '../BundleStateBox';
import BundleFieldRow from './BundleFieldRow';

/**
 * This component renders a dynamic BundleFieldRow that changes as a bundle's
 * state changes.
 *
 * If the bundle is in a final state, a simple state box will be shown.
 * If the bundle is not in a final state, a state diagram will be shown.
 */
class BundleStateTable extends React.Component {
    constructor(props) {
        super(props);
        const states = this.getStates();
        this.state = {
            states,
        };
    }

    getStates() {
        const bundleType = this.props.bundle.bundle_type.value;
        if (bundleType === 'run') {
            return ['created', 'staged', 'starting', 'preparing', 'running', 'finalizing', 'ready'];
        }
        if (bundleType === 'dataset') {
            return ['created', 'uploading', 'ready'];
        }
        if (bundleType === 'make') {
            return ['created', 'making', 'ready'];
        }
        return [];
    }

    getTime(state) {
        const bundle = this.props.bundle;
        let time;
        if (state === 'preparing') {
            time = bundle.time_preparing?.value;
        } else if (state === 'running') {
            time = bundle.time_running?.value || bundle.time?.value;
        }
        if (time && time !== '0s') {
            return time;
        }
    }

    render() {
        const { bundle, classes } = this.props;
        const { states } = this.state;
        const stateDetails = bundle.state_details?.value;
        const currentState = bundle.state.value;
        const inFinalState = FINAL_BUNDLE_STATES.includes(currentState);

        if (inFinalState) {
            return (
                <BundleFieldRow
                    label='State'
                    description="This bundle's final state."
                    value={<BundleStateBox state={currentState} title={stateDetails} isActive />}
                />
            );
        }

        return (
            <BundleFieldRow
                label='State'
                description="The bundle lifecycle diagram to the right indicates this bundle's current state."
                value={
                    <div className={classes.stateInfoContainer}>
                        <div className={classes.stateGraphic}>
                            {states.map((state) => {
                                const isLast = FINAL_BUNDLE_STATES.includes(state);
                                const isCurrent = currentState === state;
                                const margin = isCurrent ? '5px 0' : '0';
                                const timeMargin = isCurrent ? '9px 0 0' : '4px 0 0';
                                const time = this.getTime(state);
                                return (
                                    <>
                                        <div className={classes.stateBoxContainer}>
                                            <BundleStateBox
                                                state={state}
                                                isActive={isCurrent}
                                                style={{ margin }}
                                            />
                                            {time && (
                                                <span
                                                    className={classes.timeContainer}
                                                    style={{ margin: timeMargin }}
                                                >
                                                    {time}
                                                </span>
                                            )}
                                        </div>
                                        {!isLast && <div className={classes.arrowContainer}>↓</div>}
                                    </>
                                );
                            })}
                        </div>
                        <div className={classes.stateDetails}>{stateDetails}</div>
                    </div>
                }
            />
        );
    }
}

const styles = (theme) => ({
    stateInfoContainer: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'flex-start',
    },
    stateGraphic: {
        textAlign: 'center',
        marginBottom: 8,
    },
    stateDetails: {
        minHeight: 50,
        fontSize: 11,
        color: theme.color.grey.darker,
    },
    timeContainer: {
        position: 'absolute',
        paddingLeft: 5,
        fontSize: 11,
        color: theme.color.grey.darker,
    },
    arrowContainer: {
        display: 'flex',
        justifyContent: 'center',
        lineHeight: '14px',
    },
});

export default withStyles(styles)(BundleStateTable);
