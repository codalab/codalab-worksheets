import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import ArrowDownwardIcon from '@material-ui/icons/ArrowDownward';
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
        if (state === 'preparing') {
            return bundle.time_preparing?.value;
        }
        if (state === 'running') {
            return bundle.time_running?.value || bundle.time?.value;
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
            <>
                <BundleFieldRow
                    label='State'
                    description="The bundle lifecycle diagram to the right indicates this bundle's current state."
                    value={
                        <div className={classes.stateGraphic}>
                            {states.map((state) => {
                                const isCurrent = currentState === state;
                                const isLast = FINAL_BUNDLE_STATES.includes(state);
                                const time = this.getTime(state);
                                return (
                                    <>
                                        <div className={classes.stateBoxContainer}>
                                            <BundleStateBox state={state} isActive={isCurrent} />
                                            {time && (
                                                <span className={classes.timeContainer}>
                                                    {time}
                                                </span>
                                            )}
                                        </div>
                                        {!isLast && (
                                            <div className={classes.arrowContainer}>
                                                <ArrowDownwardIcon fontSize='small' />
                                            </div>
                                        )}
                                    </>
                                );
                            })}
                        </div>
                    }
                />
                <BundleFieldRow
                    label='Status'
                    description="Additional information about the bundle's current state."
                    value={<div className={classes.stateDetails}>{stateDetails}</div>}
                />
            </>
        );
    }
}

const styles = (theme) => ({
    stateGraphic: {
        textAlign: 'center',
        marginBottom: 8,
    },
    stateDetails: {
        width: 184,
        paddingTop: 5,
        paddingBottom: 5,
        paddingLeft: 8,
        paddingRight: 8,
        border: `1px solid ${theme.color.grey.base}`,
        backgroundColor: theme.color.grey.lightest,
        borderRadius: 5,
    },
    timeContainer: {
        position: 'absolute',
        marginTop: 5,
        paddingLeft: 5,
        fontSize: '11px',
        color: theme.color.grey.dark,
    },
    arrowContainer: {
        display: 'flex',
        justifyContent: 'center',
    },
});

export default withStyles(styles)(BundleStateTable);
