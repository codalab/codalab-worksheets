import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import ArrowDownwardIcon from '@material-ui/icons/ArrowDownward';
import { FINAL_BUNDLE_STATES } from '../../../../constants';
import { BundleFieldTable, BundleFieldRow } from '../BundleFieldTable/';
import BundleStateBox from './BundleStateBox';

/**
 * This component renders a dynamic BundleFieldTable that changes as a bundle's
 * state changes.
 *
 * If the bundle is in a final state, a simple state box will be shown.
 * If the bundle is not in a final state, a state diagram will be shown.
 */
class BundleStateTable extends React.Component {
    constructor(props) {
        super(props);
    }

    roundTime(time) {
        return Math.abs(Math.round(time));
    }

    getTimePassed() {
        const created = new Date(this.props.bundle?.created?.value);
        const now = new Date();

        const secondsPassed = this.roundTime((now.getTime() - created.getTime()) / 1000);
        if (secondsPassed < 60) {
            const secondText = secondsPassed <= 1 ? 'second' : 'seconds';
            return `${secondsPassed} ${secondText}`;
        }

        const minutesPassed = this.roundTime(secondsPassed / 60);
        if (minutesPassed < 60) {
            const minuteText = minutesPassed <= 1 ? 'minute' : 'minutes';
            return `${minutesPassed} ${minuteText}`;
        }

        const hoursPassed = this.roundTime(minutesPassed / 60);
        const hoursText = hoursPassed <= 1 ? 'hour' : 'hours';
        return `${hoursPassed} ${hoursText}`;
    }

    render() {
        const { bundle, classes, states } = this.props;
        const stateDetails = bundle.state_details.value; // rendered in State tooltip
        const currentState = bundle.state.value;
        const inFinalState = FINAL_BUNDLE_STATES.includes(currentState);
        const finalState = inFinalState ? currentState : 'ready';

        return (
            <BundleFieldTable>
                <BundleFieldRow
                    label='State'
                    field={bundle.state}
                    description={stateDetails}
                    value={
                        <div className={classes.stateGraphic}>
                            {states.map((state) => {
                                const showBundleLifecycle =
                                    !inFinalState && !FINAL_BUNDLE_STATES.includes(state);
                                if (showBundleLifecycle) {
                                    return (
                                        <div>
                                            <BundleStateBox
                                                state={state}
                                                isActive={currentState === state}
                                            />
                                            <div className={classes.arrowContainer}>
                                                <ArrowDownwardIcon fontSize='small' />
                                            </div>
                                        </div>
                                    );
                                }
                            })}
                            <BundleStateBox state={finalState} isActive={inFinalState} />
                        </div>
                    }
                />
                {!inFinalState && (
                    <BundleFieldRow
                        label='Time'
                        description='The amout of time that has passed between the creation of your bundle to now.'
                        value={this.getTimePassed()}
                    />
                )}
            </BundleFieldTable>
        );
    }
}

const styles = () => ({
    stateGraphic: {
        textAlign: 'center',
    },
    arrowContainer: {
        display: 'flex',
        justifyContent: 'center',
    },
});

export default withStyles(styles)(BundleStateTable);

// import { renderDuration } from '../../../util/worksheet_utils';
// import BundleStateTooltip from '../../BundleStateTooltip';

// const bundleRunTime = bundleInfo.metadata.time
//     ? renderDuration(bundleInfo.metadata.time)
//     : '-- --';

{
    /* {isRunBundle ? (
                    <div>
                        <ConfigLabel label='Run time: ' inline={true} />
                        <div className={classes.dataText}>{bundleRunTime}</div>
                    </div>
                ) : null} */
}
