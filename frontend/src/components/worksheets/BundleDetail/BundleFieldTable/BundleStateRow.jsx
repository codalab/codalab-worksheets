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



        this.stateScrollBoxRef = React.createRef();
        this.activeStateBoxRef = React.createRef();




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




    updateScrollBox() {
        const scrollBoxNode = this.stateScrollBoxRef.current;



        // TODO: if the user scrolled the element, disable auto-scroll


        if (!scrollBoxNode) {
            return;
        }


        // todo: fade away left and right



        // skip the first preceeding arrow and the first preceeding state
        // calculate the width of the rest of the states / arrow before the first preceeding
        // that width will be scrollLeft


        const activeStateNode = document.querySelector('#active-state-box');


        let skip = 2;
        let scrollLeft = 0;
        let isFirst = false;
        // let prev;

        let prev = activeStateNode?.previousElementSibling?.previousElementSibling?.previousElementSibling;



        while (prev) {

            console.log('prev', );

            scrollLeft += prev.offsetWidth;

            prev = prev.previousElementSibling
        }



        // console.log('no more prev');

        scrollBoxNode.scrollTo({
            left: scrollLeft,
            behavior: 'smooth',
        });


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


        // TODO:
        // auto scroll
        // state details
        // pockets (potentially)
        // time

        this.updateScrollBox();


        return (
            <tr>
                <td colspan={2}>
                    <div className={classes.stateInfoContainer}>

                        <div ref={this.stateScrollBoxRef} className={classes.stateScrollbox}>



                            <div className={`${classes.scrollBoxMask} ${classes.leftMask}`} />
                            <div className={`${classes.scrollBoxMask} ${classes.rightMask}`} />



                            <div className={classes.stateDiagram}>
                                {states.map((state) => {
                                    const isLast = FINAL_BUNDLE_STATES.includes(state);
                                    const isCurrent = currentState === state;
                                    const id = isCurrent ? 'active-state-box' : '';
                                    const margin = isCurrent ? '0 5px' : '0';
                                    const time = this.getTime(state);
                                    return (
                                        <>
                                            <div id={id} className={classes.stateBoxContainer}>
                                                <BundleStateBox state={state} isActive={isCurrent} style={{ margin }} />
                                                {time && (
                                                    <div className={classes.timeContainer}>
                                                        {time}
                                                    </div>
                                                )}
                                            </div>


                                            {!isLast && (
                                                <div className={classes.arrowContainer}>
                                                    →
                                                </div>
                                            )}
                                        </>
                                    );
                                })}
                            </div>
                        </div>




                        <div className={classes.stateDetails}>
                            {/* {stateDetails} */}
                            Bundle’s dependencies are all ready. Waiting for the bundle to be assigned to a worker to be run.
                        </div>



                    </div>
                </td>
            </tr>
        );





    }
}




const styles = (theme) => ({


    stateInfoContainer: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        marginBottom: 24,
    },

    stateScrollbox: {

        // position: 'relative',

        width: 225,
        height: 42,
        marginBottom: 16,
        overflowX: 'scroll',
        '-ms-overflow-style': 'none',
        'scrollbar-width': 'none',
        '&::-webkit-scrollbar': {
            display: 'none',
        }
    },


    scrollBoxMask: {
        width: 70,
        height: 21,
        position: 'absolute',
        zIndex: 1,
    },

    leftMask: {
        left: 45,
        backgroundImage: `linear-gradient(to left, transparent, ${theme.color.grey.lighter}, ${theme.color.grey.lighter})`,
    },

    rightMask: {
        right: 45,
        backgroundImage: `linear-gradient(to right, transparent, ${theme.color.grey.lighter}, ${theme.color.grey.lighter})`,
    },



    stateDiagram: {
        position: 'relative',
        display: 'flex',
    },

    stateBoxContainer: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
    },
    timeContainer: {
        // position: 'absolute',
        marginTop: 5,
        // paddingLeft: 5,
        fontSize: 11,
        color: theme.color.grey.darker,
    },
    arrowContainer: {
        paddingTop: 2,
        // color: theme.color.grey.darkest,
    },

    stateDetails: {
        width: '100%',
        height: 32,
        maxHeight: 32,
        // overflowY: 'scroll',
        textAlign: 'center',
        // fontSize: 11,
        color: theme.color.grey.darker,
    },
});

export default withStyles(styles)(BundleStateTable);
