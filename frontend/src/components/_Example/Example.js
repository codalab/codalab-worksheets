import * as React from 'react';
import classNames from 'classnames';
import Immutable from 'seamless-immutable';
import { withStyles } from '@material-ui/core/styles';
import { connect } from 'react-redux';
import { bindActionCreators } from 'redux';
import { createSelector } from 'reselect';

/**
 * This [pure dumb / stateful dumb / smart] component ___.
 */
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

// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({
    // css-key: value,
});

export default withStyles(styles)(Example);

// TODO: For dumb components, just use the code above. Delete the code below and `connect`, `bindActionCreators`
// `createSelector` imports. For smart components, use the code below.

// To inject application state into component
// ------------------------------------------

/** Connects application state objects to component props. */
function mapStateToProps(state, props) {
    // Second argument `props` is manually set prop
    return (state, props) => {
        // propName1: state.subslice,
        // propName2: doSomethingSelector(state)
    };
}

/** Connects bound action creator functions to component props. */
function mapDispatchToProps(dispatch) {
    return bindActionCreators(
        {
            // propName: doSomethingAction,
        },
        dispatch,
    );
}

// export default connect(mapStateToProps, mapDispatchToProps)(
//     withStyles(styles)(Example)
// );
