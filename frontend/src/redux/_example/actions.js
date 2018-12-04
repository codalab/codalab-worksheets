/** Action type definitions. */
export const kExampleActions = Object.freeze({
    // Must take form of `[Feature]Actions`
    SYNC_ACTION: 'path/to/example::SYNC_ACTION',
    ASYNC_ACTION: 'path/to/example::ASYNC_ACTION',
});

// TODO: Feature grouping
// ----------------------

/** Action creator to __. */
export function syncAction(value) {
    // Must take form of `[name]Action`
    return {
        type: kExampleActions.SYNC_ACTION,
        value, // ES6 shorthand for `value: value`
    };
}

/** Action creator to __. */
export function asyncAction(value) {
    return {
        type: kExampleActions.ASYNC_ACTION,
        promise: null, // e.g. `fetch(value)` or some other async action that returns Promise (see redux-pack docs)
        meta: {
            onSuccess: (result, getState) => {
                // for chaining Promises: can make another call here
            },
        },
    };
}

function anotherActionThunk(value) {
    return (dispatch) => {
        return Promise.resolve().then(dispatch(syncAction(value)));
    };
}

/** Action creator to ___ then ___ if ___. */
export function asyncActionThunk(value) {
    // Must take form of `[name]Thunk`
    return (dispatch, getState) => {
        let noDispatchCondition = getState().noDispatchCondition;
        if (noDispatchCondition) {
            return Promise.resolve();
        } else {
            return dispatch(anotherActionThunk(value))
                .then(() => {
                    dispatch(syncAction(value));
                    dispatch(syncAction(value));
                })
                .catch((error) => console.log(error));
        }
    };
}
