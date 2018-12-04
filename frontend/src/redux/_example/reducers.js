import { handle } from 'redux-pack';
import Immutable from 'seamless-immutable';
import { kExampleActions } from './actions';

/** Root reducer's state slice shape. */
export type ExampleState = {
    // Description of data member.
    data: number,
};

/** Root reducer's initial state slice. */
const initialState: ExampleState = Immutable({});

/**
 * Root reducer for state related to ____.
 */
export function exampleReducer(state: ExampleState = initialState, action = {}) {
    const { type } = action;
    switch (type) {
        case kExampleActions.SYNC_ACTION:
            return syncReducer(state, action);
        case kExampleActions.ASYNC_ACTION:
            return asyncReducer(state, action);
    }
    return state; // No effect by default
}

/** TODO: Reducer for synchronous action. */
function syncReducer(state, action) {
    return state;
}

/** TODO: Reducer for asynchronous action. */
function asyncReducer(state, action) {
    return handle(state, action, {
        start: (state) => ({ ...state }), // of form `state ==> (state)`
        finish: (state) => ({ ...state }),
        failure: (state) => ({ ...state }),
        success: (state) => ({ ...state }),
    });
}
