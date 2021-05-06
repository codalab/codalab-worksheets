Object.defineProperty(exports, "__esModule", { value: true });
var tslib_1 = require("tslib");
/* eslint-disable @typescript-eslint/no-explicit-any */
var minimal_1 = require("@sentry/minimal");
var ACTION_BREADCRUMB_CATEGORY = 'redux.action';
var ACTION_BREADCRUMB_TYPE = 'info';
var STATE_CONTEXT_KEY = 'redux.state';
var defaultOptions = {
    actionTransformer: function (action) { return action; },
    stateTransformer: function (state) { return state || null; },
};
/**
 * Creates an enhancer that would be passed to Redux's createStore to log actions and the latest state to Sentry.
 *
 * @param enhancerOptions Options to pass to the enhancer
 */
function createReduxEnhancer(enhancerOptions) {
    // Note: We return an any type as to not have type conflicts.
    var options = tslib_1.__assign(tslib_1.__assign({}, defaultOptions), enhancerOptions);
    return function (next) { return function (reducer, initialState) {
        var sentryReducer = function (state, action) {
            var newState = reducer(state, action);
            minimal_1.configureScope(function (scope) {
                /* Action breadcrumbs */
                var transformedAction = options.actionTransformer(action);
                if (typeof transformedAction !== 'undefined' && transformedAction !== null) {
                    scope.addBreadcrumb({
                        category: ACTION_BREADCRUMB_CATEGORY,
                        data: transformedAction,
                        type: ACTION_BREADCRUMB_TYPE,
                    });
                }
                /* Set latest state to scope */
                var transformedState = options.stateTransformer(newState);
                if (typeof transformedState !== 'undefined' && transformedState !== null) {
                    scope.setContext(STATE_CONTEXT_KEY, transformedState);
                }
                else {
                    scope.setContext(STATE_CONTEXT_KEY, null);
                }
                /* Allow user to configure scope with latest state */
                // eslint-disable-next-line @typescript-eslint/unbound-method
                var configureScopeWithState = options.configureScopeWithState;
                if (typeof configureScopeWithState === 'function') {
                    configureScopeWithState(scope, newState);
                }
            });
            return newState;
        };
        return next(sentryReducer, initialState);
    }; };
}
exports.createReduxEnhancer = createReduxEnhancer;
//# sourceMappingURL=redux.js.map