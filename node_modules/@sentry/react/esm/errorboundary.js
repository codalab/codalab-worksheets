import { __assign, __extends } from "tslib";
import { captureException, showReportDialog, withScope } from '@sentry/browser';
import hoistNonReactStatics from 'hoist-non-react-statics';
import * as React from 'react';
export var UNKNOWN_COMPONENT = 'unknown';
var INITIAL_STATE = {
    componentStack: null,
    error: null,
    eventId: null,
};
/**
 * A ErrorBoundary component that logs errors to Sentry.
 * Requires React >= 16
 */
var ErrorBoundary = /** @class */ (function (_super) {
    __extends(ErrorBoundary, _super);
    function ErrorBoundary() {
        var _this = _super !== null && _super.apply(this, arguments) || this;
        _this.state = INITIAL_STATE;
        _this.resetErrorBoundary = function () {
            var onReset = _this.props.onReset;
            var _a = _this.state, error = _a.error, componentStack = _a.componentStack, eventId = _a.eventId;
            if (onReset) {
                onReset(error, componentStack, eventId);
            }
            _this.setState(INITIAL_STATE);
        };
        return _this;
    }
    ErrorBoundary.prototype.componentDidCatch = function (error, _a) {
        var _this = this;
        var componentStack = _a.componentStack;
        var _b = this.props, beforeCapture = _b.beforeCapture, onError = _b.onError, showDialog = _b.showDialog, dialogOptions = _b.dialogOptions;
        withScope(function (scope) {
            if (beforeCapture) {
                beforeCapture(scope, error, componentStack);
            }
            var eventId = captureException(error, { contexts: { react: { componentStack: componentStack } } });
            if (onError) {
                onError(error, componentStack, eventId);
            }
            if (showDialog) {
                showReportDialog(__assign(__assign({}, dialogOptions), { eventId: eventId }));
            }
            // componentDidCatch is used over getDerivedStateFromError
            // so that componentStack is accessible through state.
            _this.setState({ error: error, componentStack: componentStack, eventId: eventId });
        });
    };
    ErrorBoundary.prototype.componentDidMount = function () {
        var onMount = this.props.onMount;
        if (onMount) {
            onMount();
        }
    };
    ErrorBoundary.prototype.componentWillUnmount = function () {
        var _a = this.state, error = _a.error, componentStack = _a.componentStack, eventId = _a.eventId;
        var onUnmount = this.props.onUnmount;
        if (onUnmount) {
            onUnmount(error, componentStack, eventId);
        }
    };
    ErrorBoundary.prototype.render = function () {
        var fallback = this.props.fallback;
        var _a = this.state, error = _a.error, componentStack = _a.componentStack, eventId = _a.eventId;
        if (error) {
            if (React.isValidElement(fallback)) {
                return fallback;
            }
            if (typeof fallback === 'function') {
                return fallback({ error: error, componentStack: componentStack, resetError: this.resetErrorBoundary, eventId: eventId });
            }
            // Fail gracefully if no fallback provided
            return null;
        }
        return this.props.children;
    };
    return ErrorBoundary;
}(React.Component));
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function withErrorBoundary(WrappedComponent, errorBoundaryOptions) {
    var componentDisplayName = WrappedComponent.displayName || WrappedComponent.name || UNKNOWN_COMPONENT;
    var Wrapped = function (props) { return (React.createElement(ErrorBoundary, __assign({}, errorBoundaryOptions),
        React.createElement(WrappedComponent, __assign({}, props)))); };
    Wrapped.displayName = "errorBoundary(" + componentDisplayName + ")";
    // Copy over static methods from Wrapped component to Profiler HOC
    // See: https://reactjs.org/docs/higher-order-components.html#static-methods-must-be-copied-over
    hoistNonReactStatics(Wrapped, WrappedComponent);
    return Wrapped;
}
export { ErrorBoundary, withErrorBoundary };
//# sourceMappingURL=errorboundary.js.map