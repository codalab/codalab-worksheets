Object.defineProperty(exports, "__esModule", { value: true });
var tslib_1 = require("tslib");
var browser_1 = require("@sentry/browser");
var hoist_non_react_statics_1 = tslib_1.__importDefault(require("hoist-non-react-statics"));
var React = tslib_1.__importStar(require("react"));
exports.UNKNOWN_COMPONENT = 'unknown';
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
    tslib_1.__extends(ErrorBoundary, _super);
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
        browser_1.withScope(function (scope) {
            if (beforeCapture) {
                beforeCapture(scope, error, componentStack);
            }
            var eventId = browser_1.captureException(error, { contexts: { react: { componentStack: componentStack } } });
            if (onError) {
                onError(error, componentStack, eventId);
            }
            if (showDialog) {
                browser_1.showReportDialog(tslib_1.__assign(tslib_1.__assign({}, dialogOptions), { eventId: eventId }));
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
exports.ErrorBoundary = ErrorBoundary;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function withErrorBoundary(WrappedComponent, errorBoundaryOptions) {
    var componentDisplayName = WrappedComponent.displayName || WrappedComponent.name || exports.UNKNOWN_COMPONENT;
    var Wrapped = function (props) { return (React.createElement(ErrorBoundary, tslib_1.__assign({}, errorBoundaryOptions),
        React.createElement(WrappedComponent, tslib_1.__assign({}, props)))); };
    Wrapped.displayName = "errorBoundary(" + componentDisplayName + ")";
    // Copy over static methods from Wrapped component to Profiler HOC
    // See: https://reactjs.org/docs/higher-order-components.html#static-methods-must-be-copied-over
    hoist_non_react_statics_1.default(Wrapped, WrappedComponent);
    return Wrapped;
}
exports.withErrorBoundary = withErrorBoundary;
//# sourceMappingURL=errorboundary.js.map