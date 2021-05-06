Object.defineProperty(exports, "__esModule", { value: true });
var tslib_1 = require("tslib");
/* eslint-disable @typescript-eslint/no-explicit-any */
var browser_1 = require("@sentry/browser");
var utils_1 = require("@sentry/utils");
var hoist_non_react_statics_1 = tslib_1.__importDefault(require("hoist-non-react-statics"));
var React = tslib_1.__importStar(require("react"));
exports.UNKNOWN_COMPONENT = 'unknown';
var TRACING_GETTER = {
    id: 'Tracing',
};
var globalTracingIntegration = null;
/** @deprecated remove when @sentry/apm no longer used */
var getTracingIntegration = function () {
    if (globalTracingIntegration) {
        return globalTracingIntegration;
    }
    globalTracingIntegration = browser_1.getCurrentHub().getIntegration(TRACING_GETTER);
    return globalTracingIntegration;
};
/**
 * pushActivity creates an new react activity.
 * Is a no-op if Tracing integration is not valid
 * @param name displayName of component that started activity
 * @deprecated remove when @sentry/apm no longer used
 */
function pushActivity(name, op) {
    if (globalTracingIntegration === null) {
        return null;
    }
    return globalTracingIntegration.constructor.pushActivity(name, {
        description: "<" + name + ">",
        op: "react." + op,
    });
}
/**
 * popActivity removes a React activity.
 * Is a no-op if Tracing integration is not valid.
 * @param activity id of activity that is being popped
 * @deprecated remove when @sentry/apm no longer used
 */
function popActivity(activity) {
    if (activity === null || globalTracingIntegration === null) {
        return;
    }
    globalTracingIntegration.constructor.popActivity(activity);
}
/**
 * Obtain a span given an activity id.
 * Is a no-op if Tracing integration is not valid.
 * @param activity activity id associated with obtained span
 * @deprecated remove when @sentry/apm no longer used
 */
function getActivitySpan(activity) {
    if (activity === null || globalTracingIntegration === null) {
        return undefined;
    }
    return globalTracingIntegration.constructor.getActivitySpan(activity);
}
/**
 * The Profiler component leverages Sentry's Tracing integration to generate
 * spans based on component lifecycles.
 */
var Profiler = /** @class */ (function (_super) {
    tslib_1.__extends(Profiler, _super);
    function Profiler(props) {
        var _this = _super.call(this, props) || this;
        // The activity representing how long it takes to mount a component.
        _this._mountActivity = null;
        // The span of the mount activity
        _this._mountSpan = undefined;
        var _a = _this.props, name = _a.name, _b = _a.disabled, disabled = _b === void 0 ? false : _b;
        if (disabled) {
            return _this;
        }
        // If they are using @sentry/apm, we need to push/pop activities
        // eslint-disable-next-line deprecation/deprecation
        if (getTracingIntegration()) {
            // eslint-disable-next-line deprecation/deprecation
            _this._mountActivity = pushActivity(name, 'mount');
        }
        else {
            var activeTransaction = getActiveTransaction();
            if (activeTransaction) {
                _this._mountSpan = activeTransaction.startChild({
                    description: "<" + name + ">",
                    op: 'react.mount',
                });
            }
        }
        return _this;
    }
    // If a component mounted, we can finish the mount activity.
    Profiler.prototype.componentDidMount = function () {
        if (this._mountSpan) {
            this._mountSpan.finish();
        }
        else {
            // eslint-disable-next-line deprecation/deprecation
            this._mountSpan = getActivitySpan(this._mountActivity);
            // eslint-disable-next-line deprecation/deprecation
            popActivity(this._mountActivity);
            this._mountActivity = null;
        }
    };
    Profiler.prototype.componentDidUpdate = function (_a) {
        var _this = this;
        var updateProps = _a.updateProps, _b = _a.includeUpdates, includeUpdates = _b === void 0 ? true : _b;
        // Only generate an update span if hasUpdateSpan is true, if there is a valid mountSpan,
        // and if the updateProps have changed. It is ok to not do a deep equality check here as it is expensive.
        // We are just trying to give baseline clues for further investigation.
        if (includeUpdates && this._mountSpan && updateProps !== this.props.updateProps) {
            // See what props haved changed between the previous props, and the current props. This is
            // set as data on the span. We just store the prop keys as the values could be potenially very large.
            var changedProps = Object.keys(updateProps).filter(function (k) { return updateProps[k] !== _this.props.updateProps[k]; });
            if (changedProps.length > 0) {
                // The update span is a point in time span with 0 duration, just signifying that the component
                // has been updated.
                var now = utils_1.timestampWithMs();
                this._mountSpan.startChild({
                    data: {
                        changedProps: changedProps,
                    },
                    description: "<" + this.props.name + ">",
                    endTimestamp: now,
                    op: "react.update",
                    startTimestamp: now,
                });
            }
        }
    };
    // If a component is unmounted, we can say it is no longer on the screen.
    // This means we can finish the span representing the component render.
    Profiler.prototype.componentWillUnmount = function () {
        var _a = this.props, name = _a.name, _b = _a.includeRender, includeRender = _b === void 0 ? true : _b;
        if (this._mountSpan && includeRender) {
            // If we were able to obtain the spanId of the mount activity, we should set the
            // next activity as a child to the component mount activity.
            this._mountSpan.startChild({
                description: "<" + name + ">",
                endTimestamp: utils_1.timestampWithMs(),
                op: "react.render",
                startTimestamp: this._mountSpan.endTimestamp,
            });
        }
    };
    Profiler.prototype.render = function () {
        return this.props.children;
    };
    // eslint-disable-next-line @typescript-eslint/member-ordering
    Profiler.defaultProps = {
        disabled: false,
        includeRender: true,
        includeUpdates: true,
    };
    return Profiler;
}(React.Component));
exports.Profiler = Profiler;
/**
 * withProfiler is a higher order component that wraps a
 * component in a {@link Profiler} component. It is recommended that
 * the higher order component be used over the regular {@link Profiler} component.
 *
 * @param WrappedComponent component that is wrapped by Profiler
 * @param options the {@link ProfilerProps} you can pass into the Profiler
 */
function withProfiler(WrappedComponent, 
// We do not want to have `updateProps` given in options, it is instead filled through the HOC.
options) {
    var componentDisplayName = (options && options.name) || WrappedComponent.displayName || WrappedComponent.name || exports.UNKNOWN_COMPONENT;
    var Wrapped = function (props) { return (React.createElement(Profiler, tslib_1.__assign({}, options, { name: componentDisplayName, updateProps: props }),
        React.createElement(WrappedComponent, tslib_1.__assign({}, props)))); };
    Wrapped.displayName = "profiler(" + componentDisplayName + ")";
    // Copy over static methods from Wrapped component to Profiler HOC
    // See: https://reactjs.org/docs/higher-order-components.html#static-methods-must-be-copied-over
    hoist_non_react_statics_1.default(Wrapped, WrappedComponent);
    return Wrapped;
}
exports.withProfiler = withProfiler;
/**
 *
 * `useProfiler` is a React hook that profiles a React component.
 *
 * Requires React 16.8 or above.
 * @param name displayName of component being profiled
 */
function useProfiler(name, options) {
    if (options === void 0) { options = {
        disabled: false,
        hasRenderSpan: true,
    }; }
    var _a = tslib_1.__read(React.useState(function () {
        if (options && options.disabled) {
            return undefined;
        }
        var activeTransaction = getActiveTransaction();
        if (activeTransaction) {
            return activeTransaction.startChild({
                description: "<" + name + ">",
                op: 'react.mount',
            });
        }
        return undefined;
    }), 1), mountSpan = _a[0];
    React.useEffect(function () {
        if (mountSpan) {
            mountSpan.finish();
        }
        return function () {
            if (mountSpan && options.hasRenderSpan) {
                mountSpan.startChild({
                    description: "<" + name + ">",
                    endTimestamp: utils_1.timestampWithMs(),
                    op: "react.render",
                    startTimestamp: mountSpan.endTimestamp,
                });
            }
        };
        // We only want this to run once.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);
}
exports.useProfiler = useProfiler;
/** Grabs active transaction off scope */
function getActiveTransaction(hub) {
    if (hub === void 0) { hub = browser_1.getCurrentHub(); }
    if (hub) {
        var scope = hub.getScope();
        if (scope) {
            return scope.getTransaction();
        }
    }
    return undefined;
}
exports.getActiveTransaction = getActiveTransaction;
//# sourceMappingURL=profiler.js.map