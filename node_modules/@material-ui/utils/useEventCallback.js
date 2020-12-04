"use strict";

var _interopRequireDefault = require("@babel/runtime/helpers/interopRequireDefault");

var _interopRequireWildcard = require("@babel/runtime/helpers/interopRequireWildcard");

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.default = useEventCallback;

var React = _interopRequireWildcard(require("react"));

var _useEnhancedEffect = _interopRequireDefault(require("./useEnhancedEffect"));

/**
 * https://github.com/facebook/react/issues/14099#issuecomment-440013892
 * @param {function} fn
 */
function useEventCallback(fn) {
  const ref = React.useRef(fn);
  (0, _useEnhancedEffect.default)(() => {
    ref.current = fn;
  });
  return React.useCallback((...args) => (0, ref.current)(...args), []);
}