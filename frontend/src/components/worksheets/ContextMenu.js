import * as React from 'react';
import Immutable from 'seamless-immutable';
import EventEmitter from 'wolfy87-eventemitter';
import $ from 'jquery';
import _ from 'underscore';
import 'jquery.terminal';

var menuEvents = new EventEmitter();
var ContextMenuEnum = {
    type: {
        RUN: 1,
        BUNDLE: 2,
    },
    command: {
        REMOVE_BUNDLE: 1,
        DETACH_BUNDLE: 2,
        ADD_BUNDLE_TO_HOMEWORKSHEET: 3,
        KILL_BUNDLE: 4,
    },
};

// not really a mixin
var ContextMenuMixin = {
    openContextMenu: function(type, callback) {
        menuEvents.emit('open', {
            type: type,
            callback: callback,
        });
    },
    closeContextMenu: function() {
        menuEvents.emit('close');
    },
};

var mouse = { x: 0, y: 0 };
var updateMouse = function(e) {
    mouse.x = e.pageX;
    mouse.y = e.pageY;
};

class ContextMenu extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({
            type: null,
            callback: null,
        });
    }

    getInitialState() {
        return Immutable({
            type: null,
            callback: null,
        });
    }

    componentDidMount() {
        menuEvents.on('open', this.handleOpenEvent);
        menuEvents.on('close', this.closeMenu);
        window.addEventListener('mousedown', updateMouse, false);
        document.onclick = ContextMenuMixin.closeContextMenu;
    }

    handleOpenEvent = (payload) => {
        this.setState(Immutable({ ...payload, mouseState: mouse }));
        $('.ws-container').css({ overflow: 'hidden' });
    };

    closeMenu = () => {
        this.setState(this.getInitialState());
        $('.ws-container').css({ overflow: 'auto' });
    };

    render() {
        var bundleMap, runBundleMap;

        if (!this.props.userInfo) {
            bundleMap = {};
            runBundleMap = {};
        } else if (
            this.props.userInfo &&
            ((this.props.ws.info && !this.props.ws.info.edit_permission) || !this.props.ws.info)
        ) {
            bundleMap = {
                'Add to my home worksheet': [
                    ContextMenuEnum.command.ADD_BUNDLE_TO_HOMEWORKSHEET,
                    ['add', 'bundle'],
                ],
            };
            runBundleMap = bundleMap;
        } else {
            bundleMap = {
                'Remove bundle permanently': [ContextMenuEnum.command.REMOVE_BUNDLE, ['rm']],
                'Detach from this worksheet': [ContextMenuEnum.command.DETACH_BUNDLE, ['detach']],
                'Add to my home worksheet': [
                    ContextMenuEnum.command.ADD_BUNDLE_TO_HOMEWORKSHEET,
                    ['add', 'bundle'],
                ],
            };
            runBundleMap = _.extend({}, bundleMap, {
                'Kill this run bundle': [ContextMenuEnum.command.KILL_BUNDLE, ['kill']],
            });
        }

        let labelMap = {};
        labelMap[ContextMenuEnum.type.RUN] = runBundleMap;
        labelMap[ContextMenuEnum.type.BUNDLE] = bundleMap;

        if (!this.state.type) {
            return null;
        }
        // estimate of the height and weight of the context menu
        var height = 32 * Object.keys(labelMap[this.state.type]).length;
        var width = 250;
        var style = {
            left: Math.min(this.state.mouseState.x, window.innerWidth - width),
            // avoid overflow below the window limit (30 is the height of the bottom bar)
            top: Math.min(this.state.mouseState.y, window.innerHeight - height - 30),
            position: 'fixed',
        };

        return (
            <div className='context-menu' style={style}>
                {Object.keys(labelMap[this.state.type]).map(function(display, i) {
                    return (
                        <div
                            className='context-menu-item'
                            key={i}
                            onClick={this.makeClickHandler(labelMap[this.state.type][display])}
                        >
                            {display}
                        </div>
                    );
                }, this)}
            </div>
        );
    }

    makeClickHandler = (option) => {
        return function() {
            if (this.state.callback) {
                this.state.callback(option);
                this.setState(this.getInitialState());
            }
        }.bind(this);
    };
}

export { ContextMenuMixin, ContextMenuEnum };
export default ContextMenu;
