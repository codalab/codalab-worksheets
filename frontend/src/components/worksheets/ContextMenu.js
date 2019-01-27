import * as React from 'react';
import Immutable from 'seamless-immutable';
import EventEmitter from 'wolfy87-eventemitter';

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

class ContextMenu extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    /** Renderer. */
    render() {
        return <div>ContextMenu</div>;
    }
}

export { ContextMenuMixin, ContextMenuEnum };
export default ContextMenu;
