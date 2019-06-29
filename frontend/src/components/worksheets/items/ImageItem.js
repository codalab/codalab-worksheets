import * as React from 'react';
import Immutable from 'seamless-immutable';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';

class ImageItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({});
    }

    handleClick = (event) => {
        this.props.setFocus(this.props.focusIndex, 0);
    };

    shouldComponentUpdate(nextProps, nextState) {
        return worksheetItemPropsChanged(this.props, nextProps);
    }

    render() {
        var className = 'type-image' + (this.props.focused ? ' focused' : '');
        var src = 'data:image/png;base64,' + this.props.item.image_data;
        var styles = {};
        if (this.props.item.hasOwnProperty('height')) {
            styles['height'] = this.props.item.height + 'px;';
        }
        if (this.props.item.hasOwnProperty('width')) {
            styles['width'] = this.props.item.width + 'px;';
        }

        return (
            <div className='ws-item' onClick={this.handleClick}>
                <div className={className} ref={this.props.item.ref}>
                    <img style={styles} src={src} />
                </div>
            </div>
        );
    }
}

export default ImageItem;
