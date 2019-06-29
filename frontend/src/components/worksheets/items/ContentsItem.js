import * as React from 'react';
import Immutable from 'seamless-immutable';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';

class ContentsItem extends React.Component {
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
        var className = 'type-contents' + (this.props.focused ? ' focused' : '');
        if (!this.props.item.lines) {
            return <div />;
        }
        var contents = this.props.item.lines.join('');
        var bundleInfo = this.props.item.bundles_spec.bundle_infos[0];
        return (
            <div
                className='ws-item'
                onClick={this.handleClick}
                onContextMenu={this.props.handleContextMenu.bind(
                    null,
                    bundleInfo.uuid,
                    this.props.focusIndex,
                    0,
                    bundleInfo.bundle_type === 'run',
                )}
            >
                <div className={className} ref={this.props.item.ref}>
                    <blockquote>
                        <p>{contents}</p>
                    </blockquote>
                </div>
            </div>
        );
    }
}

export default ContentsItem;
