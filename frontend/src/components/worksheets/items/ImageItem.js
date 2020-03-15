import * as React from 'react';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import BundleDetail from '../BundleDetail';

class ImageItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            showDetail: false,
            bundleInfoUpdates: {},
        };
    }

    receiveBundleInfoUpdates = (update) => {
        let { bundleInfoUpdates } = this.state;
        // Use object spread to update.
        bundleInfoUpdates = { ...bundleInfoUpdates, ...update };
        this.setState({ bundleInfoUpdates: { ...bundleInfoUpdates, ...update } });
    };

    handleClick = () => {
        this.props.setFocus(this.props.focusIndex, 0);
        this.setState({ showDetail: !this.state.showDetail });
    };

    render() {
        if (this.props.focused) {
            // Use e.preventDefault to avoid openning selected link
            Mousetrap.bind(
                ['enter'],
                (e) => {
                    e.preventDefault();
                    if (!this.props.confirmBundleRowAction(e.code)) {
                        this.setState({ showDetail: !this.state.showDetail });
                    }
                },
                'keydown',
            );
        }
        const item = this.props.item;
        const bundleInfo = item.bundles_spec.bundle_infos[0];
        var className = 'type-image' + (this.props.focused ? ' focused' : '');
        var src = 'data:image/png;base64,' + this.props.item.image_data;
        var styles = {};
        if (this.props.item.hasOwnProperty('height')) {
            styles['height'] = this.props.item.height + 'px';
        }
        if (this.props.item.hasOwnProperty('width')) {
            styles['width'] = this.props.item.width + 'px';
        }
        return (
            <div className='ws-item'>
                <div className={className} ref={this.props.item.ref} onClick={this.handleClick}>
                    <img style={styles} src={src} />
                </div>
                {this.state.showDetail && (
                    <BundleDetail
                        uuid={bundleInfo.uuid}
                        ref='bundleDetail'
                        bundleMetadataChanged={this.props.reloadWorksheet}
                        onUpdate={this.receiveBundleInfoUpdates}
                        onClose={() => {
                            this.setState({
                                showDetail: false,
                            });
                        }}
                        isFocused={this.props.focused}
                        focusIndex={this.props.focusIndex}
                        showDetail={this.state.showDetail}
                        editPermission={this.props.editPermission}
                    />
                )}
            </div>
        );
    }
}

export default ImageItem;
