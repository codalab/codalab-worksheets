import * as React from 'react';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import BundleDetail from '../BundleDetail';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';

class RecordItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            showDetail: false,
            bundleInfoUpdates: {},
        };
    }

    handleClick = () => {
        this.props.setFocus(this.props.focusIndex, 0);
        this.setState({ showDetail: !this.state.showDetail });
    };

    receiveBundleInfoUpdates = (update) => {
        let { bundleInfoUpdates } = this.state;
        // Use object spread to update.
        bundleInfoUpdates = { ...bundleInfoUpdates, ...update };
        this.setState({ bundleInfoUpdates: { ...bundleInfoUpdates, ...update } });
    };

    shouldComponentUpdate(nextProps, nextState) {
        return (
            worksheetItemPropsChanged(this.props, nextProps) ||
            this.state.showDetail !== nextState.showDetail
        );
    }

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
        var item = this.props.item;
        var className = 'table table-record' + (this.props.focused ? ' focused' : '');
        var bundleInfo = this.props.item.bundles_spec.bundle_infos[0];
        var header = item.header;
        var k = header[0];
        var v = header[1];
        var items = item.rows.map(function(item, index) {
            var ref = 'row' + index;
            return (
                <tr ref={ref} key={index}>
                    <th>{item[k]}</th>
                    <td style={{ maxWidth: '500px', wordWrap: 'break-word' }}>
                        {JSON.stringify(item[v])}
                    </td>
                </tr>
            );
        });

        return (
            <div className='ws-item'>
                <div className='type-record' onClick={this.handleClick}>
                    <table className={className}>
                        <tbody>{items}</tbody>
                    </table>
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

export default RecordItem;
