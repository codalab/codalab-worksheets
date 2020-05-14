import * as React from 'react';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import BundleDetail from '../BundleDetail';
import NewRun from '../NewRun';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';

class RecordItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            showDetail: false,
            showNewRun: 0,
            runProp: {},
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

    rerunItem = (runProp) => {
        this.setState({
            showDetail: false,
            showNewRun: 1,
            runProp: runProp,
        });
    };

    render() {
        const {
            item,
            reloadWorksheet,
            showNewRerun,
            onHideNewRerun,
            editPermission,
            focusIndex,
            focused,
            ws,
        } = this.props;
        if (focused) {
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
        var className = 'table table-record' + (focused ? ' focused' : '');
        var bundleInfo = item.bundles_spec.bundle_infos[0];
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
                        bundleMetadataChanged={reloadWorksheet}
                        onUpdate={this.receiveBundleInfoUpdates}
                        onClose={() => {
                            this.setState({
                                showDetail: false,
                            });
                        }}
                        rerunItem={this.rerunItem}
                        isFocused={focused}
                        focusIndex={focusIndex}
                        showNewRerun={showNewRerun}
                        showDetail={this.state.showDetail}
                        editPermission={editPermission}
                    />
                )}
                {/** ---------------------------------------------------------------------------------------------------
                 *  Rerun
                 */}
                {this.state.showNewRun === 1 && (
                    <NewRun
                        ws={ws}
                        onSubmit={() => {
                            this.setState({ showNewRun: 0, showDetail: false });
                            onHideNewRerun();
                        }}
                        after_sort_key={bundleInfo.sort_key}
                        reloadWorksheet={reloadWorksheet}
                        defaultRun={this.state.runProp}
                    />
                )}
            </div>
        );
    }
}

export default RecordItem;
