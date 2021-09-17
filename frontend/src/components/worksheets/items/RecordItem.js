import * as React from 'react';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import BundleDetail from '../BundleDetail';
import NewRun from '../NewRun';
import { useEffect } from 'react';
import { FETCH_STATUS_SCHEMA } from '../../../constants';
import { fetchAsyncBundleContents } from '../../../util/apiWrapper';

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
        let className = 'table table-record' + (focused ? ' focused' : '');
        let bundleInfo = item.bundles_spec.bundle_infos[0];
        let header = item.header;
        let k = header[0];
        let v = header[1];
        let items = item.rows.map(function(item, index) {
            let displayValue = JSON.stringify(item[v]); // stringify is needed to convert metadata objects
            if (displayValue) {
                displayValue = displayValue.substr(1, displayValue.length - 2); // get rid of ""
            }

            return (
                <tr id={`codalab-worksheet-item-${focusIndex}-subitem-${index}`} key={index}>
                    <th>{item[k]}</th>
                    <td style={{ maxWidth: '500px', wordWrap: 'break-word' }}>{displayValue}</td>
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

const RecordWrapper = (props) => {
    const { item, onAsyncItemLoad } = props;
    useEffect(() => {
        (async function() {
            if (item.status.code === FETCH_STATUS_SCHEMA.BRIEFLY_LOADED) {
                try {
                    const { contents } = await fetchAsyncBundleContents({
                        contents: item.rows,
                    });
                    onAsyncItemLoad({
                        ...item,
                        rows: contents,
                        status: {
                            code: FETCH_STATUS_SCHEMA.READY,
                            error_message: '',
                        },
                    });
                } catch (e) {
                    console.error(e);
                    // TODO: better error message handling here.
                }
            }
        })();
        // TODO: see how we can add onAsyncItemLoad as a dependency, if needed.
    }, [item, item.rows, item.status, onAsyncItemLoad]);
    return <RecordItem {...props} />;
};

export default RecordWrapper;

// export default RecordItem;
