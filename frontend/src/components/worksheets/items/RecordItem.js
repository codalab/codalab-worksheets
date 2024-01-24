import * as React from 'react';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
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

    receiveBundleInfoUpdates = (update) => {
        let { bundleInfoUpdates } = this.state;
        // Use object spread to update.
        bundleInfoUpdates = { ...bundleInfoUpdates, ...update };
        this.setState({ bundleInfoUpdates: { ...bundleInfoUpdates, ...update } });
    };

    render() {
        const { item, focusIndex, focused } = this.props;
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
        const uuid = bundleInfo.uuid;
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
                <div
                    className='type-record'
                    onClick={() => {
                        this.props.openBundle(uuid);
                    }}
                >
                    <table className={className}>
                        <tbody>{items}</tbody>
                    </table>
                </div>
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
