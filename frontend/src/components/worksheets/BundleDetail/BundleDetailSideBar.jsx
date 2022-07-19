import * as React from 'react';
import { formatBundle } from '../../../util/worksheet_utils';
import CollapseButton from '../../CollapseButton';
import { BundleFieldTable, BundleFieldRow } from './BundleFieldTable/';
import BundleStateTable from './BundleStateTable/';
import BundlePageLink from './BundlePageLink';
import BundleDependencies from './BundleDependencies';
import BundleHostWorksheets from './BundleHostWorksheets';
import BundlePermissions from './BundlePermissions';

/**
 * This component renders bundle metadata in a sidebar.
 *
 * It includes a dynamic bundle state component that disappears once
 * the bundle is ready.
 */
class BundleDetailSideBar extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            expandPermissons: false,
            showMoreDetails: this.props.expanded,
        };
    }

    toggleExpandPermissions() {
        this.setState({ expandPermissons: !this.state.expandPermissons });
    }

    toggleShowMoreDetails() {
        this.setState({ showMoreDetails: !this.state.showMoreDetails });
    }

    getStates() {
        const bundle_type = this.props.bundleInfo.bundle_type;
        if (bundle_type === 'run') {
            return ['created', 'staged', 'starting', 'preparing', 'running', 'finalizing', 'ready'];
        }
        if (bundle_type === 'dataset') {
            return ['created', 'uploading', 'ready'];
        }
        if (bundle_type === 'make') {
            return ['created', 'making', 'ready'];
        }
        return [];
    }

    render() {
        const { bundleInfo, hidePageLink, onUpdate, onMetaDataChange } = this.props;
        const { expandPermissons, showMoreDetails } = this.state;
        const bundle = formatBundle(bundleInfo);
        const uuid = bundle.uuid.value;
        const is_anonymous = bundle.is_anonymous.value;

        return (
            <>
                <BundleStateTable bundle={bundle} states={this.getStates()} />
                <BundleFieldTable>
                    <BundleFieldRow
                        label='Name'
                        field={bundle.name}
                        onChange={(name) => onUpdate({ name })}
                    />
                    <BundleFieldRow
                        label='Description'
                        field={bundle.description}
                        onChange={(description) => onUpdate({ description })}
                    />
                    <BundleFieldRow
                        label='Tags'
                        field={bundle.tags}
                        onChange={(tags) => onUpdate({ tags })}
                    />
                    {!is_anonymous && <BundleFieldRow label='Owner' field={bundle.user_name} />}
                    <BundleFieldRow
                        label='Permissions'
                        field={bundle.permission}
                        value={
                            <BundlePermissions
                                bundleInfo={bundleInfo}
                                onClick={() => this.toggleExpandPermissions()}
                                onChange={onMetaDataChange || function() {}}
                                showDialog={expandPermissons}
                            />
                        }
                    />
                    <BundleFieldRow label='Created' field={bundle.created} />
                    <BundleFieldRow label='Size' field={bundle.data_size} />
                    {showMoreDetails && (
                        <>
                            <BundleFieldRow label='UUID' field={bundle.uuid} allowCopy noWrap />
                            <BundleFieldRow
                                label='Dependencies'
                                field={bundle.dependencies}
                                value={<BundleDependencies bundleInfo={bundleInfo} />}
                            />
                            <BundleFieldRow
                                label='Allow Failed'
                                field={bundle.allow_failed_dependencies}
                                onChange={(allow_failed_dependencies) =>
                                    onUpdate({ allow_failed_dependencies })
                                }
                            />
                            <BundleFieldRow
                                label='Exclude Patterns'
                                field={bundle.exclude_patterns}
                                onChange={(exclude_patterns) => onUpdate({ exclude_patterns })}
                            />
                            <BundleFieldRow
                                label='Store'
                                field={bundle.store}
                                onChange={(store) => onUpdate({ store })}
                            />
                            <BundleFieldRow
                                label='Preemptible'
                                field={bundle.on_preemptible_worker}
                            />
                            <BundleFieldRow
                                label='Docker Image'
                                field={bundle.docker_image}
                                allowCopy
                                noWrap
                            />
                            <BundleFieldRow
                                label='Request Docker Image'
                                field={bundle.request_docker_image}
                                onChange={(request_docker_image) =>
                                    onUpdate({ request_docker_image })
                                }
                            />
                            <BundleFieldRow
                                label='Request Time'
                                field={bundle.request_time}
                                onChange={(request_time) => onUpdate({ request_time })}
                            />
                            <BundleFieldRow
                                label='Request Memory'
                                field={bundle.request_memory}
                                onChange={(request_memory) => onUpdate({ request_memory })}
                            />
                            <BundleFieldRow
                                label='Request Disk'
                                field={bundle.request_disk}
                                onChange={(request_disk) => onUpdate({ request_disk })}
                            />
                            <BundleFieldRow
                                label='Request CPUs'
                                field={bundle.request_cpus}
                                onChange={(request_cpus) => onUpdate({ request_cpus })}
                            />
                            <BundleFieldRow
                                label='Request GPUs'
                                field={bundle.request_gpus}
                                onChange={(request_gpus) => onUpdate({ request_gpus })}
                            />
                            <BundleFieldRow
                                label='Request Queue'
                                field={bundle.request_queue}
                                onChange={(request_queue) => onUpdate({ request_queue })}
                            />
                            <BundleFieldRow
                                label='Request Priority'
                                field={bundle.request_priority}
                                onChange={(request_priority) => onUpdate({ request_priority })}
                            />
                            <BundleFieldRow
                                label='Request Network'
                                field={bundle.request_network}
                                onChange={(request_network) => onUpdate({ request_network })}
                            />
                            <BundleFieldRow label='Time Preparing' field={bundle.time_preparing} />
                            <BundleFieldRow label='Time Running' field={bundle.time_running} />
                            <BundleFieldRow
                                label='Time Uploading'
                                field={bundle.time_uploading_results}
                            />
                            <BundleFieldRow
                                label='Time Cleaning Up'
                                field={bundle.time_cleaning_up}
                            />
                            <BundleFieldRow
                                label='License'
                                field={bundle.license}
                                onChange={(license) => onUpdate({ license })}
                            />
                            <BundleFieldRow
                                label='Source URL'
                                field={bundle.source_url}
                                onChange={(source_url) => onUpdate({ source_url })}
                            />
                            <BundleFieldRow
                                label='Link URL'
                                field={bundle.link_url}
                                onChange={(link_url) => onUpdate({ link_url })}
                            />
                            <BundleFieldRow
                                label='Link Format'
                                field={bundle.link_format}
                                onChange={(link_format) => onUpdate({ link_format })}
                            />
                            <BundleFieldRow
                                label='Host Worksheets'
                                field={bundle.host_worksheets}
                                value={<BundleHostWorksheets bundleInfo={bundleInfo} />}
                            />
                        </>
                    )}
                </BundleFieldTable>
                {!hidePageLink && showMoreDetails && <BundlePageLink uuid={uuid} />}
                <CollapseButton
                    label='More Details'
                    collapsedLabel='Less Details'
                    onClick={() => this.toggleShowMoreDetails()}
                    collapsed={showMoreDetails}
                    collapseUp
                />
            </>
        );
    }
}

export default BundleDetailSideBar;
