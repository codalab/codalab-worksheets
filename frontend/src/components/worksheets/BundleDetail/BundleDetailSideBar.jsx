import * as React from 'react';
import { withStyles } from '@material-ui/core';
import { formatBundle } from '../../../util/worksheet_utils';
import { FINAL_BUNDLE_STATES } from '../../../constants';
import CollapseButton from '../../CollapseButton';
import NewWindowLink from '../../NewWindowLink';
import { BundleFieldTable, BundleFieldRow, BundleStateRow } from './BundleFieldTable/';
import BundleDependencies from './BundleDependencies';
import BundleHostWorksheets from './BundleHostWorksheets';
import BundlePermissions from './BundlePermissions';
import { fetchBundleStores } from '../../../util/apiWrapper';

/**
 * This component renders bundle metadata in a sidebar.
 * Top-level fields like state, uuid and name are rendered for all bundle types.
 *
 * It includes a dynamic bundle state component that disappears once
 * the bundle is ready.
 */
class BundleDetailSideBar extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            expandPermissons: false,
            showMoreDetail: this.props.expanded,
        };
    }

    toggleExpandPermissions() {
        this.setState({ expandPermissons: !this.state.expandPermissons });
    }

    toggleShowMoreDetail() {
        this.setState({ showMoreDetail: !this.state.showMoreDetail });
    }

    render() {
        const { bundleInfo, classes, hidePageLink, onUpdate, onMetadataChange } = this.props;
        const { expandPermissons, showMoreDetail } = this.state;
        const bundle = formatBundle(bundleInfo);
        console.log(bundle)
        console.log(bundleInfo)
        const bundleType = bundle.bundle_type.value;
        const uuid = bundle.uuid.value;
        const state = bundle.state.value;

        const showRunFields = bundleType === 'run';
        const showDatasetFields = bundleType === 'dataset';
        const showTime = showRunFields && FINAL_BUNDLE_STATES.includes(state);
        const showPageLink = !hidePageLink;
        const showOwner = !bundle.is_anonymous.value;
        const showDependencies = !!bundle.dependencies?.value?.length;
        const showHostWorksheets = !!bundle.host_worksheets?.value.length;
        let bundleStore;
        fetchBundleStores(uuid)
            .then((response) => {
                const {
                    data: { attributes: { name: bundleStore } },
                } = response;
                console.log(bundleStore);
                console.log('!!!!!!!!'); console.log(response);});
        return (
            <div className={classes.sidebar}>
                {showPageLink && (
                    <NewWindowLink
                        style={{ position: 'absolute', right: -1 }}
                        href={`/bundles/${uuid}`}
                    />
                )}
                <BundleFieldTable>
                    <BundleStateRow bundle={bundle} />
                    <BundleFieldRow
                        label='UUID'
                        description="Click the copy icon to copy the bundle's full UUID."
                        field={bundle.uuid}
                        allowCopy
                        noWrap
                    />
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
                    {showOwner && (
                        <BundleFieldRow
                            label='Owner'
                            description='The user who owns this bundle.'
                            field={bundle.user_name}
                        />
                    )}
                    <BundleFieldRow
                        label='Permissions'
                        description='Click the right arrow to expand permissions settings.'
                        field={bundle.permission}
                        value={
                            <BundlePermissions
                                bundleInfo={bundleInfo}
                                onClick={() => this.toggleExpandPermissions()}
                                onChange={onMetadataChange}
                                showDialog={expandPermissons}
                            />
                        }
                    />
                    <BundleFieldRow label='Created' field={bundle.created} />
                    <BundleFieldRow label='Size' field={bundle.data_size} />
                    {showRunFields && (
                        <BundleFieldRow label='Remote' field={bundle.remote} allowCopy noWrap />
                    )}
                    {(showRunFields || showDatasetFields) && (
                        <BundleFieldRow
                            label='Store'
                            field={bundleStore}
                            onChange={(store) => onUpdate({ store })}
                        />
                    )}
                </BundleFieldTable>
                {showTime && (
                    <BundleFieldTable title='Time'>
                        <BundleFieldRow label='Time' field={bundle.time} />
                        <BundleFieldRow label='Time Preparing' field={bundle.time_preparing} />
                        <BundleFieldRow label='Time Running' field={bundle.time_running} />
                        <BundleFieldRow
                            label='Time Uploading'
                            field={bundle.time_uploading_results}
                        />
                        <BundleFieldRow label='Time Cleaning Up' field={bundle.time_cleaning_up} />
                        <BundleFieldRow
                            label='Time Allowed'
                            field={bundle.request_time}
                            onChange={(request_time) => onUpdate({ request_time })}
                        />
                    </BundleFieldTable>
                )}
                {showDependencies && (
                    <BundleFieldTable title='Dependencies'>
                        <BundleDependencies bundle={bundle} />
                    </BundleFieldTable>
                )}
                {showMoreDetail && (
                    <>
                        {showRunFields && (
                            <>
                                <BundleFieldTable title='Resources'>
                                    <BundleFieldRow
                                        label='Disk'
                                        field={bundle.request_disk}
                                        onChange={(request_disk) => onUpdate({ request_disk })}
                                    />
                                    <BundleFieldRow
                                        label='Memory'
                                        field={bundle.request_memory}
                                        onChange={(request_memory) => onUpdate({ request_memory })}
                                    />
                                    <BundleFieldRow
                                        label='CPUs'
                                        field={bundle.request_cpus}
                                        onChange={(request_cpus) => onUpdate({ request_cpus })}
                                    />
                                    <BundleFieldRow
                                        label='GPUs'
                                        field={bundle.request_gpus}
                                        onChange={(request_gpus) => onUpdate({ request_gpus })}
                                    />
                                    <BundleFieldRow
                                        label='Docker Image Requested'
                                        field={bundle.request_docker_image}
                                        onChange={(request_docker_image) =>
                                            onUpdate({ request_docker_image })
                                        }
                                    />
                                    <BundleFieldRow
                                        label='Docker Image Used'
                                        field={bundle.docker_image}
                                        allowCopy
                                        noWrap
                                    />
                                    <BundleFieldRow
                                        label='Preemptible'
                                        field={bundle.on_preemptible_worker}
                                    />
                                    <BundleFieldRow
                                        label='Queue'
                                        field={bundle.request_queue}
                                        onChange={(request_queue) => onUpdate({ request_queue })}
                                    />
                                    <BundleFieldRow
                                        label='Priority'
                                        field={bundle.request_priority}
                                        onChange={(request_priority) =>
                                            onUpdate({ request_priority })
                                        }
                                    />
                                    <BundleFieldRow
                                        label='Network'
                                        field={bundle.request_network}
                                        onChange={(request_network) =>
                                            onUpdate({ request_network })
                                        }
                                    />
                                    <BundleFieldRow
                                        label='Failed Dependencies'
                                        field={bundle.allow_failed_dependencies}
                                        onChange={(allow_failed_dependencies) =>
                                            onUpdate({ allow_failed_dependencies })
                                        }
                                    />
                                </BundleFieldTable>
                                <BundleFieldTable title='Exclusions'>
                                    <BundleFieldRow
                                        label='Exclude Patterns'
                                        field={bundle.exclude_patterns}
                                        onChange={(exclude_patterns) =>
                                            onUpdate({ exclude_patterns })
                                        }
                                    />
                                </BundleFieldTable>
                            </>
                        )}
                        {showDatasetFields && (
                            <BundleFieldTable title='Sources'>
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
                            </BundleFieldTable>
                        )}
                        {showHostWorksheets && (
                            <BundleFieldTable title='Worksheets'>
                                <BundleFieldRow
                                    label='Host Worksheets'
                                    description='Worksheets associated with this bundle.'
                                    field={bundle.host_worksheets}
                                    value={<BundleHostWorksheets bundle={bundle} />}
                                />
                            </BundleFieldTable>
                        )}
                    </>
                )}
                <CollapseButton
                    containerClass={classes.collapseBtn}
                    label='More Detail'
                    collapsedLabel='Less Detail'
                    onClick={() => this.toggleShowMoreDetail()}
                    collapsed={showMoreDetail}
                    collapseUp
                />
            </div>
        );
    }
}

const styles = () => ({
    sidebar: {
        position: 'relative',
    },
    collapseBtn: {
        marginTop: 5,
    },
});

export default withStyles(styles)(BundleDetailSideBar);
