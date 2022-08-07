import React from 'react';
import { BundleFieldTable, BundleFieldRow } from './BundleFieldTable/';
import BundleDependencies from './BundleDependencies';
import BundleHostWorksheets from './BundleHostWorksheets';

/**
 * Within the bundle detail sidebar, there are "top-level" fields that are
 * rendered for every bundle type by default.
 *
 * This component manages which additional bundle fields are rendered when
 * a user opts to view more bundle detail.
 */
class MoreDetail extends React.Component {
    constructor(props) {
        super(props);
    }

    checkSources() {
        const bundle = this.props.bundle;
        const sourceFields = ['license', 'source_url', 'link_url', 'link_format'];
        for (let i in sourceFields) {
            const field = sourceFields[i];
            if (bundle[field]?.editable || bundle[field]?.value) {
                return true;
            }
        }
        return false;
    }

    render() {
        const { bundle, onUpdate } = this.props;
        const bundleType = bundle.bundle_type.value;
        const exclusions = bundle.exclude_patterns;

        const showResources = bundleType === 'run';
        const showSources = bundleType === 'dataset' ? this.checkSources() : false;
        const showExclusions = exclusions?.value.length || exclusions?.editable;
        const showTime = bundle.time?.value || bundle.request_time?.editable;
        const showDependencies = !!bundle.dependencies?.value?.length;
        const showHostWorksheets = !!bundle.host_worksheets?.value.length;

        return (
            <>
                {showResources && (
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
                            onChange={(request_docker_image) => onUpdate({ request_docker_image })}
                        />
                        <BundleFieldRow
                            label='Docker Image Used'
                            field={bundle.docker_image}
                            allowCopy
                            noWrap
                        />
                        <BundleFieldRow label='Remote' field={bundle.remote} />
                        <BundleFieldRow label='Preemptible' field={bundle.on_preemptible_worker} />
                        <BundleFieldRow
                            label='Queue'
                            field={bundle.request_queue}
                            onChange={(request_queue) => onUpdate({ request_queue })}
                        />
                        <BundleFieldRow
                            label='Priority'
                            field={bundle.request_priority}
                            onChange={(request_priority) => onUpdate({ request_priority })}
                        />
                        <BundleFieldRow
                            label='Network'
                            field={bundle.request_network}
                            onChange={(request_network) => onUpdate({ request_network })}
                        />
                    </BundleFieldTable>
                )}
                {showTime && (
                    <BundleFieldTable title='Time'>
                        <BundleFieldRow
                            label='Time Allowed'
                            field={bundle.request_time}
                            onChange={(request_time) => onUpdate({ request_time })}
                        />
                        <BundleFieldRow label='Time Preparing' field={bundle.time_preparing} />
                        <BundleFieldRow label='Time Running' field={bundle.time_running} />
                        <BundleFieldRow
                            label='Time Uploading'
                            field={bundle.time_uploading_results}
                        />
                        <BundleFieldRow label='Time Cleaning Up' field={bundle.time_cleaning_up} />
                        <BundleFieldRow label='Total Time' field={bundle.time} />
                    </BundleFieldTable>
                )}
                {showSources && (
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
                {showExclusions && (
                    <BundleFieldTable title='Contents'>
                        <BundleFieldRow
                            label='Exclude Patterns'
                            field={bundle.exclude_patterns}
                            onChange={(exclude_patterns) => onUpdate({ exclude_patterns })}
                        />
                    </BundleFieldTable>
                )}
                {showDependencies && (
                    <BundleFieldTable title='Dependencies'>
                        <BundleFieldRow
                            label='Failed Dependencies'
                            field={bundle.allow_failed_dependencies}
                            onChange={(allow_failed_dependencies) =>
                                onUpdate({ allow_failed_dependencies })
                            }
                        />
                        <BundleFieldRow
                            label='Dependencies'
                            description='Bundles that this bundle depends on.'
                            field={bundle.dependencies}
                            value={<BundleDependencies bundle={bundle} />}
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
        );
    }
}

export default MoreDetail;
