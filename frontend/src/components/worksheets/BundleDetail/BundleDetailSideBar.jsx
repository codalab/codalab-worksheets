import * as React from 'react';
import { withStyles } from '@material-ui/core';
import { formatBundle, shorten_uuid } from '../../../util/worksheet_utils';
import CollapseButton from '../../CollapseButton';
import NewWindowLink from '../../NewWindowLink';
import { BundleFieldTable, BundleFieldRow, BundleStateRow } from './BundleFieldTable/';
import BundlePermissions from './BundlePermissions';
import MoreDetail from './MoreDetail';

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
        const { bundleInfo, classes, hidePageLink, onUpdate, onMetaDataChange } = this.props;
        const { expandPermissons, showMoreDetail } = this.state;
        const bundle = formatBundle(bundleInfo);
        const uuid = bundle.uuid.value;
        const isAnonymous = bundle.is_anonymous.value;

        return (
            <div className={classes.sidebar}>
                {!hidePageLink && (
                    <NewWindowLink className={classes.pageLink} href={`/bundles/${uuid}`} />
                )}
                <BundleFieldTable>
                    <BundleStateRow bundle={bundle} />
                    <BundleFieldRow
                        label='UUID'
                        description="Click the copy icon to copy the bundle's full UUID."
                        value={`${shorten_uuid(uuid)}...`}
                        copyValue={uuid}
                        allowCopy
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
                    {!isAnonymous && <BundleFieldRow label='Owner' field={bundle.user_name} />}
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
                    <BundleFieldRow
                        label='Store'
                        field={bundle.store}
                        onChange={(store) => onUpdate({ store })}
                    />
                </BundleFieldTable>
                {showMoreDetail && <MoreDetail bundle={bundle} onUpdate={onUpdate} />}
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
    pageLink: {
        position: 'absolute',
        right: 0,
    },
    collapseBtn: {
        marginTop: 5,
    },
});

export default withStyles(styles)(BundleDetailSideBar);
