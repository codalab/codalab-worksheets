// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';

import { BundleEditableField } from '../../EditableField';
import PermissionDialog from '../PermissionDialog';
import { renderFormat, shorten_uuid } from '../../../util/worksheet_utils';
import { ConfigLabel } from '../ConfigPanel';

class Dependency extends React.PureComponent<
    {
        bundleInfo: {},
        classes: {},
    }
> {

  render() {
    const { bundleInfo } = this.props;
    let dependencies_table = [];
    if (!bundleInfo.dependencies || bundleInfo.dependencies.length == 0) return <div />;

    bundleInfo.dependencies.forEach((dep, i) => {
        let dep_bundle_url = '/bundles/' + dep.parent_uuid;
        dependencies_table.push(
            <TableRow key={ dep.parent_uuid + i }>
                <TableCell>
                    <Typography variant="body1">{ dep.child_path }</Typography>
                </TableCell>
                <TableCell>
                    <Typography variant="body1">
                        &rarr; { dep.parent_name }(
                        <a href={ dep_bundle_url } target="_blank">
                          { shorten_uuid(dep.parent_uuid) }
                        </a>)
                        { dep.parent_path ? '/' + dep.parent_path : '' }
                    </Typography>
                </TableCell>
            </TableRow>
        );
    });

    return (
        <div>
            <Typography variant="body1">Dependencies</Typography>
            <Table>
                <TableBody>{ dependencies_table }</TableBody>
            </Table>
        </div>
    );
  }
}

/**
 * Content to display in sidebar of a Bundle Detail expansion panel.
 */
class SideBar extends React.Component<
    {
        classes: {},
        bundleInfo: {},
        onUpdate: () => any,
    }
> {
  
    render() {
        const { bundleInfo, classes, onUpdate } = this.props;
        const { metadata, editableMetadataFields=[], metadataType } = bundleInfo;
        const hasEditPermission = bundleInfo.permission > 1;

        const bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
        const bundleStateClass = 'bundle-state state-' + (bundleInfo.state || 'ready');
        const isRunBundle = bundleInfo.bundle_type === 'run';

        return (
            <div>
                { metadata.failure_message && <Typography variant="body1">{ metadata.failure_message }</Typography> }
                {/** ----------------------------------------------------------------------------------------------- */}
                <ConfigLabel label="Name" />
                <BundleEditableField
                    dataType={ metadataType.name }
                    fieldName="name"
                    uuid={ bundleInfo.uuid }
                    value={ metadata.name }
                    canEdit={ hasEditPermission && editableMetadataFields.includes("name") }
                    onChange={ (name) => onUpdate({ name }) }
                />
                {/** ----------------------------------------------------------------------------------------------- */}
                <ConfigLabel label="Description" />
                <BundleEditableField
                    dataType={ metadataType.description }
                    fieldName="description"
                    uuid={ bundleInfo.uuid }
                    value={ metadata.description }
                    canEdit={ hasEditPermission && editableMetadataFields.includes("description") }
                    onChange={ (description) => onUpdate({ description }) }
                />
                {/** ----------------------------------------------------------------------------------------------- */}
                { isRunBundle && <Dependency bundleInfo={ bundleInfo }/> }
                { metadata.data_size && <ConfigLabel label="Size" /> }
                { metadata.data_size && renderFormat(metadata.data_size, metadataType.data_size) }
                <ConfigLabel label="Created On" />
                {renderFormat(metadata.created, metadataType.created)}
                <ConfigLabel label="Owner" />
                { bundleInfo.owner.user_name }
                <ConfigLabel label="Attached to Worksheets" />
                {
                    bundleInfo.host_worksheets.map((worksheet) =>
                        <div
                            key={ worksheet.uuid }
                        >
                            <a
                                href={ `/worksheets/${ worksheet.uuid }`}
                                className={ classes.uuidLink }
                                target="_blank"
                            >
                                { worksheet.name }
                            </a>
                        </div>
                    )
                }
                {/** ----------------------------------------------------------------------------------------------- */}
                <div className={classes.spacer}/>
                <div>
                    <Typography variant="subtitle1">Permissions</Typography>
                    <PermissionDialog
                        uuid={ bundleInfo.uuid }
                        permission_spec={ bundleInfo.permission_spec }
                        group_permissions={ bundleInfo.group_permissions }
                        onChange={ this.props.onMetaDataChange }
                    />
                </div>
                <div>
                    <a
                        href={ `/bundles/${ bundleInfo.uuid }` }
                        className={ classes.uuidLink }
                        target="_blank"
                    >
                        More Information for Bundle { shorten_uuid(bundleInfo.uuid) }
                    </a>
                </div>
            </div>
        );
    }
}

const styles = (theme) => ({
    section: {
        marginTop: 8,
    },
    row: {
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'center',
    },
    uuidLink: {
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        }
    },
    spacer: {
        marginTop: theme.spacing.larger,
    },
});

export default withStyles(styles)(SideBar);
