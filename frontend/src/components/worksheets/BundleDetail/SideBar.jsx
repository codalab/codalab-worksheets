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
import PermissionDialog from './PermissionDialog';
import { renderFormat, shorten_uuid } from '../../../util/worksheet_utils';

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
                        <a href={ dep_bundle_url }>
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
      bundleInfo: {},
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
            <Grid container spacing={16}>
                {
                    metadata.failure_message && <Grid item xs={12}>
                        <Typography variant="body1">
                            { metadata.failure_message }
                        </Typography>
                    </Grid>
                }
                <Grid item xs={ 12 }>
                    <Typography variant="body1">Name</Typography>
                    <BundleEditableField
                        dataType={ metadataType.name }
                        fieldName="name"
                        uuid={ bundleInfo.uuid }
                        value={ metadata.name }
                        canEdit={ hasEditPermission && editableMetadataFields.includes("name") }
                        onChange={ (name) => onUpdate({ name }) }
                    />
                </Grid>
                <Grid item xs={ 12 }>
                    <Typography variant="body1">Description</Typography>
                    <BundleEditableField
                        dataType={ metadataType.description }
                        fieldName="description"
                        uuid={ bundleInfo.uuid }
                        value={ metadata.description }
                        canEdit={ hasEditPermission && editableMetadataFields.includes("description") }
                        onChange={ (description) => onUpdate({ description }) }
                    />
                </Grid>
                { isRunBundle && <Grid item xs={12}>
                        <Dependency
                            bundleInfo={ bundleInfo }
                        />
                    </Grid>
                }
                <Grid item xs={12}>
                    <Typography variant="body1">
                        { metadata.data_size && <span>Size:<u>
                                &nbsp;({ renderFormat(metadata.data_size, metadataType.data_size) })
                            </u><br/></span>
                        }
                        Created on&nbsp;
                        <u>
                            ({ renderFormat(metadata.created, metadataType.created) })
                        </u>
                        <br/>
                        Owned by &nbsp;
                        <u>
                            { bundleInfo.owner.user_name }
                        </u>
                        <br />
                        Hosted within worksheets:
                    </Typography>
                    {
                        bundleInfo.host_worksheets.map((worksheet) =>
                            <div
                                key={ worksheet.uuid }
                            >
                                <a
                                    href={ `/worksheets/${ worksheet.uuid }`}
                                    className={ classes.uuidLink }
                                >
                                    { worksheet.name }
                                </a>
                            </div>
                        )
                    }
                </Grid>
                <Grid item xs={12}>
                    <Typography variant="body1"></Typography>
                    <PermissionDialog
                        uuid={ bundleInfo.uuid }
                        permission_spec={ bundleInfo.permission_spec }
                        group_permissions={ bundleInfo.group_permissions }
                    />
                </Grid>
                <Grid item xs={12}>
                    <a
                        href={ `/bundles/${ bundleInfo.uuid }` }
                        className={ classes.uuidLink }
                    >
                        More about bundle { shorten_uuid(bundleInfo.uuid) }
                    </a>
                </Grid>
            </Grid>);
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
});

export default withStyles(styles)(SideBar);
