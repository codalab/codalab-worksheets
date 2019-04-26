// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core';

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
            <tr key={ dep.parent_uuid + i }>
                <td>{ dep.child_path }</td>
                <td>
                    &rarr; { dep.parent_name }(
                    <a href={ dep_bundle_url }>
                      { shorten_uuid(dep.parent_uuid) }
                    </a>)
                    { dep.parent_path ? '/' + dep.parent_path : '' }
                </td>
            </tr>
        );
    });

    return (
        <div>
            <h4>dependencies</h4>
            <table className='bundle-meta table'>
                <tbody>{ dependencies_table }</tbody>
            </table>
        </div>
    );
  }
}

// Main class definition.
class SideBar extends React.Component<
    {
      bundleInfo: {},
    }
> {
  
    render() {
        const { bundleInfo, classes } = this.props;
        const { metadata, editableMetadataFields=[], metadataType } = bundleInfo;
        const hasEditPermission = bundleInfo.permission > 1;

        const bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
        const bundleStateClass = 'bundle-state state-' + (bundleInfo.state || 'ready');
        const isRunBundle = bundleInfo.bundle_type === 'run';

        return (
            <Grid container spacing={8}>
                {
                    metadata.failure_message && <Grid item xs={12}>
                        <Typography variant="body1">
                            { metadata.failure_message }
                        </Typography>
                    </Grid>
                }
                <Grid item xs={ 12 }>
                    <Grid container spacing={4}>
                    {
                        ['name', 'description'].map((field) => 
                            <Grid item xs={12} key={ field }>
                                <Typography variant="body1">{ field }:</Typography>
                                <BundleEditableField
                                    dataType={ metadataType[field] }
                                    fieldName={ field }
                                    uuid={ bundleInfo.uuid }
                                    value={ metadata[field] }
                                    canEdit={ hasEditPermission && editableMetadataFields.includes(field) }
                                />
                            </Grid>
                        )
                    }
                    </Grid>
                </Grid>
                { isRunBundle && <Grid item xs={12}>
                        <Dependency
                            bundleInfo={ bundleInfo }
                        />
                    </Grid>
                }
                <Grid item xs={12}>
                    <Typography variant="body1">
                        <a
                            href={ `/bundles/${ bundleInfo.uuid }` }
                            className={ classes.uuidLink }
                        >
                            { shorten_uuid(bundleInfo.uuid) }
                        </a>
                        { metadata.data_size && <u>
                                ({ renderFormat(metadata.data_size, metadataType.data_size) })
                            </u>
                        }
                        <br />
                        Created on &nbsp;
                        <u>
                            ({ renderFormat(metadata.created, 'date') })
                        </u>
                        <br />
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
                    <PermissionDialog
                        permission_spec={ bundleInfo.permission_spec }
                        group_permissions={ bundleInfo.group_permissions }
                    />
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
        color: theme.color.primary.base,
        '&:hover': {
            color: theme.color.primary.dark,
        }
    },
});

export default withStyles(styles)(SideBar);
