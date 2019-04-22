// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import { withStyles } from '@material-ui/core';

import PermissionDialog from './PermissionDialog';
import Editable from '../Editable';
import { shorten_uuid } from '../../../util/worksheet_utils';

const metadataFields = [
  'name',
  'description',
  'created',
  'data_size',
];

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
    const { metadata, editableMetadataFields } = bundleInfo;

    const bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
    const bundleStateClass = 'bundle-state state-' + (bundleInfo.state || 'ready');

    return (<div className={ classes.container }>
      {/* Clickable UUID */}
      <a
        href={ `/bundles/${ bundleInfo.uuid }` }
        className={ classes.uuidLink }>
        { bundleInfo.uuid }
      </a>
      {/* General Metadata */}
      {
        metadata.failure_message && <Editable
          label="Failure Message"
          value={ metadata.failure_message }
        />
      }
      {
        metadataFields.map((field) => <Editable
          key={ field }
          label={ field }
          value={ metadata[field] }
          canEdit={ editableMetadataFields.includes(field) }
        />)
      }
      <div className={ classes.section }>
        <Dependency
          bundleInfo={ bundleInfo }
        />
      </div>
      <div
        className={ classes.section }
      >
        <Typography variant="body1">
          Owned by { bundleInfo.owner.user_name }, hosted within:
        </Typography>
        {
          bundleInfo.host_worksheets.map((worksheet) => <div
            key={ worksheet.uuid }
          >
            <a href={ `/worksheets/${ worksheet.uuid }` }>
              { worksheet.name }
            </a>
          </div>)
        }
      </div>
      <div className={ classes.section }>
        <PermissionDialog
          permission_spec={ bundleInfo.permission_spec }
          group_permissions={ bundleInfo.group_permissions }
        />
      </div>
    </div>);
  }
}

const styles = (theme) => ({
  container: {
    display: 'flex',
    flexDirection: 'column',
  },
  section: {
    marginTop: 8,
  },
  row: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
  },
  uuidLink: {
    textDecoration: 'none',
    '&:hover': {
      opacity: 0.5,
    }
  },
});

export default withStyles(styles)(SideBar);
