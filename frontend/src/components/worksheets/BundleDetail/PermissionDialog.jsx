// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import NativeSelect from '@material-ui/core/NativeSelect';
import IconButton from '@material-ui/core/IconButton';
import ShareIcon from '@material-ui/icons/Share';
import Popper from '@material-ui/core/Popper';
import Paper from '@material-ui/core/Paper';
import Input from '@material-ui/core/Input';
import { withStyles } from '@material-ui/core';

class PermissionDialog extends React.Component<
    {
      /* self permission */
      permission_spec: string,
      group_permissions: Array<{group_name: string, permission_spec: string}>,
      classes: {},
    }
>{

  constructor(props) {
    super(props);
    this.anchorEl = null;
  }

  render() {
    const { classes, permission_spec, group_permissions } = this.props;
    const permissions = [
      { group_name: 'you', permission_spec },
      ...group_permissions
    ];

    return (
      <div>
        {
          permissions.map((entry, idx) => <div
            key={ idx }
            className={ classes.row }
          >
            <Typography variant="body1" className={ classes.textIsolate } >
              { `${ entry.group_name }: ` }
            </Typography>
            <NativeSelect
              defaultValue={ entry.permission_spec }
              onChange={ (event) => this.handlePermissionValueChange(
                  entry.group_name, event.target.value) }
              input={
                <Input
                  className={ classes.textField }
                />
              }
            >
              {/* Set to 'none' = removing all permission */}
              <option value="none">none</option>
              <option value="read">read</option>
              <option value="all">all</option>
            </NativeSelect>
          </div>)
        }
      </div>
    );
  }
}

const styles = (theme) => ({
  row: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
  },
  textField: {
    marginTop: 0,
    marginBottom: 0,
    width: 120,
  },
  textIsolate: {
    marginLeft: theme.spacing.large,
    marginRight: theme.spacing.large,
  },
});

export default withStyles(styles)(PermissionDialog);
