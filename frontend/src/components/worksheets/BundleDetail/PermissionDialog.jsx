// @flow
import * as React from 'react';
import $ from 'jquery';
import Typography from '@material-ui/core/Typography';
import NativeSelect from '@material-ui/core/NativeSelect';
import IconButton from '@material-ui/core/IconButton';
import AddIcon from '@material-ui/icons/Add';
import Popper from '@material-ui/core/Popper';
import Paper from '@material-ui/core/Paper';
import Input from '@material-ui/core/Input';
import { withStyles } from '@material-ui/core/styles';
import { buildTerminalCommand } from '../../../util/worksheet_utils';
import { executeCommand } from '../../../util/cli_utils';

function parseGlsOutput(output) {
	const lines = output.split(/[\n]+/);
	// Remove empty lines.
	const records = lines.splice(2).filter(record => Boolean(record));
	const names = records.map(record => record.split(/[\s\t]+/)[0]);
	return names;
}

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
        this.nGroupName = null;
        this.state = {
            showAddSection: false,
            groupNames: [],
        };
        this.getGroups();
	}

	getGroups = () => {
		executeCommand('gls')
        .done((resp) => {
        	const groupNames = parseGlsOutput(resp.output);
        	this.setState({ groupNames });
        });
	}

	handlePermissionValueChange = (name, value) => {
		const { uuid } = this.props;

		executeCommand(buildTerminalCommand(['perm', uuid, name, value])).done(() => {
			this.props.onChange();
		});
	}

    handleAddPermission = (value) => {
        if (!this.nGroupName) {
            // Group name not inputted.
            return;
        }
        const { uuid } = this.props;

        executeCommand(buildTerminalCommand(['perm', uuid, this.nGroupName, value]))
        .done(() => {
            this.setState({ showAddSection: false });
            this.props.onChange();
        });
    }

	render() {
		const { classes, permission_spec, group_permissions } = this.props;
        const { showAddSection, groupNames } = this.state;
		const permissions = [
			{ group_name: 'you', permission_spec },
			...(group_permissions || [])
		];
		const assignedGroups = permissions.map(permission => permission.group_name);
		const unassignedGroups = groupNames.filter(groupName => !assignedGroups.includes(groupName));

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
				{ /** Adding permissions editor ==================================================================== */}
                {   (showAddSection && unassignedGroups.length)
                    ? <div className={ classes.row }>
                    	<NativeSelect
                            onChange={ (event) => { if (event.target.value) this.nGroupName = event.target.value; } }
                            input={
                                <Input
                                    className={ classes.textField }
                                />
                            }
                        >
                            {/* Set to 'none' = removing all permission */}
                            <option value="" disabled selected>Select a group</option>
                            {
                            	unassignedGroups.map(group => <option key={ group } value={ group }>{ group }</option>)
                            }
                        </NativeSelect>
                        <NativeSelect
                            defaultValue="none"
                            onChange={ (event) => this.handleAddPermission(
                                    event.target.value) }
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
                    </div>
                    : null
                }
                { Boolean(unassignedGroups.length) && <IconButton
	                    onClick={ () => {
	                        this.setState({ showAddSection: true });
	                    } }
	                >
	                    <AddIcon />
	                </IconButton>
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
		marginRight: theme.spacing.large,
        width: 60,
	},
});

export default withStyles(styles)(PermissionDialog);
