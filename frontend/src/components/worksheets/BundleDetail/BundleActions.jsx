// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { buildTerminalCommand } from '../../../util/worksheet_utils';
import { executeCommand } from '../../../util/cli_utils';

class BundleActions extends React.Component<
	{
		bundleInfo: {},
		onComplete: () => any,
	}
> {

	static defaultProps = {
		onComplete: () => undefined,
	};

	rerun = () => {
		const { bundleInfo } = this.props;
		const run = {};
		run.command = bundleInfo.command;
		const dependencies = [];
		bundleInfo.dependencies.forEach((dep) => {
			dependencies.push({
				target: { name: dep.parent_name, uuid: dep.parent_uuid, path: dep.parent_path },
				alias: dep.child_path,
			});
		});
		run.dependencies = dependencies;
		this.props.rerunItem(run);
	}

	kill = () => {
		const { bundleInfo } = this.props;
		executeCommand(buildTerminalCommand(['kill', bundleInfo.uuid])).done(() => {
			this.props.onComplete();
		});
	}

	componentDidUpdate = () => {
		const { showNewRerun } = this.props;
		if (showNewRerun) {
			this.rerun();
		}
	}

	render() {
		const { bundleInfo, editPermission } = this.props;
		const bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
		const isRunBundle = bundleInfo.bundle_type === 'run';

		return (
			isRunBundle
			? <div style={ { display: 'flex', flexDirection: 'row', alignItems: 'center' } }>
	            <Button variant='text' color='primary'
					onClick={ this.kill }
					disable={!editPermission}
	            >
	            	Kill
	            </Button>
	            <Button variant='contained' color='primary'
					onClick={ this.rerun }
					disable={!editPermission}
	            >
	            	Edit and Rerun
	            </Button>
	        </div>
	        : <Button
	        	variant='contained'
	        	color='primary'
	        	onClick={ () => { window.location.href = bundleDownloadUrl; } }
	        >
	        	Download
	        </Button>
        );
	}
}

export default BundleActions;
