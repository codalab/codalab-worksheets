// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';


class BundleActions extends React.Component {
	render(): React.node {
		const { bundleInfo } = this.props;
		const isRunBundle = bundleInfo.bundle_type === 'run';

		return (
			isRunBundle
			? <div style={ { display: 'flex', flexDirection: 'row', alignItems: 'center' } }>
	            <Button variant='text' color='primary'>Kill</Button>
	            <Button variant='contained' color='primary'>Rerun</Button>
	        </div>
	        : <Button variant='contained' color='primary'>Download</Button>
        );
	}
}

export default BundleActions;
