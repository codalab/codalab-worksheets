import * as React from 'react';
import { withStyles } from '@material-ui/core';
import BundleDetail from '../components/worksheets/BundleDetail';

/**
 * This route page displays a bundle's metadata and contents.
 */
class BundleRoute extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { uuid } = this.props.match.params;
        const { classes } = this.props;
        return (
            <div className={classes.bundleContainer}>
                <BundleDetail
                    uuid={uuid}
                    onUpdate={() => {}}
                    contentExpanded
                    sidebarExpanded
                    hideBundlePageLink
                    showBorder
                />
            </div>
        );
    }
}

const styles = () => ({
    bundleContainer: {
        margin: '12px 10px',
        paddingBottom: 36,
    },
});

export default withStyles(styles)(BundleRoute);
