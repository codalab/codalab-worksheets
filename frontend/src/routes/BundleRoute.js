import * as React from 'react';
import { withStyles } from '@material-ui/core';
import BundleDetail from '../components/worksheets/BundleDetail';

/**
 * This page-level component renders info about a single bundle.
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
                    fullMinHeight
                />
            </div>
        );
    }
}

const headerHeight = '58px';
const footerHeight = '25px';

const styles = () => ({
    bundleContainer: {
        // We create our own content viewport to eliminate native auto-scrolling.
        // Context: https://github.com/codalab/codalab-worksheets/issues/4204
        height: `calc(100vh - ${headerHeight} - ${footerHeight})`,
        display: 'flex',
        overflowY: 'scroll',
        overflowX: 'hidden',
    },
});

export default withStyles(styles)(BundleRoute);
