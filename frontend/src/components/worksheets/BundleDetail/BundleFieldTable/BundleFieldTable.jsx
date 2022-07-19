import React from 'react';
import { withStyles } from '@material-ui/core/styles';

/**
 * This component is a generic table that has been optimized to render
 * BundleFieldRow children components.
 */
class BundleFieldTable extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { children, classes } = this.props;
        return <table className={classes.table}>{children}</table>;
    }
}

const styles = (theme) => ({
    table: {
        tableLayout: 'fixed',
        width: '100%',
        backgroundColor: theme.color.grey.lighter,
    },
});

export default withStyles(styles)(BundleFieldTable);
