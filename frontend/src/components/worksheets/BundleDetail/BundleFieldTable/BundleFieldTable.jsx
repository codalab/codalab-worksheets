import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';

/**
 * This component is a generic table that has been optimized to render
 * BundleFieldRow children components.
 */
class BundleFieldTable extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { children, classes, title } = this.props;
        return (
            <>
                {title && (
                    <Typography classes={{ root: classes.title }} variant='subtitle1'>
                        {title}
                    </Typography>
                )}
                <table className={classes.table}>{children}</table>
            </>
        );
    }
}

const styles = (theme) => ({
    title: {
        marginTop: 10,
        marginBottom: 10,
        borderBottom: `1px solid ${theme.color.grey.base}`,
    },
    table: {
        tableLayout: 'fixed',
        width: '100%',
        backgroundColor: theme.color.grey.lighter,
    },
});

export default withStyles(styles)(BundleFieldTable);
