import * as React from 'react';
import { withStyles } from '@material-ui/core';
import TableCellBase from '@material-ui/core/TableCell';

class TableCell extends React.Component {
    render() {
        const { classes, children, ...others } = this.props;
        return (
            <TableCellBase classes={{ root: classes.root }} {...others}>
                {children}
            </TableCellBase>
        );
    }
}

const styles = (theme) => ({
    root: {
        verticalAlign: 'middle !important',
    },
});

export default withStyles(styles)(TableCell);
