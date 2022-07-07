import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Tooltip from '@material-ui/core/Tooltip';
import HelpIcon from '@material-ui/icons/Help';

class BundleFieldRow extends React.Component {
    render() {
        const { classes, field, value, description } = this.props;
        return (
            <tr>
                <td style={{ fontWeight: 500 }} className={classes.tableField}>
                    {field}
                    <Tooltip title={description}>
                        <span className={classes.tooltipIcon}>
                            <HelpIcon fontSize='inherit' style={{ verticalAlign: 'text-top' }} />
                        </span>
                    </Tooltip>
                </td>
                <td className={classes.tableValues}>{value}</td>
            </tr>
        );
    }
}

const styles = (theme) => ({
    tooltipIcon: {
        color: theme.color.grey.base,
        paddingLeft: theme.spacing.unit,
        paddingRight: theme.spacing.unit,
        fontSize: '11px',
    },
    tableField: {
        width: '150px',
        verticalAlign: 'top',
        paddingLeft: 15,
        paddingTop: 5,
        paddingBottom: 5,
    },
    tableValues: {
        overflowWrap: 'anywhere',
        verticalAlign: 'top',
        paddingRight: 15,
        paddingTop: 5,
        paddingBottom: 5,
    },
});

export default withStyles(styles)(BundleFieldRow);
