// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core/styles';
import HelpOutlineOutlinedIcon from '@material-ui/icons/HelpOutlineOutlined';
import Tooltip from '@material-ui/core/Tooltip';
import Typography from '@material-ui/core/Typography';

class ConfigLabel extends React.Component<{
    label: string,
    tooltip?: string,
    inline?: boolean,
    optional?: boolean,
}> {
    render() {
        const { classes, label, tooltip, inline, optional } = this.props;
        const marginBottom = this.props.hasMargin ? 4 : 0;
        const contents = (
            <span className={classes.label} style={{ marginBottom }}>
                <Typography variant='subtitle2' inline>
                    {label}
                </Typography>
                {!optional ? null : (
                    <Typography variant='subtitle2' inline className={classes.optional}>
                        {'(optional)'}
                    </Typography>
                )}
                {!tooltip ? null : (
                    <Tooltip title={tooltip} classes={{ tooltip: classes.tooltipBox }}>
                        <span className={classes.tooltipIcon}>
                            <HelpOutlineOutlinedIcon
                                fontSize='inherit'
                                style={{ verticalAlign: 'sub' }}
                            />
                        </span>
                    </Tooltip>
                )}
            </span>
        );
        return inline === true ? contents : <div>{contents}</div>;
    }
}

// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({
    label: {
        display: 'inline-flex',
        verticalAlign: 'middle',
    },
    optional: {
        color: theme.color.grey.dark,
        paddingLeft: theme.spacing.small,
    },
    tooltipBox: {
        fontSize: 14,
    },
    tooltipIcon: {
        color: theme.color.grey.dark,
        paddingLeft: theme.spacing.unit,
        paddingRight: theme.spacing.unit,
        fontSize: 'small',
    },
});

export default withStyles(styles)(ConfigLabel);
