// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import HelpIcon from '@material-ui/icons/Help';
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
        const contents = (
            <span className={classes.label}>
                <Typography variant='subtitle2' inline>{label}</Typography>
                {!optional ? null : (
                    <Typography variant='subtitle2' inline className={classes.optional}>
                        {"(optional)"}
                    </Typography>
                )}
                {!tooltip ? null : (
                    <Tooltip title={tooltip}
                             classes={{ tooltip: classes.tooltipBox }}>
                            <span className={classes.tooltipIcon}>
                                <HelpIcon fontSize='inherit' style={{ verticalAlign: 'middle' }}/>
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
        padding: `${theme.spacing.large}px ${theme.spacing.larger}px`,
    },
    tooltipIcon: {
        color: theme.color.grey.base,
        paddingLeft: theme.spacing.unit,
        paddingRight: theme.spacing.unit,
        fontSize: 'small',
    },
});

export default withStyles(styles)(ConfigLabel);
