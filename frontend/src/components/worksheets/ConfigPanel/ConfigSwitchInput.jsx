// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import Switch from '@material-ui/core/Switch';


class ConfigSwitchInput extends React.Component<{
    value: string,
    onValueChange: (boolean) => void,
}> {
    render() {
        const { classes, value, onValueChange } = this.props;
        return (
            <Switch
                className={classes.switchInput}
                checked={value}
                onChange={(e) => onValueChange(e.target.checked)}
                color="primary"
            />
        );
    }
}


// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({
    switchInput: {
        marginLeft: -8,  // Remove default space on left
    },
});

export default withStyles(styles)(ConfigSwitchInput);
