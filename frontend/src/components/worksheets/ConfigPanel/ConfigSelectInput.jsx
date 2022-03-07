// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';


class ConfigSelectInput extends React.Component<{
    placeholder?: string,
    multiline?: boolean,
    maxRows?: number,
    value: string,
    onValueChange: (string) => void,
}> {
    render() {
        const { classes, placeholder, multiline, maxRows, value, onValueChange } = this.props;
        return (
            <TextField
                className={classes.textInput}
                value={value}
                onChange={(e) => onValueChange(e.target.value)}
                placeholder={placeholder}
                multiline={multiline}
                margin="none"
                fullWidth
                InputProps={{
                    classes: {
                        root: classes.inputRoot,
                        input: classes.inputNative,
                    },
                    rowsMax: maxRows,
                }}
            />
        );
    }
}


// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({
    textInput: {
        paddingBottom: theme.spacing.large,
    },
    inputRoot: {
        paddingTop: 0,
        paddingBottom: 0,
    },
    inputNative: {
        paddingTop: 2,
        paddingBottom: 2,
    },
});

export default withStyles(styles)(ConfigSelectInput);
