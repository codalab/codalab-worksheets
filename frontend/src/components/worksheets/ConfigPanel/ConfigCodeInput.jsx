// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import TextField from '@material-ui/core/TextField';


class ConfigCodeInput extends React.Component<{
    placeholder?: string,
    multiline?: boolean,
    maxRows?: number,
    value: string,
    onValueChange?: (string) => void,
}> {
    render() {
        const { classes, placeholder, multiline, maxRows, value, onValueChange } = this.props;
        return (
            <TextField
                value={value}
                onChange={(e) => onValueChange(e.target.value)}
                placeholder={placeholder}
                multiline={multiline}
                margin="dense"
                variant="filled"
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
    inputRoot: {
        paddingTop: 0,
        paddingBottom: 0,
        fontFamily: theme.typography.fontFamilyMonospace,
        fontWeight: 500,
        fontSize: 16,
    },
    inputNative: {
        fontFamily: theme.typography.fontFamilyMonospace,
        fontWeight: 500,
        fontSize: 16,
        paddingTop: theme.spacing.large,
        paddingBottom: theme.spacing.large,
    },
});

export default withStyles(styles)(ConfigCodeInput);
