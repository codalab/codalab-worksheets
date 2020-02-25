// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import ChipInput from 'material-ui-chip-input';


class ConfigChipInput extends React.Component<{
    values: string[],
    onValueAdd: (string) => void,
    onValueDelete: (string, number) => void,
}> {
    render() {
        const { classes, values, onValueAdd, onValueDelete } = this.props;
        return (
            <ChipInput
                value={values}
                fullWidth
                onAdd={(value) => onValueAdd(value)}
                onDelete={(value, idx) => onValueDelete(value, idx)}
            />
        );
    }
}


// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({

});

export default withStyles(styles)(ConfigChipInput);
