import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import CircularProgress from '@material-ui/core/CircularProgress';

class Loading extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { classes, style } = this.props;
        return (
            <div className={classes.container} style={style}>
                <CircularProgress color='inherit' size={14} />
            </div>
        );
    }
}

const styles = (theme) => ({
    container: {
        width: '100%',
        display: 'flex',
        justifyContent: 'center',
        color: theme.color.primary.base,
    },
});

export default withStyles(styles)(Loading);
