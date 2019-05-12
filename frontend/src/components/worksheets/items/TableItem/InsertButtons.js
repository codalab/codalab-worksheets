import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Button from '@material-ui/core/Button';

class InsertButtons extends React.Component{

    upload = (ev) => {
        ev.stopPropagation();
    };

    newRun = (ev) => {
        ev.stopPropagation();
    };

    render() {
        const { classes } = this.props;
        return (
            <div
                onMouseMove={ (ev) => { ev.stopPropagation(); } }
                className={ classes.buttonsPanel }
            >
                <Button
                    variant="outlined"
                    color="primary"
                    onClick={ this.newRun }
                >
                    New Run
                </Button>
                &nbsp;&nbsp;
                <Button
                    variant="outlined" 
                    color="primary"
                    onClick={ this.upload }
                >
                    Upload
                </Button>
            </div>
        );
    }
}

const styles = (theme) => ({
    buttonsPanel: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        width: '100%',
        transform: 'translateY(-50%)',
    },
});

export default withStyles(styles)(InsertButtons);
