import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Button from '@material-ui/core/Button';

class InsertButtons extends React.Component{

    render() {
        const { classes, clickUpload, clickNewRun } = this.props;
        return (
            <div
                onMouseMove={ (ev) => { ev.stopPropagation(); } }
                className={ classes.buttonsPanel }
            >
                <Button
                    size="small"
                    variant="outlined"
                    color="primary"
                    onClick={ clickNewRun }
                    classes={ { root: classes.buttonRoot } }
                >
                    New Run
                </Button>
                &nbsp;&nbsp;
                <Button
                    size="small"
                    variant="outlined" 
                    color="primary"
                    onClick={ clickUpload }
                    classes={ { root: classes.buttonRoot } }
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
    buttonRoot: {
        backgroundColor: theme.color.grey.light,
    },
});

export default withStyles(styles)(InsertButtons);
