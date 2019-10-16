// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleOutline';
import UploadIcon from '@material-ui/icons/CloudUploadOutlined';
import AddIcon from '@material-ui/icons/AddBoxOutlined';

class ActionButtons extends React.Component<{
    classes: {},
    onShowNewUpload: () => void,
    onShowNewRun: () => void,
    onShowNewText: () => void,
}> {
    render() {
        const { classes, onShowNewUpload, onShowNewRun, onShowNewText } = this.props;
        return (
            <div
                onMouseMove={(ev) => {
                    ev.stopPropagation();
                }}
                // className={classes.buttonsPanel}
            >
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Add Text'
                    onClick={onShowNewText}
                >
                    <AddIcon className={classes.buttonIcon} />
                    Cell
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Add New Upload'
                    onClick={onShowNewUpload}
                    // classes={{ root: classes.buttonRoot }}
                    // startIcon={<UploadIcon />}
                >
                    <UploadIcon className={classes.buttonIcon} />
                    Upload
                </Button>
                <Button
                    size='small'
                    color='inherit'
                    aria-label='Add New Run'
                    onClick={onShowNewRun}
                    // startIcon={<RunIcon />}
                >
                    <RunIcon className={classes.buttonIcon} />
                    Run
                </Button>
            </div>
        );
    }
}

const styles = (theme) => ({
    container: {
        position: 'relative',
        marginBottom: 20,
        zIndex: 5,
    },
    main: {
        zIndex: 10,
        border: `2px solid transparent`,
        '&:hover': {
            backgroundColor: theme.color.grey.lightest,
            border: `2px solid ${theme.color.grey.base}`,
        }
    },
    buttonsPanel: {
        display: 'flex',
        flexDirection: 'row',
        overflow: 'visible',
        justifyContent: 'center',
        width: '100%',
        height: 0,
        transform: 'translateY(-16px)',
        zIndex: 20,
    },
    buttonRoot: {
        width: 120,
        height: 32,
        marginLeft: theme.spacing.unit,
        marginRight: theme.spacing.unit,
        backgroundColor: '#f7f7f7',
        '&:hover': {
            backgroundColor: '#f7f7f7',
        },
    },
    buttonIcon: {
        marginRight: theme.spacing.large,
    },
    insertBox: {
        border: `2px solid ${theme.color.primary.base}`,
        margin: '32px 64px !important',
    },
});

export default withStyles(styles)(ActionButtons);
