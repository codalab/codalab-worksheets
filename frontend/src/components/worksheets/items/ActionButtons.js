// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleFilled';
import UploadIcon from '@material-ui/icons/CloudUpload';
import TextIcon from '@material-ui/icons/FontDownload';

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
                className={classes.buttonsPanel}
            >
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add New Upload'
                    onClick={onShowNewUpload}
                    classes={{ root: classes.buttonRoot }}
                >
                    <UploadIcon className={classes.buttonIcon} />
                    Upload
                </Button>
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add New Run'
                    onClick={onShowNewRun}
                    classes={{ root: classes.buttonRoot }}
                >
                    <RunIcon className={classes.buttonIcon} />
                    Run
                </Button>
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add Text'
                    onClick={onShowNewText}
                    classes={{ root: classes.buttonRoot }}
                >
                    <TextIcon className={classes.buttonIcon} />
                    Text
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
