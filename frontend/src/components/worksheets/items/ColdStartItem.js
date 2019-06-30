// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import RunIcon from '@material-ui/icons/PlayCircleFilled';
import UploadIcon from '@material-ui/icons/CloudUpload';
import TextIcon from '@material-ui/icons/FontDownload';

import NewRun from '../NewRun';
import NewUpload from '../NewUpload';
import TextEditorItem from './TextEditorItem';

class InsertButtons extends React.Component<{
    classes: {},
    showNewUpload: () => void,
    showNewRun: () => void,
    showNewText: () => void,
}> {
    render() {
        const { classes, showNewUpload, showNewRun, showNewText, style } = this.props;
        return (
            <div className={classes.buttonsPanel} style={ style } >
                <Button
                    variant='outlined'
                    size='small'
                    color='primary'
                    aria-label='Add New Upload'
                    onClick={showNewUpload}
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
                    onClick={showNewRun}
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
                    onClick={showNewText}
                    classes={{ root: classes.buttonRoot }}
                >
                    <TextIcon className={classes.buttonIcon} />
                    Text
                </Button>
            </div>
        );
    }
}

class ColdStartItem extends React.Component {

	state = {
		showNewText: false,
		showNewRun: false,
		showNewUpload: false,
	}

	render() {
		const { classes, ws, worksheetUUID, after_sort_key, reloadWorksheet, buttonStyle } = this.props;
		const { showNewText, showNewRun, showNewUpload } = this.state;

		return (
			<div className={ classes.container }>
				<InsertButtons
		            classes={classes}
		            showNewUpload={() => {
		                this.setState({ showNewUpload: true });
		            }}
		            showNewRun={() => {
		                this.setState({ showNewRun: true });
		            }}
		            showNewText={() => {
		                this.setState({ showNewText: true });
		            }}
                    style={ buttonStyle }
		        />
				{showNewUpload && (
	                <NewUpload
                        after_sort_key={ after_sort_key }
	                    worksheetUUID={worksheetUUID}
	                    reloadWorksheet={reloadWorksheet}
	                    onClose={() => this.setState({ showNewUpload: false })}
	                />
	            )}
	            {showNewRun && (
	                <div className={classes.insertBox}>
                        <NewRun
                            after_sort_key={ after_sort_key }
                            ws={ws}
                            reloadWorksheet={reloadWorksheet}
                            onSubmit={() => this.setState({ showNewRun: false })}
                        />
	                </div>
	            )}
	            {showNewText && (
	                <TextEditorItem
	                    mode="create"
                        after_sort_key={ after_sort_key }
	                    worksheetUUID={worksheetUUID}
	                    reloadWorksheet={reloadWorksheet}
	                    closeEditor={() => {
	                        this.setState({ showNewText: false });
	                    }}
	                />
	            )}
        	</div>
		);
	}
}

const styles = (theme) => ({
	container: {
		width: '100%',
	},
	buttonsPanel: {
        display: 'flex',
        flexDirection: 'row',
        justifyContent: 'center',
        width: '100%',
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

export default withStyles(styles)(ColdStartItem);
