// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';
import AddIcon from '@material-ui/icons/PlayCircleFilled';

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
        const { classes, showNewUpload, showNewRun, showNewText } = this.props;
        return (
            <div onMouseMove={ (ev) => { ev.stopPropagation(); } }
                 className={ classes.buttonsPanel }
            >
                <Button
                    variant="outlined"
                    size="small"
                    color="primary"
                    aria-label="Upload"
                    onClick={ showNewUpload }
                    classes={ { root: classes.buttonRoot } }
                >
                    <AddIcon className={classes.buttonIcon} />
                    Upload
                </Button>
                <Button
                    variant="outlined"
                    size="small"
                    color="primary"
                    aria-label="Add New Run"
                    onClick={ showNewRun }
                    classes={ { root: classes.buttonRoot } }
                >
                    <AddIcon className={classes.buttonIcon} />
                    Run
                </Button>
                <Button
                    variant="outlined"
                    size="small"
                    color="primary"
                    aria-label="Add Text"
                    onClick={ showNewText }
                    classes={ { root: classes.buttonRoot } }
                >
                    <AddIcon className={classes.buttonIcon} />
                    Text
                </Button>
            </div>
        );
    }
}

class ItemWrapper extends React.Component {

	state = {
        showNewUpload: 0,
        showNewRun: 0,
        showNewText: 0,
        showInsertButtons: 0,
    }

	showButtons = (ev) => {
        const row = ev.currentTarget;
        const {
            top,
            height,
        } = row.getBoundingClientRect();
        const { clientY } = ev;
        const onTop = (clientY >= top
                && clientY <= top + 8);
        const onBotttom = (clientY >= top + height - 8
                && clientY <= top + height);
        if (onTop) {
            this.setState({
                showInsertButtons: -1,
            });
        }
        if (onBotttom) {
            this.setState({
                showInsertButtons: 1,
            });
        }
    }

	render() {
		const { children, classes, bundleInfo={}, worksheetUUID, reloadWorksheet } = this.props;
		const { showInsertButtons, showNewUpload, showNewRun, showNewText } = this.state;

		return (
			<div
				className={ classes.container }
				onMouseMove={ this.showButtons }
				onMouseLeave={ () => {
	                this.setState({
	                    showInsertButtons: 0,
	                });
	            } }
			>
				{
					(showInsertButtons === -1) && <InsertButtons
						classes={ classes }
						showNewUpload={ () => { this.setState({ showNewUpload: -1 }); } }
						showNewRun={ () => { this.setState({ showNewRun: -1 }); } }
						showNewText={ () => { this.setState({ showNewText: -1 }); } }
					/>
				}
				{
	                (showNewUpload === -1) &&
                        <NewUpload
                            after_sort_key={ bundleInfo.sort_key }
                            worksheetUUID={ worksheetUUID }
                            reloadWorksheet={ reloadWorksheet }
                            onClose={ () => this.setState({ showNewUpload: 0 }) }
                        />
		            }
	            {
	                (showNewRun === -1) &&
                        <NewRun
                            ws={this.props.ws}
                            onSubmit={() => this.setState({ showNewRun: 0 })}
                            after_sort_key={ bundleInfo.sort_key }
                        />
	            }
	            {
	                (showNewText === -1) &&
                        <TextEditorItem
                            after_sort_key={ bundleInfo.sort_key }
                            worksheetUUID={ worksheetUUID }
                            reloadWorksheet={ reloadWorksheet }
                        />
	            }
	            <div className={ classes.main }>
					{ children }
				</div>
				{
	                (showNewUpload === 1) &&
                        <NewUpload
                            after_sort_key={ bundleInfo.sort_key }
                            worksheetUUID={ worksheetUUID }
                            reloadWorksheet={ reloadWorksheet }
                            onClose={ () => this.setState({ showNewUpload: 0 }) }
                        />
		            }
	            {
	                (showNewRun === 1) &&
                        <NewRun
                            ws={this.props.ws}
                            onSubmit={() => this.setState({ showNewRun: 0 })}
                            after_sort_key={ bundleInfo.sort_key }
                        />
	            }
	            {
	                (showNewText === 1) &&
                        <TextEditorItem
                            after_sort_key={ bundleInfo.sort_key }
                            worksheetUUID={ worksheetUUID }
                            reloadWorksheet={ reloadWorksheet }
                        />
	            }
				{
					(showInsertButtons === 1) && <InsertButtons
						classes={ classes }
						showNewUpload={ () => { this.setState({ showNewUpload: 1 }); } }
						showNewRun={ () => { this.setState({ showNewRun: 1 }); } }
						showNewText={ () => { this.setState({ showNewText: 1 }); } }
					/>
				}
			</div>
		)
	}
}

const styles = (theme) => ({
	container: {
		position: 'relative',
	},
	main: {
		zIndex: 10,
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
        }
	}
});

export default withStyles(styles)(ItemWrapper);
