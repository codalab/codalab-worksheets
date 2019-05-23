// @flow
import * as React from 'react';
import $ from 'jquery';
import Paper from '@material-ui/core/Paper';
import Button from '@material-ui/core/Button';
import InputBase from '@material-ui/core/InputBase';
import { withStyles } from '@material-ui/core/styles';
import { createAlertText } from '../../../../util/worksheet_utils';

class TextEditorItem extends React.Component<
	{
		worksheetUUID: string,
		after_sort_key: number,
		reloadWorksheet: () => any,
	}
>{

	constructor(props) {
		super(props);
		this.text = null;
	}

	updateText = (ev) => {
		this.text = ev.target.value;
	}

	saveText = () => {
		if (this.text === null) {
			// Nothing to save.
			return;
		}

		const { worksheetUUID, after_sort_key, reloadWorksheet } = this.props;
		let url = `/rest/worksheets/${ worksheetUUID }/add-markup`;
		if (after_sort_key) {
			url += `/?after_sort_key=${ after_sort_key }`;
		}

		$.ajax({
            url,
            data: this.text,
            contentType: 'text/plain',
            type: 'POST',
            success: (data, status, jqXHR) => {
                reloadWorksheet();
            },
            error: (jqHXR, status, error) => {
                alert(createAlertText(this.url, jqHXR.responseText));
            },
        });
	}

	render() {
		const { classes } = this.props;

		return (<Paper className={ classes.container }>
			<InputBase
				className={ classes.input }
				onChange={ this.updateText }
				multiline
				rows="4"
			/>
			<Button
                variant='text'
                color='primary'
                onClick={ this.saveText }
            >
            	Done
            </Button>
		</Paper>);
	}
}

const styles = (theme) => ({
	container: {
		width: '100%',
		display: 'flex',
		flexDirection: 'row',
	},
	input: {
		flex: 1,
		marginLeft: 8,
	},

});

export default withStyles(styles)(TextEditorItem);
