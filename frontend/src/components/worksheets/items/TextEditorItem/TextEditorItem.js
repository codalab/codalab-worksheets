// @flow
import * as React from 'react';
import $ from 'jquery';
import Paper from '@material-ui/core/Paper';
import Button from '@material-ui/core/Button';
import InputBase from '@material-ui/core/InputBase';
import { withStyles } from '@material-ui/core/styles';
import { createAlertText } from '../../../../util/worksheet_utils';


/*
This component has to mode:
	1. edit: to update an existing markdown item.
	2. create: to create a new markdown item.
Special Note:
	When the user is creating a markdown item immediately adjacent to
	an existing markdown item, it's treated as an edit 'neath the hood.
*/
class TextEditorItem extends React.Component<
	{
		/*
		Only used in 'edit' mode, to update an item with this
		specific id.
		*/
		id: number,
		/* Can be either 'edit' or 'create' */
		mode: string,
		defaultValue: string,
		/*
		When:
		showDefault = 0, we show the defaultValue,
		showDefult = -1, we don't show defaultValue, but prepend it to our edit
		showDefult = 1, we don't show defaultValue, but append it to our edit
		*/
		showDefault: number,
		worksheetUUID: string,
		after_sort_key: number,
		reloadWorksheet: () => any,
		closeEditor: () => any,
	}
>{

	static defaultProps = {
		defaultValue: '',
		showDefault: 0,
	}

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

		const {
			id,
			mode,
			showDefault,
			defaultValue,
			worksheetUUID,
			after_sort_key,
			reloadWorksheet,
			closeEditor,
		} = this.props;

		let url = `/rest/worksheets/${ worksheetUUID }/add-markup`;
		let nText = this.text;
		
		if (after_sort_key) {
			url += `?after_sort_key=${ after_sort_key }`;
		}

		if (mode === 'edit') {
			// Updating an existing item.
			url = `/rest/worksheets/${ worksheetUUID }/update-markup?id=${ id }`;
			if (showDefault === 1) {
				nText = `${ defaultValue }\n${ nText }`;
			} else if (showDefault === -1) {
				nText += `\n${ defaultValue }`;
			}
		}

		console.log('sending ===>', url, mode);

		$.ajax({
            url,
            data: nText,
            contentType: 'text/plain',
            type: 'POST',
            success: (data, status, jqXHR) => {
                reloadWorksheet();
                closeEditor();
            },
            error: (jqHXR, status, error) => {
                alert(createAlertText(this.url, jqHXR.responseText));
            },
        });
	}

	render() {
		const { classes, defaultValue, showDefault } = this.props;

		return (
			<Paper className={ classes.container }>
				<InputBase
					defaultValue={ showDefault === 0 ? defaultValue : '' }
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
			</Paper>
		);
	}
}

const styles = (theme) => ({
	container: {
		width: '100%',
		display: 'flex',
		flexDirection: 'row',
		margin: '8px 0px',
	},
	input: {
		flex: 1,
		marginLeft: 8,
	},

});

export default withStyles(styles)(TextEditorItem);
