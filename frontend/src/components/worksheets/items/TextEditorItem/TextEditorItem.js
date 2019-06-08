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
class TextEditorItem extends React.Component<{
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
}> {
    static defaultProps = {
        defaultValue: '',
        showDefault: 0,
    };

    constructor(props) {
        super(props);
        this.text = null;
        this.keymap = {};
    }

    capture_keys = (ev) => {
        this.keymap[ev.keyCode] = ev.type == 'keydown';
        const pressed = [];
        Object.keys(this.keymap).forEach((key) => {
            if (this.keymap[key]) {
                pressed.push(key);
            }
        });
        if (pressed.includes('17') && (pressed.includes('13') || pressed.includes('83'))) {
            /* Pressed ctrl+enter or ctrl+s */
            this.saveText();
        }
    };

    updateText = (ev) => {
        this.text = ev.target.value;
    };

    saveText = () => {
        if (this.text === null) {
            // Nothing to save.
            return;
        }

        const {
            ids,
            mode,
            showDefault,
            defaultValue,
            worksheetUUID,
            after_sort_key,
            reloadWorksheet,
            closeEditor,
        } = this.props;

        let url = `/rest/worksheets/${worksheetUUID}/add-items`;
        const items = this.text.split(/[\n]+/);

        console.log('items ===>', items);

        const data = { items };

        if (after_sort_key) {
            data['after_sort_key'] = after_sort_key;
        }

        if (mode === 'edit') {
            // Updating an existing item.
            data['ids'] = ids;
        }

        $.ajax({
            url,
            data: JSON.stringify(data),
            contentType: 'application/json',
            type: 'POST',
            success: (data, status, jqXHR) => {
                reloadWorksheet();
                closeEditor();
            },
            error: (jqHXR, status, error) => {
                alert(createAlertText(this.url, jqHXR.responseText));
            },
        });
    };

    render() {
        const { classes, defaultValue, showDefault } = this.props;

        return (
            <Paper className={classes.container}>
                <InputBase
                    defaultValue={defaultValue || ''}
                    className={classes.input}
                    onChange={this.updateText}
                    onKeyUp={this.capture_keys}
                    onKeyDown={this.capture_keys}
                    autoFocus={true}
                    multiline
                />
                <Button variant='text' color='primary' onClick={this.saveText}>
                    Save
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
        minHeight: 100,
    },
    input: {
        flex: 1,
        marginLeft: 8,
        alignItems: 'flex-start',
    },
});

export default withStyles(styles)(TextEditorItem);
