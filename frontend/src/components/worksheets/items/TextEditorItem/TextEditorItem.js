// @flow
import * as React from 'react';
import Button from '@material-ui/core/Button';
import InputBase from '@material-ui/core/InputBase';
import { withStyles } from '@material-ui/core/styles';
import { createAlertText } from '../../../../util/worksheet_utils';
import * as Mousetrap from '../../../../util/ws_mousetrap_fork';
import { addItems } from '../../../../util/apiWrapper';

/*
This component has 2 modes:
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

    updateText = (ev) => {
        this.text = ev.target.value;
    };

    saveText = () => {
        if (this.text === null) {
            // Nothing to save.
            this.props.closeEditor();
            return;
        }

        const {
            ids,
            mode,
            worksheetUUID,
            after_sort_key,
            reloadWorksheet,
            closeEditor,
        } = this.props;

        let url = `/rest/worksheets/${worksheetUUID}/add-items`;
        const items = this.text.split(/[\n]/);
        if (mode === 'create') {
            // Add an extra line to the beginning of new text items,
            // so they are separate from previous items.
            items.unshift('');
        }

        const data = { items };

        if (after_sort_key || after_sort_key === 0) {
            data['after_sort_key'] = after_sort_key;
        }

        if (mode === 'edit') {
            // Updating an existing item.
            data['ids'] = ids;
        }
        const callback = () => {
            const moveIndex = mode === 'create';
            const param = { moveIndex };
            closeEditor();
            reloadWorksheet(undefined, undefined, param);
        };
        const errorHandler = (error) => {
            alert(createAlertText(url, error));
        };
        addItems(worksheetUUID, data)
            .then(callback)
            .catch(errorHandler);
    };

    render() {
        const { classes, defaultValue } = this.props;
        Mousetrap.bindGlobal(['ctrl+enter'], () => {
            this.saveText();
            Mousetrap.unbindGlobal(['ctrl+enter']);
        });

        Mousetrap.bindGlobal(['esc'], () => {
            this.props.closeEditor();
            this.text = defaultValue;
            Mousetrap.unbindGlobal(['esc']);
        });

        return (
            <div className={classes.container}>
                <InputBase
                    defaultValue={defaultValue || ''}
                    className={classes.input}
                    onChange={this.updateText}
                    autoFocus={true}
                    multiline
                />
                <Button variant='text' color='primary' onClick={this.props.closeEditor}>
                    Cancel
                </Button>
                <Button variant='text' color='primary' onClick={this.saveText}>
                    Save
                </Button>
            </div>
        );
    }
}

const styles = (theme) => ({
    container: {
        width: '100%',
        display: 'flex',
        flexDirection: 'row',
        margin: '0px',
        minHeight: 100,
        border: `2px solid ${theme.color.primary.base}`,
    },
    input: {
        flex: 1,
        marginLeft: 8,
        alignItems: 'flex-start',
    },
});

export default withStyles(styles)(TextEditorItem);
