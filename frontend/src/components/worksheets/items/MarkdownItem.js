import * as React from 'react';
import $ from 'jquery';
import Immutable from 'seamless-immutable';
import { worksheetItemPropsChanged } from '../../../util/worksheet_utils';
import marked from 'marked';
import ReactDOM from 'react-dom';
import { withStyles } from '@material-ui/core/styles';
import IconButton from '@material-ui/core/IconButton';
import EditIcon from '@material-ui/icons/Edit';
import DeleteIcon from '@material-ui/icons/Delete';
import * as Mousetrap from '../../../util/ws_mousetrap_fork';
import TextEditorItem from './TextEditorItem';
import { createAlertText } from '../../../util/worksheet_utils';
import Tooltip from '@material-ui/core/Tooltip';
class MarkdownItem extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = Immutable({ showEdit: false, deleting: false });
        this.placeholderText = '@MATH@';
    }

    processMathJax = () => {
        window.MathJax &&
            window.MathJax.Hub.Queue(['Typeset', window.MathJax.Hub, ReactDOM.findDOMNode(this)]);
    };

    componentDidMount() {
        this.processMathJax();
    }

    componentDidUpdate() {
        this.processMathJax();
        if (this.props.focused) this.capture_keys();
    }
    shouldComponentUpdate(nextProps, nextState) {
        return (
            worksheetItemPropsChanged(this.props, nextProps) ||
            this.state.showEdit !== nextState.showEdit
        );
    }
    handleClick = () => {
        this.props.setFocus(this.props.focusIndex, 0);
    };

    processMarkdown = (text) => {
        var mathSegments = [];
        // 'we have $x^2$' => 'we have @MATH@'
        text = this.removeMathJax(text, mathSegments);
        // 'we have @ppp@' => '<p>we have @MATH@</p>'
        text = marked(text, { sanitize: true });
        // '<p>we have @ppp@</p>' => '<p>we have @x^2@</p>'
        text = this.restoreMathJax(text, mathSegments);
        return text;
    };

    toggleEdit = () => {
        this.setState({ showEdit: !this.state.showEdit });
    };

    capture_keys = () => {
        // Edit the markdown
        const { editPermission } = this.props;
        Mousetrap.bind(
            ['enter'],
            function(ev) {
                ev.preventDefault();
                if (editPermission && !this.props.item.error) {
                    this.toggleEdit();
                }
            }.bind(this),
        );

        // Delete the line
        Mousetrap.bind(
            ['backspace', 'del'],
            function(ev) {
                ev.preventDefault();
                if (!this.props.item.error && this.props.focused) {
                    if (editPermission) {
                        this.props.setDeleteItemCallback(this.deleteItem);
                    }
                }
            }.bind(this),
        );

        // unbind shortcuts that are active for table_block and worksheet_block
        Mousetrap.unbind('shift+enter');
        Mousetrap.unbind('a s');
        Mousetrap.unbind('x');
        Mousetrap.unbind('i');
    };

    handleDeleteClick = () => {
        this.props.setDeleteItemCallback(this.deleteItem);
    };

    deleteItem = () => {
        const { reloadWorksheet, item, worksheetUUID, setFocus, focused, focusIndex } = this.props;
        const url = `/rest/worksheets/${worksheetUUID}/add-items`;
        $.ajax({
            url,
            data: JSON.stringify({ ids: item.ids }),
            contentType: 'application/json',
            type: 'POST',
            success: (data, status, jqXHR) => {
                const textDeleted = true;
                const param = { textDeleted };
                this.setState({ deleting: false });
                reloadWorksheet(undefined, undefined, param);
                Mousetrap.unbind(['backspace', 'del']);
            },
            error: (jqHXR, status, error) => {
                this.setState({ deleting: false });
                alert(createAlertText(this.url, jqHXR.responseText));
            },
        });
    };

    render() {
        const { classes, item, editPermission } = this.props;
        var { showEdit } = this.state;
        var contents = item.text;
        if (item.error) {
            contents += ', please fix the line in source';
        }
        // Order is important!
        contents = this.processMarkdown(contents);

        // create a string of html for innerHTML rendering
        // more info about dangerouslySetInnerHTML
        // http://facebook.github.io/react/docs/special-non-dom-attributes.html
        // http://facebook.github.io/react/docs/tags-and-attributes.html#html-attributes
        var className = 'type-markup ' + (this.props.focused ? 'focused' : '');

        let after_sort_key = null;
        if (item.sort_keys && item.sort_keys.length > 0) {
            const { sort_keys, ids } = item;
            const keys = [];
            sort_keys.forEach((k, idx) => {
                const key = k || ids[idx];
                if (key !== null && key !== undefined) {
                    keys.push(key);
                }
            });
            if (keys.length > 0) {
                after_sort_key = Math.min(...keys);
            }
        }

        return showEdit ? (
            <TextEditorItem
                ids={item.ids}
                mode='edit'
                defaultValue={item.text}
                after_sort_key={after_sort_key}
                reloadWorksheet={this.props.reloadWorksheet}
                worksheetUUID={this.props.worksheetUUID}
                closeEditor={() => {
                    this.setState({ showEdit: false });
                }}
            />
        ) : (
            <div className={'ws-item ' + classes.textContainer} onClick={this.handleClick}>
                <div
                    className={`${className} ${classes.textRender}`}
                    dangerouslySetInnerHTML={{ __html: contents }}
                />
                {editPermission && !item.error && (
                    <div className={classes.buttonsPanel}>
                        <Tooltip title='Edit'>
                            <IconButton
                                onClick={this.toggleEdit}
                                classes={{ root: classes.iconButtonRoot }}
                            >
                                <EditIcon />
                            </IconButton>
                        </Tooltip>
                        &nbsp;&nbsp;
                        <Tooltip title='Delete'>
                            <IconButton
                                onClick={this.handleDeleteClick}
                                classes={{ root: classes.iconButtonRoot }}
                            >
                                <DeleteIcon />
                            </IconButton>
                        </Tooltip>
                    </div>
                )}
            </div>
        );
    }

    /// helper functions for making markdown and mathjax work together
    removeMathJax(text, mathSegments) {
        var curr = 0; // Current position
        // Replace math (e.g., $x^2$ or $$x^2$$) with placeholder so that it
        // doesn't interfere with Markdown.
        var newText = '';
        while (true) {
            // Figure out next block of math from current position.
            // Example:
            //   0123456 [indices]
            //   $$x^2$$ [text]
            //   start = 0, inStart = 2, inEnd = 5, end = 7
            var start = text.indexOf('$', curr);
            if (start === -1) break; // No more math blocks
            var inStart = text[start + 1] === '$' ? start + 2 : start + 1;
            var inEnd = text.indexOf('$', inStart);
            if (inEnd === -1) {
                // We've reached the end without closing
                console.error("Math '$' not matched", text);
                break;
            }
            var end = text[inEnd + 1] === '$' ? inEnd + 2 : inEnd + 1;

            var mathText = text.slice(start, end); // e.g., "$\sum_z p_\theta$"
            mathSegments.push(mathText);
            newText += text.slice(curr, start) + this.placeholderText;
            curr = end; // Look for the next occurrence of math
        }
        newText += text.slice(curr);
        return newText;
    }

    restoreMathJax(text, mathSegments) {
        // Restore the MathJax, replacing placeholders with the elements of mathSegments.
        var newText = '';
        var curr = 0;
        for (var i = 0; i < mathSegments.length; i++) {
            var start = text.indexOf(this.placeholderText, curr);
            if (start === -1) {
                console.error("Internal error: shouldn't happen");
                break;
            }
            newText += text.slice(curr, start) + mathSegments[i];
            curr = start + this.placeholderText.length; // Advance cursor
        }
        newText += text.slice(curr);
        return newText;
    }
}

const styles = (theme) => ({
    textContainer: {
        position: 'relative',
        '&:hover $buttonsPanel': {
            display: 'flex',
        },
        minHeight: 36,
        display: 'flex',
        alignItems: 'center',
    },
    buttonsPanel: {
        display: 'none',
        position: 'absolute',
        top: 0,
        right: 0,
    },
    iconButtonRoot: {
        backgroundColor: theme.color.grey.lighter,
    },
    textRender: {},
});

export default withStyles(styles)(MarkdownItem);
