import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import Copy from './Copy';
import NewWindowLink from './NewWindowLink';

/**
 * This component renders text as a code snippet.
 *
 * If a copyMessage is provided, a copy icon will be rendered.
 * If an href is provided, a new-window icon will be rendered.
 */
class CodeSnippet extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { classes, code, copyMessage, expanded, href } = this.props;
        const maxHeight = expanded ? 'none' : 300;
        const marginBottom = this.props.noMargin ? 0 : 16;
        return (
            <Grid item xs={12}>
                <div className={classes.snippet} style={{ maxHeight, marginBottom }}>
                    <div>{code}</div>
                    {copyMessage && <Copy message={copyMessage} text={code} />}
                    {href && <NewWindowLink href={href} />}
                </div>
            </Grid>
        );
    }
}

const styles = (theme) => ({
    snippet: {
        display: 'flex',
        justifyContent: 'space-between',
        fontFamily: 'monospace',
        padding: 10,
        flexShrink: 1,
        overflow: 'auto',
        whiteSpace: 'pre-wrap',
        backgroundColor: theme.color.grey.lightest,
    },
});

export default withStyles(styles)(CodeSnippet);
