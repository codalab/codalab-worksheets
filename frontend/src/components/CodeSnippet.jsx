import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import Copy from './Copy';

/**
 * This component renders text as a code snippet.
 * If a copyMessage is provided, a copy icon will be rendered.
 *
 * This component accepts a `failure` variant which renders the code in red.
 */
class CodeSnippet extends React.Component {
    constructor(props) {
        super(props);
    }

    getVariantClass() {
        const { classes, variant } = this.props;
        if (variant === 'failure') {
            return classes.failure;
        }
        return '';
    }

    render() {
        const { classes, code, copyMessage } = this.props;
        return (
            <Grid item xs={12}>
                <div className={classes.snippet}>
                    <div className={this.getVariantClass()}>{code}</div>
                    <Copy message={copyMessage} text={code} />
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
        maxHeight: 300,
        padding: 10,
        flexShrink: 1,
        overflow: 'auto',
        whiteSpace: 'pre-wrap',
        backgroundColor: theme.color.grey.lightest,
        marginBottom: 16,
    },
    failure: {
        color: theme.color.red.base,
    },
});

export default withStyles(styles)(CodeSnippet);
