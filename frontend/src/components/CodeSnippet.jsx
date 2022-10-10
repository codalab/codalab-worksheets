import React from 'react';
import Ansi from 'ansi-to-react';
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
                    <div className={classes.codeContainer}>
                        <Ansi>{code}</Ansi>
                    </div>
                    {copyMessage && <Copy message={copyMessage} text={code} fill='white' />}
                    {href && <NewWindowLink style={{ color: 'white' }} href={href} />}
                </div>
            </Grid>
        );
    }
}

const styles = () => ({
    snippet: {
        display: 'flex',
        justifyContent: 'space-between',
        fontFamily: 'monospace',
        fontSize: 14,
        padding: 10,
        flexShrink: 1,
        whiteSpace: 'pre-wrap',
        borderRadius: '4px',
        backgroundColor: 'black',
        opacity: '0.80',
    },
    codeContainer: {
        paddingRight: 10,
        overflow: 'auto',
        overflowWrap: 'anywhere',
        color: 'white',
    },
});

export default withStyles(styles)(CodeSnippet);
