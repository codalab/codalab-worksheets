// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import Button from '@material-ui/core/Button';

class ConfigurationPanel extends React.Component<{
    /** CSS-in-JS styling object. */
    classes: {},

    /** React components within opening & closing tags. */
    children: React.Node,

    /** React components on in sidebar. */
    sidebar?: React.Node,

    /** React components for bottom buttons. */
    buttons?: React.Node,
}> {

    /**
     * Renderer.
     */
    render() {
        const { classes, children, sidebar, buttons } = this.props;
        return (
            <Grid container direction='row' className={classes.container}>
                {/* Column 1: Main content area ================================================ */}
                <Grid item xs={12} sm={sidebar ? 8 : 12}
                      container direction='column' justify='space-between'
                      className={classes.content}>
                    <Grid item container direction='column'>
                        { children }
                    </Grid>
                    { !buttons ? null : (
                        <Grid item container className={classes.buttons} justify='flex-end'>
                            { buttons }
                        </Grid>
                    )}
                </Grid>
                {/* Column 2: Sidebar ========================================================== */}
                { !sidebar ? null : (
                    <Grid item xs={12} sm={4}
                          container direction='column'
                          className={classes.sidebar}>
                        { sidebar }
                    </Grid>
                )}
            </Grid>
        );
    }
}

// To inject styles into component
// -------------------------------

/** CSS-in-JS styling function. */
const styles = (theme) => ({
    container: {
        height: '100%',
        flexGrow: 1,
    },
    content: {
        backgroundColor: 'white',
        padding: theme.spacing.larger,
    },
    sidebar: {
        backgroundColor: theme.color.grey.light,
        padding: theme.spacing.larger,
    },
    buttons: {
        '& button': {
            marginLeft: theme.spacing.larger,
        },
        paddingBottom: theme.spacing.larger,
    }
});

export default withStyles(styles)(ConfigurationPanel);
