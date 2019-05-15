// @flow
import * as React from 'react';
import classNames from 'classnames';

import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';

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
                <Grid item xs={12} md={sidebar ? 8 : 12}
                      container direction='column' justify='space-between' wrap='nowrap'
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
                    <Grid item xs={12} md={4}
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
        flexGrow: 1,
        height: '100%',
    },
    content: {
        backgroundColor: 'white',
        padding: theme.spacing.larger,
        maxHeight: '100%',
        overflow: 'auto',
    },
    sidebar: {
        backgroundColor: theme.color.grey.light,
        padding: theme.spacing.larger,
        maxHeight: '100%',
        overflow: 'auto',
    },
    buttons: {
        '& button': {
            marginLeft: theme.spacing.larger,
        },
        paddingBottom: theme.spacing.larger,
    }
});

export default withStyles(styles)(ConfigurationPanel);
