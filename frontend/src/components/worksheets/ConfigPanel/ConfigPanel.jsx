// @flow
import * as React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';

/** This reusable components displays a panel with a main content area, control buttons (optional), and sidebar
    (optional). Using this component ensures aesthetic consistency across parts of the UI. */
class ConfigPanel extends React.Component<{
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
        const { classes, children, sidebar, buttons, showBorder } = this.props;
        const borderClass = showBorder ? classes.border : '';
        return (
            <Grid container direction='row' className={`${classes.container} ${borderClass}`}>
                {/* Column 1: Main content area ================================================ */}
                <Grid
                    item
                    xs={12}
                    md={sidebar ? 9 : 12}
                    container
                    direction='column'
                    justify='space-between'
                    wrap='nowrap'
                    className={classes.content}
                >
                    <Grid item container direction='column'>
                        {children}
                    </Grid>
                    {!buttons ? null : (
                        <Grid item container className={classes.buttons} justify='flex-start'>
                            {buttons}
                        </Grid>
                    )}
                </Grid>
                {/* Column 2: Sidebar ========================================================== */}
                {!sidebar ? null : (
                    <Grid
                        item
                        xs={12}
                        md={3}
                        container
                        direction='column'
                        className={classes.sidebar}
                    >
                        {sidebar}
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
        flexWrap: 'nowrap',
        height: '100%',
        maxWidth: '100%',
    },
    content: {
        backgroundColor: 'white',
        padding: theme.spacing.larger,
        maxHeight: '100%',
        overflow: 'auto',
        flexGrow: 1,
        maxWidth: '90%',
    },
    sidebar: {
        backgroundColor: theme.color.grey.lighter,
        padding: theme.spacing.larger,
        maxHeight: '100%',
        overflow: 'auto',
        minWidth: '400px',
        flexGrow: 1,
    },
    buttons: {
        '& button': {
            marginLeft: theme.spacing.larger,
        },
        paddingBottom: theme.spacing.large,
        paddingTop: theme.spacing.larger,
        maxWidth: '90%',
    },
    border: {
        border: `2px solid ${theme.color.grey.light}`,
    },
});

export default withStyles(styles)(ConfigPanel);
