import React from 'react';
import { withStyles } from '@material-ui/core/styles';

/**
 * This component renders a simple link out to a bungle view page.
 */
class BundlePageLink extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { classes, uuid } = this.props;
        return (
            <div className={classes.linkContainer}>
                <a href={`/bundles/${uuid}`} className={classes.link} target='_blank'>
                    Bundle Page
                </a>
            </div>
        );
    }
}

const styles = (theme) => ({
    linkContainer: {
        marginBottom: 15,
    },
    link: {
        fontSize: 14,
        fontWeight: 500,
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        },
    },
});

export default withStyles(styles)(BundlePageLink);
