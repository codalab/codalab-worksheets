import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import OpenInNewIcon from '@material-ui/icons/OpenInNew';

/**
 * This component renders a new-window icon that, when clicked, will open the
 * given href in a new window.
 */
class NewWindowLink extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { classes, href, style } = this.props;

        if (!href) {
            return null;
        }
        return (
            <a
                className={classes.link}
                style={style}
                href={href}
                target='_blank'
                rel='noopener noreferrer'
            >
                <OpenInNewIcon fontSize='inherit' />
            </a>
        );
    }
}

const styles = (theme) => ({
    link: {
        fontSize: 14,
        color: theme.color.grey.darker,
        '&:hover': {
            color: theme.color.grey.darker, // override defaults
        },
        '&:focus': {
            color: theme.color.grey.darker, // override defaults
        },
    },
});

export default withStyles(styles)(NewWindowLink);
