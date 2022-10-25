import * as React from 'react';
import { withStyles } from '@material-ui/core';
import Link from '@material-ui/core/Link';

class DownloadLink extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { classes, href } = this.props;
        return (
            <Link classes={{ root: classes.downloadLink }} href={href}>
                <span className='glyphicon glyphicon-download-alt' />
            </Link>
        );
    }
}

const styles = (theme) => ({
    downloadLink: {
        display: 'inline-block',
        padding: '8px 11px',
        fontSize: 14,
        border: '1px solid rgba(49, 131, 200, 0.5)',
        borderRadius: '4px',
        color: theme.color.primary.base,
        transition:
            'background-color 250ms cubic-bezier(0.4, 0, 0.2, 1) 0ms,box-shadow 250ms cubic-bezier(0.4, 0, 0.2, 1) 0ms,border 250ms cubic-bezier(0.4, 0, 0.2, 1) 0ms', // mimic MUI button transition
        '&:hover': {
            color: theme.color.primary.base, // override defaults
            border: `1px solid ${theme.color.primary.base}`,
            backgroundColor: 'rgba(49, 131, 200, 0.08)',
            textDecoration: 'none',
        },
        '&:focus': {
            color: theme.color.primary.base, // override defaults
        },
    },
});

export default withStyles(styles)(DownloadLink);
