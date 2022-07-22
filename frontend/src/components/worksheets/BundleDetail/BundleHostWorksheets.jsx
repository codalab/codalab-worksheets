import React from 'react';
import { withStyles } from '@material-ui/core/styles';

/**
 * This component renders a list of host worksheet links.
 */
class BundleHostWorksheets extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const { bundle, classes } = this.props;
        const hostWorksheets = bundle.host_worksheets.value;

        if (!hostWorksheets.length) {
            return <div>None</div>;
        }
        return hostWorksheets.map((worksheet) => (
            <div key={worksheet.uuid}>
                <a
                    href={`/worksheets/${worksheet.uuid}`}
                    className={classes.uuidLink}
                    target='_blank'
                >
                    {worksheet.name}
                </a>
            </div>
        ));
    }
}

const styles = (theme) => ({
    uuidLink: {
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        },
    },
});

export default withStyles(styles)(BundleHostWorksheets);
