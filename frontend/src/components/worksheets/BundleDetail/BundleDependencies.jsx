import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Tooltip from '@material-ui/core/Tooltip';

/**
 * This component renders a list of bundle dependency links.
 */
class BundleDependencies extends React.PureComponent {
    constructor(props) {
        super(props);
    }

    render() {
        const { bundle, classes } = this.props;
        const dependencies = bundle.dependencies.value;

        if (!dependencies.length) {
            return <div>None</div>;
        }

        return dependencies.map((dep) => {
            const uuid = dep.parent_uuid;
            const name = dep.parent_name;
            const alias = name === dep.child_path ? null : dep.child_path;
            const href = '/bundles/' + uuid;
            return (
                <div className={classes.depContainer} key={uuid}>
                    <div className={classes.truncate}>
                        <a className={classes.link} href={href} target='_blank'>
                            {name}
                        </a>
                    </div>
                    {alias && (
                        <Tooltip classes={{ tooltip: classes.tooltip }} title={`Alias: ${alias}`}>
                            <div className={`${classes.alias} ${classes.truncate}`}>as {alias}</div>
                        </Tooltip>
                    )}
                </div>
            );
        });
    }
}

const styles = (theme) => ({
    depContainer: {
        paddingTop: 5,
        paddingBottom: 5,
        borderBottom: `1px solid ${theme.color.grey.base}`,
        '&:first-of-type': {
            paddingTop: 0,
        },
        '&:last-of-type': {
            paddingBottom: 0,
            borderBottom: 'unset',
        },
    },
    truncate: {
        maxWidth: 168,
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
    },
    link: {
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        },
    },
    alias: {
        fontSize: 12,
        paddingLeft: 15,
        color: theme.color.grey.darkest,
        cursor: 'default',
    },
    tooltip: {
        fontSize: 14,
        padding: `${theme.spacing.large}px ${theme.spacing.larger}px`,
    },
});

export default withStyles(styles)(BundleDependencies);
