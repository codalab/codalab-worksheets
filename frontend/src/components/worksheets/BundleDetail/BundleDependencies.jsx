import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import { shorten_uuid } from '../../../util/worksheet_utils';
import BundleStateIndicator from './BundleStateIndicator';

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
            const name = dep.parent_name;
            const alias = name === dep.child_path ? null : dep.child_path;
            const state = dep.parent_state;
            const uuid = dep.parent_uuid;
            const href = '/bundles/' + uuid;
            return (
                <div className={classes.depContainer} key={uuid}>
                    <div className={classes.stateContainer}>
                        <BundleStateIndicator state={state} />
                    </div>
                    <div className={classes.nameContainer}>
                        {name} (
                        <a className={classes.uuidLink} href={href} target='_blank'>
                            {shorten_uuid(uuid)}
                        </a>
                        ){alias && <div className={classes.alias}>as {alias}</div>}
                    </div>
                </div>
            );
        });
    }
}

const styles = (theme) => ({
    depContainer: {
        display: 'flex',
        marginBottom: 4,
        fontSize: 14,
    },
    nameContainer: {
        paddingLeft: 5,
    },
    uuidLink: {
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        },
    },
    alias: {
        paddingLeft: 12,
        fontSize: 12,
        color: theme.color.grey.darkest,
    },
    tooltip: {
        fontSize: 14,
    },
});

export default withStyles(styles)(BundleDependencies);
