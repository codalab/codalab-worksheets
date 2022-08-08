import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Tooltip from '@material-ui/core/Tooltip';
import { shorten_uuid } from '../../../util/worksheet_utils';
import BundleStateIndicator from './BundleStateIndicator';

/**
 * This component renders a list of bundle dependency links.
 */
class BundleDependencies extends React.PureComponent {
    constructor(props) {
        super(props);
    }

    getTitle(dep = {}) {
        const uuid = shorten_uuid(dep.parent_uuid);
        if (dep.parent_name === dep.child_path) {
            return `${dep.parent_name} (${uuid})`;
        }
        return `${dep.parent_name} (${uuid}) as ${dep.child_path}`;
    }

    render() {
        const { bundle, classes } = this.props;
        const dependencies = bundle.dependencies.value;

        if (!dependencies.length) {
            return <div>None</div>;
        }

        return dependencies.map((dep) => {
            const name = dep.child_path || dep.parent_name;
            const state = dep.parent_state;
            const uuid = dep.parent_uuid;
            const href = '/bundles/' + uuid;
            const title = this.getTitle(dep);
            return (
                <div className={classes.container} key={uuid}>
                    <div className={classes.truncate}>
                        <BundleStateIndicator state={state} />
                        <Tooltip title={title} classes={{ tooltip: classes.tooltip }}>
                            <a className={classes.link} href={href} target='_blank'>
                                {name}
                            </a>
                        </Tooltip>
                    </div>
                </div>
            );
        });
    }
}

const styles = (theme) => ({
    container: {
        marginBottom: 2,
    },
    truncate: {
        width: '100%',
        whiteSpace: 'nowrap',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
    },
    link: {
        paddingLeft: 5,
        fontSize: 14,
        color: theme.color.primary.dark,
        '&:hover': {
            color: theme.color.primary.base,
        },
    },
    tooltip: {
        fontSize: 14,
    },
});

export default withStyles(styles)(BundleDependencies);
