import React from 'react';
import { withStyles } from '@material-ui/core';
import Button from '@material-ui/core/Button';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';
import KeyboardArrowUpIcon from '@material-ui/icons/KeyboardArrowUp';

/**
 * This button is optimized to be used alongside collapsable content. Example:
 *
 * label='Show More'
 * collapsedLabel='Show Less'
 *
 * When collapsed=true, the button will be labeled 'Show More'
 * When collapsed=false, the button will be labled 'Show Less'
 *
 * This component also applies an arrow icon that indicates the current
 * collapsed direction.
 */
class CollapseButton extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const {
            classes,
            containerClass,
            collapsed,
            collapseUp,
            collapsedLabel,
            label,
            onClick,
        } = this.props;
        const currentLabel = collapsed && collapsedLabel ? collapsedLabel : label;

        return (
            <div className={containerClass}>
                <Button
                    classes={{ focusVisible: classes.focusVisible }}
                    onClick={onClick}
                    size='small'
                    color='inherit'
                    disableRipple
                >
                    {currentLabel}
                    {collapsed ? (
                        collapseUp ? (
                            <KeyboardArrowUpIcon />
                        ) : (
                            <KeyboardArrowDownIcon />
                        )
                    ) : (
                        <ChevronRightIcon />
                    )}
                </Button>
            </div>
        );
    }
}

const styles = () => ({
    focusVisible: {
        backgroundColor: 'rgba(0, 0, 0, 0.08)',
    },
});

export default withStyles(styles)(CollapseButton);
