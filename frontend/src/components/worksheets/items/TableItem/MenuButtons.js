import * as React from 'react';
import $ from 'jquery';
import { withStyles } from '@material-ui/core';
import IconButton from '@material-ui/core/IconButton';
import DeleteIcon from '@material-ui/icons/Delete';
import MoreIcon from '@material-ui/icons/MoreVert';
import { buildTerminalCommand } from '../../../../util/worksheet_utils';

class MenuButtons extends React.Component {
	
	showMore = (ev) => {
        ev.stopPropagation();
    };

    deleteItem = (ev) => {
        ev.stopPropagation();
        const { uuid } = this.props.bundleInfo;
        $('#command_line')
            .terminal()
            .exec(buildTerminalCommand(['rm', uuid]));
    };

	render() {
		const { rowcenter, classes } = this.props;

		return (
            <div
                className={ classes.rightButtonStripe }
                style={ {
                    top: rowcenter + 56,
                } }
            >
                <IconButton
                    onClick={ this.showMore }
                >
                    <MoreIcon />
                </IconButton>
                &nbsp;&nbsp;
                <IconButton
                    onClick={ this.deleteItem }
                >
                    <DeleteIcon />
                </IconButton>
            </div>
        );
	}
}

const styles = (theme) => ({
	rightButtonStripe: {
        display: 'flex',
        flexDirection: 'row',
        position: 'absolute',
        justifyContent: 'center',
        left: '100%',
        transform: 'translateY(-50%) translateX(-100%)',
    },
});

export default withStyles(styles)(MenuButtons);

