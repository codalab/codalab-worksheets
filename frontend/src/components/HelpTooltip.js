import React from 'react';
import Tooltip from '@material-ui/core/Tooltip';
import HelpOutlineOutlinedIcon from '@material-ui/icons/HelpOutlineOutlined';

class HelpTooltip extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        if (this.props.title) {
            return (
                <Tooltip title={this.props.title}>
                    <span className={this.props.className}>
                        <HelpOutlineOutlinedIcon fontSize='inherit' />
                    </span>
                </Tooltip>
            );
        }
        return null;
    }
}

export default HelpTooltip;
