import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import HelpOutlineOutlinedIcon from '@material-ui/icons/HelpOutlineOutlined';
import { DOCS } from '../../constants';
import { getBundleStates } from './utils/';

class BundleStateTooltip extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const states = getBundleStates(this.props.bundleType);
        const title = states.map((state, i) => {
            const step = state == 'worker_offline' ? 'Offline State' : `State: ${i}`;
            return (
                <div>
                    {step}: {state}
                </div>
            );
        });

        return (
            <Tooltip title={title}>
                <IconButton
                    href={DOCS.features.bundles.states}
                    target='_blank'
                    rel='noopener noreferrer'
                    style={{
                        padding: 4,
                        fontSize: 13,
                        ...this.props.style,
                    }}
                >
                    <HelpOutlineOutlinedIcon fontSize='inherit' />
                </IconButton>
            </Tooltip>
        );
    }
}

export default BundleStateTooltip;
