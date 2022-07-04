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
        const states = getBundleStates(this.props.bundleType) || [];
        const title = states.map((state, i) => {
            const isLast = i == states.length - 1;
            const isCurrent = state.includes(this.props.bundleState);
            const border = isCurrent ? '1px solid white' : '';
            return (
                <div style={{ textAlign: 'center' }}>
                    <span style={{ border, borderRadius: '10px', padding: '1px 6px' }}>
                        {state}
                    </span>
                    {!isLast && <div>↓</div>}
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
