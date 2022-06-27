import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import InfoIcon from '@material-ui/icons/Info';
import { bundleStates, makeBundleStates, offlineState } from './bundleStates';
import { DOCS } from '../../constants';

class StateTooltip extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const type = this.props.bundleType;
        const states = type == 'make' ? makeBundleStates : bundleStates;
        const title = (
            <>
                {states.map((state, i) => (
                    <div>
                        State {i}: {state}
                    </div>
                ))}
                <div>Offline State: {offlineState}</div>
            </>
        );

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
                    <InfoIcon fontSize='inherit' />
                </IconButton>
            </Tooltip>
        );
    }
}

export default StateTooltip;
