import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import InfoIcon from '@material-ui/icons/Info';
import { DOCS } from '../constants';

class StateTooltip extends React.Component {
    constructor(props) {
        super(props);
    }

    render() {
        const title = (
            <div>
                <div>State 1: uploading</div>
                <div>State 2: created</div>
                <div>State 3: staged</div>
                <div>State 4: making</div>
                <div>State 5: starting</div>
                <div>State 6: preparing</div>
                <div>State 7: running</div>
                <div>State 8: finalizing</div>
                <div>State 9: ready, failed or killed</div>
                <div>Offline State: worker_offline</div>
            </div>
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
