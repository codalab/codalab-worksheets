import React from 'react';
import IconButton from '@material-ui/core/IconButton';
import Tooltip from '@material-ui/core/Tooltip';
import InfoIcon from '@material-ui/icons/Info';

class StateTooltip extends React.Component {
    render() {
        const href = 'https://codalab-worksheets.readthedocs.io/en/latest/features/bundles/states';
        const states = [
            'created',
            'staged',
            'making',
            'starting',
            'preparing',
            'running',
            'finalizing',
            'ready',
            'failed',
            'killed',
        ];
        const title = (
            <div>
                Bundle States:
                {states.map((state) => (
                    <div>- {state}</div>
                ))}
            </div>
        );

        return (
            <Tooltip title={title}>
                <IconButton
                    href={href}
                    target='_blank'
                    rel='noopener noreferrer'
                    style={{
                        padding: 4,
                        fontSize: 13,
                    }}
                >
                    <InfoIcon fontSize='inherit' />
                </IconButton>
            </Tooltip>
        );
    }
}

export default StateTooltip;
