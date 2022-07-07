import React from 'react';
import { withStyles } from '@material-ui/core/styles';
import Typography from '@material-ui/core/Typography';
import Button from '@material-ui/core/Button';
import ArrowDownwardIcon from '@material-ui/icons/ArrowDownward';

// import FileDownloadIcon from '@material-ui/icons/FileDownload';

import InsertDriveFileIcon from '@material-ui/icons/InsertDriveFile';

import BundleFieldRow from '../shared/BundleFieldRow';

// test run bundle
const bundle = {
    // bundle info
    uuid: '0x87a99d9d143f46a49939ae5ffac14bb9',
    name: 'run-echo',
    owner_id: 'leilenah',
    created: 'Sun Jul 03 2022 16:01:34',
    permissions: '[you(all) public(read)]',

    host_worksheets: ['leilenah-home'],
    dependencies: [],

    // editable fields
    editable_fields: {
        name: 'run-echo',
        description: '',
        allow_failed_dependencies: '',
        request_docker_image: '',
        request_time: '',
        request_memory: '',
        request_disk: '',
        request_cpus: '',
        request_gpus: '',
        request_queue: '',
        request_priority: '',
        request_network: '',
        exclude_patterns: '',
        store: '',
        tags: '',
    },

    // run info
    state: 'staged',
    state_details:
        'Bundle’s dependencies are all ready, so just waiting for workers to do their job.',
    failure_message: 'Error uploading bundle',
    cpu_usage: '0',
    memory_usage: '0',
    memory: '4k',
    docker_image:
        'codalab/default-cpu@sha256:28ad5759188ff9ad9f7fb226cac996789dbe250e764fbce8771ef65b63750c18',
    on_preemptible_worker: 'false',
    started: 'Sun Jul 03 2022 16:01:37',
    last_updated: 'Sun Jul 03 2022 16:02:18',
    time: '0s',
    time_cleaning_up: '4.3s',
    time_preparing: '4.4s',
    time_running: '3s',
    time_user: '2s',
    time_system: '0s',
    time_uploading_results: '8s',
    data_size: '4k',

    // code
    command: 'echo test 1',
    error_traceback: '',
    stdout: 'test 1',
};

const states = ['created', 'staged', 'starting', 'preparing', 'running', 'finalizing', 'ready'];

class RunBundle extends React.Component {
    render() {
        // TODO: state component

        const { classes } = this.props;
        return (
            <div className={classes.bundleContainer}>
                <div className={classes.bundleRunInfoContainer}>
                    <Typography className={classes.heading} variant='h6'>
                        Bundle Run
                    </Typography>

                    <div className={classes.bundleStateGraphic}>
                        <Typography variant='body1'>
                            {states.map((state, i) => {
                                const isCurrent = state == 'staged';
                                const isLast = i == states.length - 1;
                                const className = isCurrent ? classes.activeState : classes.state;
                                return (
                                    <div>
                                        <span className={className}>{state}</span>
                                        {!isLast && (
                                            <div>
                                                <ArrowDownwardIcon />
                                            </div>
                                        )}
                                    </div>
                                );
                            })}
                        </Typography>
                    </div>

                    <div className={classes.bundleStateInfo}>
                        <Typography variant='body2'>
                            <table>
                                <BundleFieldRow field='Status' value={bundle.state_details} />
                                <BundleFieldRow field='Started' value={bundle.started} />
                                <BundleFieldRow field='Last Updated' value={bundle.last_updated} />
                                <BundleFieldRow field='Time' value={bundle.time} />
                                <BundleFieldRow field='Time Running' value={bundle.time_running} />
                                <BundleFieldRow
                                    field='Time Preparing'
                                    value={bundle.time_preparing}
                                />
                                <BundleFieldRow
                                    field='Time Cleaning Up'
                                    value={bundle.time_cleaning_up}
                                />
                                <BundleFieldRow
                                    field='Time Uploading'
                                    value={bundle.time_uploading_results}
                                />
                                <BundleFieldRow field='User Time' value={bundle.time_user} />
                                <BundleFieldRow field='System Time' value={bundle.time_system} />
                                <BundleFieldRow field='CPU Usage' value={bundle.cpu_usage} />
                                <BundleFieldRow field='Memory Usage' value={bundle.memory_usage} />
                                <BundleFieldRow field='Memory' value={bundle.memory} />
                            </table>
                        </Typography>
                    </div>
                </div>

                <div className={classes.bundleInfoContainer}>
                    <div className={classes.bundleIDInfo}>
                        <Typography className={classes.heading} variant='h6'>
                            Bundle Info
                        </Typography>

                        <Typography variant='body2'>
                            <table>
                                <BundleFieldRow field='UUID' value={bundle.uuid} />
                                <BundleFieldRow field='Name' value='run-echo' />
                                <BundleFieldRow field='Owner' value={bundle.owner_id} />
                                <BundleFieldRow field='Created' value={bundle.created} />
                                <BundleFieldRow field='Permissions' value={bundle.permissions} />

                                <BundleFieldRow field='Host Worksheets' value='home-leilenah' />
                                <BundleFieldRow field='Data Size' value={bundle.data_size} />
                                <BundleFieldRow
                                    field='Preemptible'
                                    value={bundle.on_preemptible_worker}
                                />
                                <BundleFieldRow field='Docker Image' value={bundle.docker_image} />
                            </table>

                            <Button className={classes.addButton} variant='text' size='small'>
                                + Add Bundle Info
                            </Button>
                        </Typography>
                    </div>

                    <div className={classes.bundleCommand}>
                        <Typography className={classes.heading} variant='h6'>
                            Bundle Command
                        </Typography>

                        <div className={classes.commandBlock}>
                            <pre>
                                <code>{bundle.command}</code>
                            </pre>
                        </div>
                    </div>

                    <div className={classes.bundleStdout}>
                        <Typography className={classes.heading} variant='h6'>
                            Bundle Stdout
                        </Typography>

                        <div className={classes.commandBlock}>
                            <pre>
                                <code>this is the bundle output</code>
                            </pre>
                        </div>
                    </div>

                    <div className={classes.bundleContents}>
                        <Typography className={classes.heading} variant='h6'>
                            Bundle Contents{' '}
                            <span
                                className={`glyphicon glyphicon-download-alt ${classes.downloadIcon}`}
                            />
                        </Typography>

                        <div className={classes.filesContainer}>
                            <Typography variant='body2'>
                                <div className={classes.file}>
                                    <span
                                        className={`glyphicon-file glyphicon ${classes.fileIcon}`}
                                    />{' '}
                                    <a href='#'>stderr</a>
                                </div>
                                <div className={classes.file}>
                                    <span
                                        className={`glyphicon-file glyphicon ${classes.fileIcon}`}
                                    />{' '}
                                    <a href='#'>stdout</a>
                                </div>
                                <div className={classes.lastFile}>
                                    <span
                                        className={`glyphicon-file glyphicon ${classes.fileIcon}`}
                                    />{' '}
                                    <a href='#'>image.png</a>
                                </div>
                            </Typography>
                        </div>
                    </div>
                </div>
            </div>
        );
    }
}

const styles = (theme) => ({
    bundleContainer: {
        display: 'flex',
        justifyContent: 'center',
        padding: 30,
    },
    bundleRunInfoContainer: {
        backgroundColor: 'white',
        borderRadius: 12,
        boxShadow: '0 2px 4px 0 rgb(138 148 159 / 20%)',
        marginRight: 20,
        width: 300,
        border: '1px solid rgb(222, 226, 230)',
    },
    bundleInfoContainer: {
        width: 600,
    },
    bundleIDInfo: {
        backgroundColor: 'white',
        borderRadius: 12,
        boxShadow: '0 2px 4px 0 rgb(138 148 159 / 20%)',
        border: '1px solid rgb(222, 226, 230)',
    },
    downloadIcon: {
        fontSize: '15px',
    },
    fileIcon: {
        fontSize: '12px',
        color: '#999',
    },
    bundleStateGraphic: {
        textAlign: 'center',
        padding: '5px 0 30px',
    },
    heading: {
        textAlign: 'center',
        paddingTop: 10,
        paddingBottom: 10,
    },
    addButton: {
        marginLeft: 20,
        marginBottom: 8,
    },
    state: {
        border: '1px solid rgb(222, 226, 230)',
        padding: '2px 10px',
        borderRadius: '35px',
    },
    activeState: {
        color: 'white',
        backgroundColor: 'rgb(244, 202, 100)',
        padding: '2px 10px',
        borderRadius: '35px',
        animation: 'blinker 1100ms linear infinite',
    },
    '@keyframes blinker': {
        '50%': {
            opacity: 0,
        },
    },
    filesContainer: {
        border: '1px solid #ccc',
        borderRadius: 4,
        padding: '6px 15px',
        backgroundColor: 'white',
    },
    file: {
        borderBottom: '1px solid rgb(222, 226, 230)',
        padding: '5px 0',
    },
    lastFile: {
        padding: '5px 0',
    },
});

export default withStyles(styles)(RunBundle);
