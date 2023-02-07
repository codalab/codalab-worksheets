// @flow
import * as React from 'react';
import queryString from 'query-string';
import { withStyles } from '@material-ui/core';
import Dialog from '@material-ui/core/Dialog';
import IconButton from '@material-ui/core/IconButton';
import CloseIcon from '@material-ui/icons/Close';
import Button from '@material-ui/core/Button';
import Snackbar from '@material-ui/core/Snackbar';
import DownloadLink from '../../DownloadLink';
import NewRun from '../NewRun';
import { buildTerminalCommand } from '../../../util/worksheet_utils';
import { executeCommand } from '../../../util/apiWrapper';

class BundleActions extends React.Component<{
    bundleInfo: {},
    onComplete: () => any,
}> {
    constructor(props) {
        super(props);
        this.state = {
            killSnackbarIsOpen: false,
            showNewRerun: false,
            defaultRun: {},
            rerunErrorMessage: null,
        };
    }

    static defaultProps = {
        onComplete: () => undefined,
    };

    handleShowNewRerun = () => {
        const { bundleInfo } = this.props;
        const run = {};
        run.command = bundleInfo.command;
        const dependencies = [];
        bundleInfo.dependencies.forEach((dep) => {
            dependencies.push({
                target: { name: dep.parent_name, uuid: dep.parent_uuid, path: dep.parent_path },
                alias: dep.child_path,
            });
        });
        // The rerun config matches genpath = 'args', except for request_time and request_priority
        run.dependencies = dependencies;
        run.name = bundleInfo.metadata.name;
        run.description = bundleInfo.metadata.description;
        run.disk = bundleInfo.metadata.request_disk;
        run.cpu = bundleInfo.metadata.request_cpus;
        run.gpu = bundleInfo.metadata.request_gpus;
        run.memory = bundleInfo.metadata.request_memory;
        run.docker = bundleInfo.metadata.request_docker_image;
        run.networkAccess = bundleInfo.metadata.request_network;
        run.failedDependencies = bundleInfo.metadata.allow_failed_dependencies;
        run.queue = bundleInfo.metadata.request_queue;
        run.exclude_patterns = bundleInfo.metadata.exclude_patterns;

        this.setState({
            showNewRerun: true,
            defaultRun: run,
        });
    };

    handleHideNewRerun = () => {
        this.setState({
            showNewRerun: false,
            rerunErrorMessage: null,
        });
    };

    handleSubmitRerun = (resp) => {
        this.handleHideNewRerun();
        const bundleUUID = resp?.output;
        if (bundleUUID) {
            const { wsUUID } = this.props;
            const { focus, subfocus } = queryString.parse(window.location.search);
            const newSubFocus = parseInt(subfocus) + 1;
            const url = `/worksheets/${wsUUID}?bundle=${bundleUUID}&focus=${focus}&subfocus=${newSubFocus}`;
            window.open(url, '_self');
        }
    };

    handleRerunError = (errorMessage) => {
        this.setState({
            showNewRerun: false,
            rerunErrorMessage: errorMessage,
        });
    };

    handleOpenKillSnackbar = () => {
        this.setState({ killSnackbarIsOpen: true });
    };

    handleCloseKillSnackbar = () => {
        this.setState({ killSnackbarIsOpen: false });
    };

    killSnackbarAction = (
        <IconButton
            size='small'
            aria-label='close'
            color='inherit'
            onClick={this.handleCloseKillSnackbar}
        >
            <CloseIcon fontSize='small' />
        </IconButton>
    );

    kill = () => {
        const { bundleInfo } = this.props;
        this.handleOpenKillSnackbar();
        executeCommand(buildTerminalCommand(['kill', bundleInfo.uuid])).then(() => {
            this.props.onComplete();
            this.handleCloseKillSnackbar();
        });
    };

    render() {
        const { bundleInfo, classes, editPermission, wsUUID, after_sort_key } = this.props;
        const { showNewRerun, defaultRun, rerunErrorMessage } = this.state;
        const state = bundleInfo.state;
        const bundleDownloadUrl = '/rest/bundles/' + bundleInfo.uuid + '/contents/blob/';
        const isRunBundle = bundleInfo.bundle_type === 'run' && bundleInfo.metadata;
        const isKillableBundle = state === 'running' || state === 'preparing' || state === 'staged';
        const isDownloadableRunBundle =
            state !== 'preparing' &&
            state !== 'starting' &&
            state !== 'created' &&
            state !== 'staged';
        const showDownloadLink = isRunBundle ? isDownloadableRunBundle : true;

        return (
            <>
                <div className={classes.ctaContainer}>
                    {isRunBundle && editPermission && (
                        <>
                            <Snackbar
                                classes={{ root: classes.snackbar }}
                                open={this.state.killSnackbarIsOpen}
                                onClose={this.handleCloseKillSnackbar}
                                message='Executing kill command...'
                                action={this.killSnackbarAction}
                            />
                            <Button
                                classes={{ root: classes.killButton }}
                                variant='text'
                                color='primary'
                                disabled={!isKillableBundle}
                                onClick={this.kill}
                            >
                                Kill
                            </Button>
                            <Button
                                classes={{ root: classes.rerunButton }}
                                variant='contained'
                                color='primary'
                                onClick={this.handleShowNewRerun}
                            >
                                Rerun
                            </Button>
                        </>
                    )}
                    {showDownloadLink && <DownloadLink href={bundleDownloadUrl} />}
                </div>
                {rerunErrorMessage && (
                    <div className={classes.rerunError}>
                        <span className={classes.bold}>ERROR:</span> {rerunErrorMessage}
                    </div>
                )}
                <Dialog open={showNewRerun} onClose={this.handleHideNewRerun} maxWidth='lg'>
                    <NewRun
                        ws={{ info: { uuid: wsUUID } }}
                        after_sort_key={after_sort_key}
                        onError={this.handleRerunError}
                        onSubmit={this.handleSubmitRerun}
                        defaultRun={defaultRun}
                    />
                </Dialog>
            </>
        );
    }
}

const styles = (theme) => ({
    ctaContainer: {
        display: 'flex',
    },
    killButton: {
        minWidth: 50,
    },
    rerunButton: {
        marginRight: 14,
    },
    snackbar: {
        marginBottom: 40,
    },
    rerunError: {
        width: '100%',
        marginTop: 30,
        fontSize: 16,
        color: theme.color.red.base,
    },
    bold: {
        fontWeight: 500,
    },
});

export default withStyles(styles)(BundleActions);
