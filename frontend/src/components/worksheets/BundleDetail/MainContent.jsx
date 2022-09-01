// @flow
import * as React from 'react';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import { FINAL_BUNDLE_STATES } from '../../../constants';
import { FileBrowserLite } from '../../FileBrowser/FileBrowser';
import CollapseButton from '../../CollapseButton';
import CodeSnippet from '../../CodeSnippet';
import Loading from '../../Loading';

class MainContent extends React.Component<{
    bundleInfo: {},
    stdout: string | null,
    strerr: string | null,
    fileContents: string | null,
    classes: {},
}> {
    constructor(props) {
        super(props);
        this.state = {
            showCommand: true,
            showFailureMessage: true,
            showStdOut: true,
            showStdError: true,
            showFileBrowser: true,
        };
    }

    toggleCommand() {
        this.setState({ showCommand: !this.state.showCommand });
    }

    toggleShowFailureMessage() {
        this.setState({ showFailureMessage: !this.state.showFailureMessage });
    }

    toggleFileViewer() {
        this.setState({ showFileBrowser: !this.state.showFileBrowser });
    }

    toggleStdOut() {
        this.setState({ showStdOut: !this.state.showStdOut });
    }

    toggleStdError() {
        this.setState({ showStdError: !this.state.showStdError });
    }

    isRunning() {
        const bundleInfo = this.props.bundleInfo;
        if (bundleInfo.bundle_type !== 'run') {
            return false;
        }
        const runStates = ['running', 'preparing', 'starting', 'staged'];
        return runStates.includes(bundleInfo.state);
    }

    render() {
        const {
            bundleInfo,
            classes,
            contentType,
            expanded,
            fileContents,
            stderr,
            stdout,
        } = this.props;
        const { command, metadata, state, uuid } = bundleInfo || {};
        const stdoutUrl = '/rest/bundles/' + uuid + '/contents/blob/stdout';
        const stderrUrl = '/rest/bundles/' + uuid + '/contents/blob/stderr';
        const failureMessage = metadata.failure_message;
        const inFinalState = FINAL_BUNDLE_STATES.includes(state);
        const isLoading = !inFinalState || !contentType;

        return (
            <div className={classes.outter}>
                <Grid container>
                    {/** Failure components ================================================================= */}
                    {failureMessage && (
                        <Grid classes={{ container: classes.failureContainer }} container>
                            <CollapseButton
                                label='Failure Message'
                                collapsed={this.state.showFailureMessage}
                                onClick={() => this.toggleShowFailureMessage()}
                            />
                            {this.state.showFailureMessage && (
                                <CodeSnippet code={failureMessage} expanded={expanded} />
                            )}
                        </Grid>
                    )}
                    {/** Command components ================================================================= */}
                    {command && (
                        <Grid container>
                            <CollapseButton
                                label='Command'
                                collapsed={this.state.showCommand}
                                onClick={() => this.toggleCommand()}
                            />
                            {this.state.showCommand && (
                                <CodeSnippet
                                    code={command}
                                    expanded={expanded}
                                    copyMessage='Command Copied!'
                                    noMargin={!isLoading && !stdout && !stderr && !contentType}
                                />
                            )}
                        </Grid>
                    )}
                    {isLoading ? (
                        <Loading />
                    ) : (
                        <>
                            {/** Stdout/stderr components ================================================================= */}
                            <Grid container>
                                {stdout && (
                                    <Grid container>
                                        <CollapseButton
                                            label='Stdout'
                                            collapsed={this.state.showStdOut}
                                            onClick={() => this.toggleStdOut()}
                                        />
                                        {this.state.showStdOut && (
                                            <CodeSnippet
                                                code={stdout}
                                                href={stdoutUrl}
                                                expanded={expanded}
                                            />
                                        )}
                                    </Grid>
                                )}
                                {stderr && (
                                    <Grid container>
                                        <CollapseButton
                                            label='Stderr'
                                            collapsed={this.state.showStdError}
                                            onClick={() => this.toggleStdError()}
                                        />
                                        {this.state.showStdError && (
                                            <CodeSnippet
                                                code={stderr}
                                                href={stderrUrl}
                                                expanded={expanded}
                                            />
                                        )}
                                    </Grid>
                                )}
                            </Grid>
                            {/** Bundle contents browser ================================================================== */}
                            {contentType && (
                                <>
                                    <CollapseButton
                                        label={fileContents ? 'Contents' : 'Files'}
                                        collapsed={this.state.showFileBrowser}
                                        onClick={() => this.toggleFileViewer()}
                                    />
                                    {this.state.showFileBrowser && (
                                        <Grid item xs={12}>
                                            {fileContents ? (
                                                <CodeSnippet
                                                    code={fileContents}
                                                    expanded={expanded}
                                                    noMargin
                                                />
                                            ) : (
                                                <FileBrowserLite
                                                    uuid={bundleInfo.uuid}
                                                    isRunningBundle={this.isRunning()}
                                                    showBreadcrumbs
                                                />
                                            )}
                                        </Grid>
                                    )}
                                </>
                            )}
                        </>
                    )}
                </Grid>
            </div>
        );
    }
}

const styles = (theme) => ({
    outter: {
        flex: 1,
    },
    failureContainer: {
        color: theme.color.red.base,
    },
});

export default withStyles(styles)(MainContent);
