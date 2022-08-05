// @flow
import * as React from 'react';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
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
            fetchingContent,
            fileContents,
            stderr,
            stdout,
        } = this.props;
        const uuid = bundleInfo.uuid;
        const stdoutUrl = '/rest/bundles/' + uuid + '/contents/blob/stdout';
        const stderrUrl = '/rest/bundles/' + uuid + '/contents/blob/stderr';
        const command = bundleInfo.command;
        const failureMessage = bundleInfo.metadata.failure_message;

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
                            {this.state.showFailureMessage && <CodeSnippet code={failureMessage} />}
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
                                <CodeSnippet code={command} copyMessage='Command Copied!' />
                            )}
                        </Grid>
                    )}
                    {fetchingContent ? (
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
                                            <CodeSnippet code={stdout} href={stdoutUrl} />
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
                                            <CodeSnippet code={stderr} href={stderrUrl} />
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
                                                <div
                                                    className={`${classes.snippet} ${classes.greyBorder}`}
                                                >
                                                    {fileContents}
                                                </div>
                                            ) : (
                                                <div className={classes.snippet}>
                                                    <FileBrowserLite
                                                        uuid={bundleInfo.uuid}
                                                        isRunningBundle={this.isRunning()}
                                                        showBreadcrumbs
                                                    />
                                                </div>
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
    snippet: {
        fontFamily: 'monospace',
        maxHeight: 300,
        padding: 10,
        flexWrap: 'wrap',
        flexShrink: 1,
        overflow: 'auto',
        whiteSpace: 'pre-wrap',
    },
    failureContainer: {
        color: theme.color.red.base,
    },
    greyBorder: {
        border: `1px solid ${theme.color.grey.light}`,
    },
});

export default withStyles(styles)(MainContent);
