// @flow
import * as React from 'react';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import { FileBrowserLite } from '../../FileBrowser/FileBrowser';
import CollapseButton from '../../CollapseButton';
import CodeSnippet from '../../CodeSnippet';

class MainContent extends React.Component<{
    bundleInfo: {},
    stdout: string | null,
    strerr: string | null,
    fileContents: string | null,
    classes: {},
}> {
    state = {
        showCommand: true,
        showFailureMessage: true,
        showStdOut: true,
        showStdError: true,
        showFileBrowser: true,
    };

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

    render() {
        const { classes, bundleInfo, stdout, stderr, fileContents } = this.props;
        const command = bundleInfo.command;
        const failure_message = bundleInfo.metadata.failure_message;
        let isRunningBundle =
            bundleInfo.bundle_type === 'run' &&
            (bundleInfo.state === 'running' ||
                bundleInfo.state === 'preparing' ||
                bundleInfo.state === 'starting' ||
                bundleInfo.state === 'staged');

        return (
            <div className={classes.outter}>
                <Grid container>
                    {/** Failure components ================================================================= */}
                    {failure_message && (
                        <Grid container>
                            <CollapseButton
                                label='Failure Message'
                                collapsed={this.state.showFailureMessage}
                                onClick={() => this.toggleShowFailureMessage()}
                            />
                            {this.state.showFailureMessage && (
                                <CodeSnippet code={failure_message} variant='failure' />
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
                                <CodeSnippet code={command} copyMessage='Command Copied!' />
                            )}
                        </Grid>
                    )}
                    {/** Stdout/stderr components ================================================================= */}
                    <Grid container>
                        {stdout && (
                            <Grid container>
                                <CollapseButton
                                    label='Stdout'
                                    collapsed={this.state.showStdOut}
                                    onClick={() => this.toggleStdOut()}
                                />
                                {this.state.showStdOut && <CodeSnippet code={stdout} />}
                            </Grid>
                        )}
                        {stderr && (
                            <Grid container>
                                <CollapseButton
                                    label='Stderr'
                                    collapsed={this.state.showStdError}
                                    onClick={() => this.toggleStdError()}
                                />
                                {this.state.showStdError && <CodeSnippet code={stderr} />}
                            </Grid>
                        )}
                    </Grid>
                    {/** Bundle contents browser ================================================================== */}
                    <CollapseButton
                        label={fileContents ? 'Contents' : 'Files'}
                        collapsed={this.state.showFileBrowser}
                        onClick={() => this.toggleFileViewer()}
                    />
                    {this.state.showFileBrowser ? (
                        <Grid item xs={12}>
                            {fileContents ? (
                                <div className={`${classes.snippet} ${classes.greyBorder}`}>
                                    {fileContents}
                                </div>
                            ) : (
                                <div className={classes.snippet}>
                                    <FileBrowserLite
                                        uuid={bundleInfo.uuid}
                                        isRunningBundle={isRunningBundle}
                                    />
                                </div>
                            )}
                        </Grid>
                    ) : null}
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
    greyBorder: {
        border: `1px solid ${theme.color.grey.light}`,
    },
});

export default withStyles(styles)(MainContent);
