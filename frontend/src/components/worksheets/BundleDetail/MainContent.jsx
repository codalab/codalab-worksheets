// @flow
import * as React from 'react';
import classNames from 'classnames';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import { renderDuration } from '../../../util/worksheet_utils';
import { FileBrowserLite } from '../../FileBrowser/FileBrowser';
import Button from '@material-ui/core/Button';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';

class MainContent extends React.Component<{
    bundleInfo: {},
    stdout: string | null,
    strerr: string | null,
    fileContents: string | null,
    classes: {},
}> {
    state = {
        showStdOut: true,
        showStdError: true,
        showFileBrowser: true,
    };

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
        let isRunningBundle =
            bundleInfo.bundle_type === 'run' &&
            (bundleInfo.state === 'running' ||
                bundleInfo.state === 'preparing' ||
                bundleInfo.state === 'starting' ||
                bundleInfo.state === 'staged');

        return (
            <div className={classes.outter}>
                <Grid container>
                    {/** Stdout/stderr components ================================================================= */}
                    <Grid container>
                        {stdout && (
                            <Grid container>
                                <Button
                                    onClick={(e) => this.toggleStdOut()}
                                    size='small'
                                    color='inherit'
                                    aria-label='Show stdout'
                                >
                                    {'Stdout'}
                                    {this.state.showStdOut ? (
                                        <KeyboardArrowDownIcon />
                                    ) : (
                                        <ChevronRightIcon />
                                    )}
                                </Button>
                                {this.state.showStdOut && (
                                    <Grid item xs={12}>
                                        <div
                                            className={classNames({
                                                [classes.snippet]: true,
                                                [classes.greyBackground]: true,
                                            })}
                                        >
                                            {stdout}
                                        </div>
                                    </Grid>
                                )}
                            </Grid>
                        )}
                        {stderr && (
                            <Grid container>
                                <Button
                                    onClick={(e) => this.toggleStdError()}
                                    size='small'
                                    color='inherit'
                                    aria-label='Show stderr'
                                >
                                    {'Stderr'}
                                    {this.state.showStdError ? (
                                        <KeyboardArrowDownIcon />
                                    ) : (
                                        <ChevronRightIcon />
                                    )}
                                </Button>
                                {this.state.showStdError && (
                                    <Grid item xs={12}>
                                        <div
                                            className={classNames({
                                                [classes.snippet]: true,
                                                [classes.greyBackground]: true,
                                            })}
                                        >
                                            {stderr}
                                        </div>
                                    </Grid>
                                )}
                            </Grid>
                        )}
                    </Grid>
                    {/** Bundle contents browser ================================================================== */}
                    <Button
                        onClick={(e) => this.toggleFileViewer()}
                        size='small'
                        color='inherit'
                        aria-label='Expand file viewer'
                    >
                        {fileContents ? 'Contents' : 'Files'}
                        {this.state.showFileBrowser ? (
                            <KeyboardArrowDownIcon />
                        ) : (
                            <ChevronRightIcon />
                        )}
                    </Button>
                    {this.state.showFileBrowser ? (
                        <Grid item xs={12}>
                            {fileContents ? (
                                <div className={classes.snippet}>{fileContents}</div>
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
    row: {
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'center',
        justifyContent: 'space-between',
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
    greyBackground: {
        backgroundColor: theme.color.grey.lightest,
    },
});

export default withStyles(styles)(MainContent);
