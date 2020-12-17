import React from 'react';
import cx from 'clsx';
import { withStyles } from '@material-ui/core/styles';
import Box from '@material-ui/core/Box';
import Card from '@material-ui/core/Card';
import Avatar from '@material-ui/core/Avatar';
import Divider from '@material-ui/core/Divider';
import LinearProgress from '@material-ui/core/LinearProgress';
import $ from 'jquery';
import { lighten } from '@material-ui/core/es/styles/colorManipulator';

const styles = ({ spacing, palette }) => {
    const family =
        '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"';
    return {
        box: { marginLeft: 8, marginTop: 8, marginBottom: 8 },
        progressBox: {
            display: 'flex',
            alignItems: 'center',
            width: '100%',
            height: 0,
            paddingRight: 4,
            paddingTop: 8,
        },
        card: {
            display: 'flex',
            flexDirection: 'column',
            padding: spacing(2),
            height: '100%',
            boxShadow: '0 2px 4px 0 rgba(138, 148, 159, 0.2)',
            '& > *:nth-child(1)': {
                marginRight: spacing(2),
            },
            '& > *:nth-child(2)': {
                flex: 'auto',
            },
        },
        placeholderBox: {
            marginBottom: 800,
        },
        avatar: { marginLeft: 8, marginTop: 8, marginBottom: 8 },
        heading: {
            fontFamily: family,
            fontSize: 16,
            marginBottom: 0,
        },
        subheader: {
            fontSize: 14,
            marginBottom: 4,
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 300,
            letterSpacing: 0.1,
            color: '#000000',
            lineHeight: '150%',
        },
        affiliation: {
            fontSize: 15,
            marginBottom: 4,
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 'normal',
            letterSpacing: 0.15,
            color: '#000000',
        },
        value: {
            fontSize: 14,
            color: palette.grey[500],
        },
        name: {
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 500,
            fontSize: 20,
            letterSpacing: 0.15,
        },
    };
};

const BorderLinearProgress = withStyles({
    root: {
        height: 10,
        width: 600,
        backgroundColor: lighten('#3577cb', 0.5),
    },
    bar: {
        borderRadius: 20,
        backgroundColor: '#3577cb',
    },
})(LinearProgress);

class SideBar extends React.Component {
    /** Constructor. */
    constructor(props) {
        super(props);
        this.state = {
            bundles: [],
        };
    }

    componentDidMount() {
        const { classes } = this.props;
        // Fetch bundles' count in different states owned by the current user one by one
        let states: String[] = [
            'uploading',
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
            'worker_offline',
        ];
        const bundleUrl: URL = '/rest/interpret/search';
        const fetchBundles = (stateIndex, bundlesDict) => {
            $.ajax({
                url: bundleUrl,
                dataType: 'json',
                type: 'POST',
                cache: false,
                data: JSON.stringify(
                    stateIndex < states.length
                        ? {
                              keywords: [
                                  'owner=' + this.props.userInfo.user_name,
                                  '.count',
                                  'state=' + states[stateIndex],
                              ],
                          }
                        : {
                              keywords: [
                                  'owner=' + this.props.userInfo.user_name,
                                  '.count',
                                  '.floating',
                              ],
                          },
                ),
                contentType: 'application/json; charset=utf-8',
                success: (data) => {
                    if (stateIndex < states.length) {
                        bundlesDict[states[stateIndex]] = data.response.result;
                        fetchBundles(stateIndex + 1, bundlesDict);
                    } else {
                        bundlesDict['floating'] = data.response.result;
                        const bundles: HTMLElement[] = [];
                        for (let state in bundlesDict) {
                            // Only show the non-zero items
                            if (bundlesDict[state] > 0) {
                                bundles.push(
                                    <li key={state} className={classes.subheader}>
                                        {state + ': ' + bundlesDict[state]}
                                    </li>,
                                );
                            }
                        }
                        this.setState({ bundles: bundles });
                    }
                },
                error: (xhr, status, err) => {
                    console.error(xhr.responseText);
                },
            });
        };
        // Start to fetch the bundles' count
        fetchBundles(0, {});
    }

    render() {
        const { classes, userInfo } = this.props;
        if (!userInfo) {
            return null;
        }
        return (
            <Card className={cx(classes.card)} elevation={0} style={{ height: '100%' }}>
                <Avatar src={'https://i.pravatar.cc/30'} className={classes.avatar} />
                <Box className={classes.box}>
                    <h3 className={classes.name}>{userInfo.user_name}</h3>
                    {userInfo.affiliation ? (
                        <p className={classes.affiliation}>Affiliation: {userInfo.affiliation}</p>
                    ) : null}
                </Box>
                <Divider />

                {this.props.showQuota ? (
                    <Box className={classes.box}>
                        <a
                            className={classes.subheader}
                            href={
                                'https://github.com/codalab/codalab-worksheets/blob/master/docs/FAQ.md#how-do-i-reduce-the-amount-of-disk-usage'
                            }
                        >
                            Disk Usage (Bytes)
                        </a>
                        <Box className={classes.progressBox}>
                            <BorderLinearProgress
                                className={classes.margin}
                                variant='determinate'
                                color='secondary'
                                value={(userInfo.disk_used / userInfo.disk_quota) * 100}
                            />
                            <span className={classes.value}>
                                {Math.floor(userInfo.disk_quota / 1024 / 1024) + 'GB'}
                            </span>
                        </Box>
                        <span className={classes.value}>
                            {Math.floor((userInfo.disk_used / userInfo.disk_quota) * 100) + '%'}
                        </span>
                        <br />
                        <a
                            className={classes.subheader}
                            href={
                                'https://github.com/codalab/codalab-worksheets/blob/master/docs/FAQ.md'
                            }
                        >
                            Time Usage (Seconds)
                        </a>
                        <Box className={classes.progressBox}>
                            <BorderLinearProgress
                                className={classes.margin}
                                variant='determinate'
                                color='secondary'
                                value={(userInfo.time_used / userInfo.time_quota) * 100}
                            />
                            <span className={classes.value}>
                                {Math.floor(userInfo.time_quota / 3600) + 'hr'}
                            </span>
                        </Box>
                        <span className={classes.value}>
                            {Math.floor((userInfo.time_used / userInfo.time_quota) * 100) + '%'}
                        </span>
                    </Box>
                ) : null}
                <Box className={classes.box}>
                    <p className={classes.subheader}>
                        Bundles (
                        <a
                            className={classes.subheader}
                            href={
                                'https://github.com/codalab/codalab-worksheets/blob/master/codalab/worker/bundle_state.py'
                            }
                        >
                            Status
                        </a>
                        : Count)
                    </p>
                    <Box display={'flex'} alignItems={'center'}>
                        <ul style={{ listStyleType: 'circle' }}>{this.state.bundles}</ul>
                    </Box>
                </Box>
                <Divider />
                <Box className={classes.placeholderBox}></Box>
            </Card>
        );
    }
}

export default withStyles(styles)(SideBar);
