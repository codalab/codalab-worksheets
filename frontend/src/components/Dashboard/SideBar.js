import React from 'react';
import cx from 'clsx';
import { withStyles } from '@material-ui/core/styles';
import Card from '@material-ui/core/Card';
import Divider from '@material-ui/core/Divider';
import LinearProgress from '@material-ui/core/LinearProgress';
import { lighten } from '@material-ui/core/es/styles/colorManipulator';
import { renderSize, renderDuration } from '../../util/worksheet_utils';
import { BUNDLE_STATES } from '../../constants';
import { default as AvatarEditorModal } from './EditableAvatar';
import Tooltip from '@material-ui/core/Tooltip';
import Button from '@material-ui/core/Button';
import { apiWrapper } from '../../util/apiWrapper';

const styles = ({ palette }) => {
    return {
        box: { marginLeft: 8, marginRight: 8, marginTop: 8, marginBottom: 0 },
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
            padding: 8,
            height: '100%',
            boxShadow: '0 2px 4px 0 rgba(138, 148, 159, 0.2)',
            '& > *:nth-child(1)': {
                marginRight: 8,
            },
        },
        placeholderBox: {
            marginBottom: 800,
        },
        avatar: { marginLeft: 8, marginTop: 12, marginBottom: 8 },
        subheader: {
            fontSize: 14,
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 300,
            letterSpacing: 0.1,
            color: '#000000',
            lineHeight: '150%',
            marginBottom: 0,
        },
        affiliation: {
            fontSize: 15,
            paddingBottom: 8,
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
        fullname: {
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 500,
            fontSize: 20,
            letterSpacing: 0.15,
        },
        username: {
            fontFamily: 'Roboto',
            fontStyle: 'normal',
            fontWeight: 400,
            fontSize: 18,
            letterSpacing: 0.15,
        },
        progress: {
            minWidth: '85%',
            maxWidth: '85%',
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
        const bundleUrl: URL = '/rest/interpret/search';
        const fetchBundles = (stateIndex, bundlesDict) => {
            const data =
                stateIndex < BUNDLE_STATES.length
                    ? {
                          keywords: [
                              'owner=' + this.props.userInfo.user_name,
                              '.count',
                              'state=' + BUNDLE_STATES[stateIndex],
                          ],
                      }
                    : {
                          keywords: [
                              'owner=' + this.props.userInfo.user_name,
                              '.count',
                              '.floating',
                          ],
                      };
            apiWrapper
                .post(bundleUrl, data)
                .then((data) => {
                    if (stateIndex < BUNDLE_STATES.length) {
                        bundlesDict[BUNDLE_STATES[stateIndex]] = data.response.result;
                        fetchBundles(stateIndex + 1, bundlesDict);
                    } else {
                        bundlesDict['floating'] = data.response.result;
                        const bundles: HTMLElement[] = [];
                        for (let state in bundlesDict) {
                            // Only show the non-zero items
                            if (bundlesDict[state] > 0) {
                                bundles.push(
                                    <li key={state} className={classes.subheader}>
                                        {state}
                                        {': ' + bundlesDict[state]}
                                    </li>,
                                );
                            }
                        }
                        this.setState({ bundles: bundles });
                    }
                })
                .catch((error) => {
                    console.error(error);
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
                <AvatarEditorModal
                    userInfo={userInfo}
                    ownDashboard={this.props.ownDashboard}
                ></AvatarEditorModal>
                {/*<Avatar className={classes.avatar}>{userInfo.user_name.charAt(0)}</Avatar>*/}
                <div className={classes.box}>
                    <h3 className={classes.fullname}>
                        {userInfo.first_name + ' ' + userInfo.last_name}
                    </h3>
                    <h4 className={classes.username}>{userInfo.user_name}</h4>
                    {userInfo.affiliation ? (
                        <p className={classes.affiliation}>Affiliation: {userInfo.affiliation}</p>
                    ) : null}
                </div>
                <Divider />

                {this.props.ownDashboard ? (
                    <div className={classes.box}>
                        <a
                            className={classes.subheader}
                            href={
                                'https://codalab-worksheets.readthedocs.io/en/latest/FAQ/#how-do-i-reduce-the-amount-of-disk-usage'
                            }
                        >
                            Disk Usage
                        </a>
                        <div className={classes.progressBox}>
                            <BorderLinearProgress
                                className={classes.progress}
                                variant='determinate'
                                color='secondary'
                                value={(userInfo.disk_used / userInfo.disk_quota) * 100}
                            />
                            <span className={classes.value}>{renderSize(userInfo.disk_quota)}</span>
                        </div>
                        <span className={classes.value}>
                            {Math.floor((userInfo.disk_used / userInfo.disk_quota) * 100) + '%'}
                        </span>
                        <br />
                        <a
                            className={classes.subheader}
                            href={'https://codalab-worksheets.readthedocs.io/en/latest/FAQ'}
                        >
                            Time Usage
                        </a>
                        <div className={classes.progressBox}>
                            <BorderLinearProgress
                                className={classes.progress}
                                variant='determinate'
                                color='secondary'
                                value={(userInfo.time_used / userInfo.time_quota) * 100}
                            />
                            <span className={classes.value}>
                                {renderDuration(userInfo.time_quota)}
                            </span>
                        </div>
                        <span className={classes.value}>
                            {Math.floor((userInfo.time_used / userInfo.time_quota) * 100) + '%'}
                        </span>
                    </div>
                ) : null}
                <div className={classes.box}>
                    <p className={classes.subheader}>Bundles</p>
                    <div display={'flex'} alignItems={'center'}>
                        <ul style={{ listStyleType: 'circle' }}>{this.state.bundles}</ul>
                    </div>
                </div>
                <Divider />
                {this.props.ownDashboard ? (
                    <Tooltip title='Dashboard'>
                        <Button
                            variant='contained'
                            color='primary'
                            onClick={() => (window.location.href = '/worksheets?name=dashboard')}
                        >
                            View Dashboard
                        </Button>
                    </Tooltip>
                ) : null}
                <div className={classes.placeholderBox}></div>
            </Card>
        );
    }
}

export default withStyles(styles)(SideBar);
