import React from 'react';
import cx from 'clsx';
import { withStyles } from '@material-ui/core/styles';
import Box from '@material-ui/core/Box';
import Card from '@material-ui/core/Card';
import Avatar from '@material-ui/core/Avatar';
import Slider from '@material-ui/core/Slider';
import Divider from '@material-ui/core/Divider';

const styles = ({ spacing, palette }) => {
    const family =
        '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif, "Apple Color Emoji", "Segoe UI Emoji", "Segoe UI Symbol"';
    return {
        box: { marginLeft: 8, marginTop: 8, marginBottom: 8 },
        card: {
            display: 'flex',
            flexDirection: 'column',
            padding: spacing(2),
            height: '100%',
            minWidth: 288,
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
            fontFamily: family,
            fontSize: 14,
            color: palette.grey[600],
            letterSpacing: '1px',
            marginBottom: 4,
        },
        email: {
            fontFamily: family,
            textDecoration: 'underline',
            fontSize: 14,
            marginBottom: 4,
        },
        value: {
            fontSize: 14,
            color: palette.grey[500],
        },
    };
};

const sliderStyles = () => ({
    root: {
        height: 4,
    },
    rail: {
        borderRadius: 10,
        height: 4,
        backgroundColor: 'rgb(202,211,216)',
    },
    track: {
        borderRadius: 10,
        height: 4,
        backgroundColor: 'rgb(117,156,250)',
    },
    thumb: {
        display: 'none',
    },
});

const StyledSlider = withStyles(sliderStyles)(Slider);

class SideBar extends React.Component {
    render() {
        const { classes, userInfo } = this.props;
        if (!userInfo) {
            return null;
        }
        return (
            <Card className={cx(classes.card)} elevation={0} style={{ height: '100%' }}>
                <Avatar src={'https://i.pravatar.cc/30'} className={classes.avatar} />
                <Box className={classes.box}>
                    <h3 className={classes.heading}>{userInfo.user_name}</h3>
                    <a className={classes.email} href={userInfo.email}>
                        {userInfo.email}
                    </a>
                    <p className={classes.subheader}>Affiliation: {userInfo.affiliation}</p>
                </Box>
                <Divider />
                <Box className={classes.box}>
                    <h3 className={classes.heading}>Basic Statistics</h3>
                    <p className={classes.subheader}>My Disk Usage</p>
                    <Box display={'flex'} alignItems={'center'}>
                        <StyledSlider
                            classes={sliderStyles}
                            defaultValue={userInfo.disk_used / userInfo.disk_quota}
                        />
                        <span className={classes.value}>
                            {userInfo.disk_used}/{userInfo.disk_quota}
                        </span>
                    </Box>
                    <p className={classes.subheader}>My Time Usage</p>
                    <Box display={'flex'} alignItems={'center'}>
                        <StyledSlider
                            classes={sliderStyles}
                            defaultValue={userInfo.time_used / userInfo.time_quota}
                        />
                        <span className={classes.value}>
                            {userInfo.time_used}/{userInfo.time_quota}
                        </span>
                    </Box>
                </Box>
                <Divider />
                <Box className={classes.placeholderBox}></Box>
            </Card>
        );
    }
}

export default withStyles(styles)(SideBar);
