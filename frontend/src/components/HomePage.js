import * as React from 'react';
import classNames from 'classnames';
import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import Typography from '@material-ui/core/Typography';
import Button from '@material-ui/core/Button';
import Table from '@material-ui/core/Table';
import TableCell from '@material-ui/core/TableCell';
import TableRow from '@material-ui/core/TableRow';
import ResponsiveEmbed from 'react-responsive-embed';

import UploadIcon from '@material-ui/icons/CloudUploadOutlined'; // insert_chart, cloud upload
import ExperimentIcon from '@material-ui/icons/InsertChartOutlined'; // extension, barchart, score
import PublishIcon from '@material-ui/icons/PublicOutlined'; // share, public

const kSidePadding = 32;
const kSpacerPadding = 24;

class HomePage extends React.Component<{
    classes: {},
    auth: {
        isAuthenticated: boolean,
        signout: () => void,
    },
}> {
    renderButton(title, href) {
        const { classes } = this.props;
        return (
            <Button
                variant='contained'
                color='primary'
                href={href}
                classes={{ root: classes.buttonRoot, label: classes.buttonLabel }}
            >
                {title}
            </Button>
        );
    }

    renderTableItem(title, description, href) {
        return (
            <TableRow style={{ height: 0 }}>
                <TableCell>
                    <a target='_blank' href={href}>
                        {title}
                    </a>
                </TableCell>
                <TableCell>{description}</TableCell>
            </TableRow>
        );
    }

    render() {
        const { classes, auth } = this.props;

        return (
            <Grid container>
                {/** Splash w/ tagline, primary buttons, and video.*/}
                <Grid item xs={12} container className={classes.splash}>
                    <Grid item xs={12} container className={classes.outer}>
                        <Grid item xs={12} container className={classes.inner}>
                            <Grid
                                item
                                xs={12}
                                sm={6}
                                container
                                className={classes.textBox}
                                alignContent='center'
                            >
                                    <Typography variant='h4' className={classes.tagline}>
                                        A collaborative platform for reproducible research.
                                    </Typography>
                                    <div className={classes.buttons}>
                                        {!auth.isAuthenticated && [
                                            this.renderButton('Sign Up', '/account/signup'),
                                            this.renderButton('Sign In', '/account/login'),
                                        ]}
                                        {auth.isAuthenticated && [
                                            this.renderButton(
                                                'My Home',
                                                '/rest/worksheets/?name=%2F',
                                            ),
                                            this.renderButton(
                                                'My Dashboard',
                                                '/rest/worksheets/?name=dashboard',
                                            ),
                                        ]}
                                    </div>
                            </Grid>
                            <Grid item xs={12} sm={6} container>
                                <div className={classes.video}>
                                    <ResponsiveEmbed
                                        src='https://www.youtube.com/embed/WwFGfgf3-5s'
                                        allowFullScreen
                                    />
                                </div>
                            </Grid>
                        </Grid>
                    </Grid>
                </Grid>

                <Grid item xs={12} className={classes.outer} container>
                    <Grid item xs={12} className={classes.inner} container>
                        {/** Summary. */}
                        <Grid item xs={12} className={classNames(classes.textBox, classes.spacer)}>
                            <Typography variant='h5' color='textSecondary' textAlign='center'>
                                Run your machine learning experiments in the cloud. Manage them in a
                                digital lab notebook. Publish them so other researchers can
                                reproduce your results.
                            </Typography>
                        </Grid>

                        <Grid item xs={12} sm={5} className={classes.textBox} container alignContent='center'>
                            <div className={classes.titleBox}>
                                <UploadIcon className={classes.titleIcon}/>
                                <Typography variant='h6' inline>Upload</Typography>
                            </div>
                            <Typography>
                                Upload code (in any programming language) and datasets (in any
                                format) as bundles. There are no constraints on how you structure
                                your bundles.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} sm={7} className={classes.textBox}>
                            <img
                                src={`${process.env.PUBLIC_URL}/img/summary1.png`}
                                className={classes.summaryImg}
                            />
                        </Grid>

                        <Grid item xs={12} sm={5} className={classes.textBox} container alignContent='center'>
                            <div className={classes.titleBox}>
                                <ExperimentIcon className={classes.titleIcon}/>
                                <Typography variant='h6' inline>Experiment</Typography>
                            </div>
                            <Typography>
                                Run your code in the cloud by specifying an arbitrary command along
                                with your bundle dependencies, a Docker execution environment, and
                                resource requirements. The output of the run becomes a new bundle.
                            </Typography>
                        </Grid>
                        <Grid item xs={12} sm={7} className={classes.textBox}>
                            <img
                                src={`${process.env.PUBLIC_URL}/img/summary2.png`}
                                className={classes.summaryImg}
                            />
                        </Grid>

                        <Grid item xs={12} sm={5} className={classes.textBox} container alignContent='center'>
                            <div className={classes.titleBox}>
                                <PublishIcon className={classes.titleIcon}/>
                                <Typography variant='h6' inline>Publish</Typography>
                            </div>
                            <Typography>
                                Organize your experiments in a worksheet (a digital lab notebook)
                                using an extended version of Markdown. You can set up custom tables
                                and graphs that are automatically populated with the output of your
                                runs. Share the worksheet with the world!
                            </Typography>
                        </Grid>
                        <Grid item xs={12} sm={7} className={classes.textBox}>
                            <img
                                src={`${process.env.PUBLIC_URL}/img/summary3.png`}
                                className={classes.summaryImg}
                            />
                        </Grid>

                        {/** Getting started. */}
                        <Grid
                            item
                            xs={12}
                            container
                            className={classNames(classes.textBox, classes.spacer)}
                        >
                            <Grid item xs={12} className={classes.textBox}>
                                <Typography variant='h5' textAlign='center'>
                                    Getting Started
                                </Typography>
                            </Grid>
                            <Grid item xs={12} sm={4} className={classes.textBox}>
                                <Typography>
                                    <pre>pip install codalab</pre>
                                </Typography>
                                <Typography gutterBottom>
                                    For <b>beginning users</b>, the best place to start is to clone
                                    the
                                    <code>codalab/worksheet-examples</code>
                                    repository and walk through the material there:
                                </Typography>
                                <Button
                                    variant='outlined'
                                    color='primary'
                                    href='https://github.com/codalab/worksheets-examples'
                                    target='_blank'
                                >
                                    Go To Repo
                                </Button>
                            </Grid>
                            <Grid item xs={12} sm={8} className={classes.textBox}>
                                <Typography>
                                    For more <b>advanced users</b>, the CodaLab Wiki has a wealth of
                                    information about many aspects of using the platform:
                                </Typography>
                                <Typography>
                                    <Table padding='dense'>
                                        {this.renderTableItem(
                                            'Workflow',
                                            'Use CodaLab in your daily research.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/Workflow',
                                        )}
                                        {this.renderTableItem(
                                            'Executable Papers',
                                            'Put your research paper on CodaLab.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/Executable-Papers',
                                        )}
                                        {this.renderTableItem(
                                            'CLI Reference',
                                            'Be an expert CodaLab user.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/CLI-Reference',
                                        )}
                                        {this.renderTableItem(
                                            'Worksheet Reference',
                                            'Insert custom tables, graphs, and images.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/Worksheet-Markdown',
                                        )}
                                        {this.renderTableItem(
                                            'REST API Reference',
                                            'Develop your own application against our REST API.',
                                            'https://github.com/codalab/codalab-cli/blob/master/docs/rest.md',
                                        )}
                                        {this.renderTableItem(
                                            'Execution',
                                            'Learn how bundles are executed in Docker.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/Execution',
                                        )}
                                        {this.renderTableItem(
                                            'Latest Features',
                                            'See what features have been added recently.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/Latest-Features',
                                        )}
                                        {this.renderTableItem(
                                            'Competitions',
                                            'Host your own machine learning competition.',
                                            'https://github.com/codalab/codalab-worksheets/wiki/Competitions',
                                        )}
                                        {this.renderTableItem(
                                            'Contributors',
                                            'Meet the team behind CodaLab and get involved!',
                                            'https://github.com/codalab/codalab-worksheets/wiki/About',
                                        )}
                                    </Table>
                                </Typography>
                            </Grid>
                        </Grid>

                        {/** Promotional. */}
                        <Grid
                            item
                            xs={12}
                            className={classNames(classes.textBox)}
                            justify='space-evenly'
                            container
                        >
                            <Grid item xs={12} className={classes.textBox}>
                                <Typography variant='h5' textAlign='center' gutterBottom>
                                    Featured Users
                                </Typography>
                            </Grid>

                            <Grid item className={classes.textBox}>
                                <img
                                    src={`${process.env.PUBLIC_URL}/img/users/stanford.png`}
                                    className={classes.userLogo}
                                />
                            </Grid>
                            <Grid item className={classes.textBox}>
                                <img
                                    src={`${process.env.PUBLIC_URL}/img/users/microsoft.png`}
                                    className={classes.userLogo}
                                />
                            </Grid>
                        </Grid>
                    </Grid>
                </Grid>
            </Grid>
        );
    }
}

const styles = (theme) => ({
    outer: {
        maxWidth: 1000,
        margin: '0 auto',
    },
    inner: {
        padding: `${kSpacerPadding}px ${kSidePadding}px`,
    },
    splash: {
        backgroundImage: `url(${process.env.PUBLIC_URL}/img/splash.png)`,
        backgroundSize: 'cover',
        minHeight: 300,
    },
    tagline: {
        fontWeight: 300,
        color: '#FFFFFF',
    },
    buttonRoot: {
        margin: 10,
    },
    buttonLabel: {
        color: '#FFFFFF',
    },
    video: {
        width: '100%',
        margin: 'auto',
    },
    textBox: {
        padding: '8px',
        verticalAlign: 'middle',
    },
    titleBox: {
        display: 'flex',
        alignItems: 'center',
    },
    titleIcon: {
        marginRight: 10,
    },
    spacer: {
        paddingTop: `${kSpacerPadding}px`,
        paddingBottom: `${kSpacerPadding}px`,
    },
    userLogo: {
        maxHeight: 100,
    },
    list: {
        paddingLeft: 32,
    },
    summaryImg: {
        width: '100%',
        borderRadius: 4,
        border: '1.5px solid #D5D5D5',
        boxShadow: '0px 0px 80px -40px rgba(0, 0, 0, 0.3)',
    },
});

export default withStyles(styles)(HomePage);
