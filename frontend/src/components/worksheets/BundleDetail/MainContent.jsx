// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import CopyIcon from '@material-ui/icons/FileCopy';
import Tooltip from '@material-ui/core/Tooltip';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import { CopyToClipboard } from 'react-copy-to-clipboard';

import { FileBrowserLite } from '../../FileBrowser';

class MainContent extends React.Component<
		{
			bundleInfo: {},
			stdout: string | null,
			strerr: string | null,
			fileContents: string | null,
			classes: {},
		}
> {	
	
	render() {
		const {
			classes, bundleInfo, stdout, stderr, fileContents } = this.props;
		const bundleState = (bundleInfo.state == 'running' &&
							bundleInfo.metadata.run_status != 'Running')
					? bundleInfo.metadata.run_status
					: bundleInfo.state;
        const isRunBundle = bundleInfo.bundle_type === 'run';
		const stateSpecClass = bundleInfo.state === 'failed'
            ? 'failedState'
            : (bundleInfo.state === 'ready' ? 'readyState' : 'otherState');
		
		return (
            <div className={ classes.outter }>
                <div className={ `${ classes.stateBox } ${ classes[stateSpecClass] }`}>
                    { bundleState }
                </div>
    			<Grid container classes={ { container: classes.container } } spacing={16}>
                    { /** Run bundle specific components =========================================================== */}
                    { isRunBundle &&
                        <Grid item xs={12} md="auto">
                            <Typography variant="body1">
                                run time: { bundleInfo.metadata.time || '-- --' }
                            </Typography>
                        </Grid>
                    }
                    { isRunBundle &&
                        <Grid item xs={12}>  
                            <CopyToClipboard
                                text={ bundleInfo.command }
                            >
                                <div
                                    className={ `${ classes.row } ${ classes.command }` }
                                >
                                    <span>{ bundleInfo.command }</span>
                                    <Tooltip title="Copy to clipboard">
                                        <CopyIcon
                                            style={ { color: 'white', marginLeft: 8 } }
                                        />
                                    </Tooltip>
                                </div>
                            </CopyToClipboard>
                        </Grid>
                    }
                    { /** Stdout/stderr components ================================================================= */}
                    <Grid item xs={12}>
                        <Grid container>
            				{ stdout &&
                                <Grid item xs={12}>
                                    <Typography variant="subtitle1">stdout</Typography>
                					<div className={ classes.snippet }>
                						{ stdout }
                					</div>
                                </Grid>
            				}
            				{ stderr &&
                                <Grid item xs={12}>
                                    <Typography variant="subtitle1">stderr</Typography>
                					<div className={ classes.snippet }>
                						{ stderr }
                					</div>
                                </Grid>
            				}
                        </Grid>
                    </Grid>
                    { /** Bundle contents browser ================================================================== */}
                    <Grid item xs={12}>
        				{ fileContents
        					? <div className={ classes.snippet }>
        						{ fileContents }
        					</div>
        					: <FileBrowserLite
                                uuid={ bundleInfo.uuid }
                            />
        				}
                    </Grid>
    			</Grid>
            </div>
		);
	}
}

const styles = (theme) => ({
    outter: {
        flex: 1,
    },
	container: {
		padding: theme.spacing.larger,
	},
    row: {
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'center',
        justifyContent: 'space-between',
    },
    stateBox: {
        color: 'white',
        fontSize: '1.25rem',
        width: `calc(100% + ${ 2*theme.spacing.larger }px)`,
        textAlign: 'center',
        marginTop: -theme.spacing.larger,
        marginLeft: -theme.spacing.larger,
    },
    readyState: {
        backgroundColor: theme.color.green.base,
    },
    failedState: {
        backgroundColor: theme.color.red.base,
    },
    otherState: {
        backgroundColor: theme.color.yellow.base,
    },
	command: {
        flex: 1,
		backgroundColor: '#333',
		color: 'white',
		fontFamily: 'monospace',
		padding: theme.spacing.large,
        borderRadius: theme.spacing.unit,
	},
	snippet: {
		fontFamily: 'monospace',
		backgroundColor: theme.color.grey.lightest,
		height: 160,
		marginBottom: theme.spacing.large,
        overflow: 'auto',
	},
});

export default withStyles(styles)(MainContent);
