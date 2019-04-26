// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import CopyIcon from '@material-ui/icons/FileCopy';
import Tooltip from '@material-ui/core/Tooltip';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core';
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
			<Grid container classes={ { container: classes.container } } spacing={16}>
                <Grid item xs={12}>
                    <Grid
                        container
                        direction="row"
                        alignItems="center"
                        spacing={16}
                    >
        				<Grid item xs={12} md="auto">
                            <div className={ `${ classes.stateBox } ${ classes[stateSpecClass] }`}>
                                { bundleState }
                            </div>
                        </Grid>
                        { isRunBundle &&
                            <Grid item xs={12} md="auto">
                                <Typography variant="body1">
                                    run time: { bundleInfo.metadata.time || '-- --' }
                                </Typography>
                            </Grid>
                        }
                    </Grid>
                </Grid>
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
                <Grid item xs={12} md={8}>
                    <Grid container>
        				{ stdout &&
                            <Grid item xs={12}>
            					<div className={ classes.snippet }>
            						<b>stdout</b>
            						{ stdout }
            					</div>
                            </Grid>
        				}
        				{ stderr &&
                            <Grid item xs={12}>
            					<div className={ classes.snippet }>
            						<b>stderr</b>
            						{ stderr }
            					</div>
                            </Grid>
        				}
                    </Grid>
                </Grid>
                <Grid item xs={12} md={8}>
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
		);
	}
}

const styles = (theme) => ({
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
        padding: theme.spacing.large,
        borderRadius: theme.spacing.unit,
        color: 'white',
        fontSize: '1.5rem',
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
		marginTop: theme.spacing.large,
	},
});

export default withStyles(styles)(MainContent);
