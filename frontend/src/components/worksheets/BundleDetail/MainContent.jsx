// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import {renderDuration} from '../../../util/worksheet_utils';
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
        console.log(classes);
		const bundleState = (bundleInfo.state == 'running' &&
							bundleInfo.metadata.run_status != 'Running')
					? bundleInfo.metadata.run_status
					: bundleInfo.state;
        const isRunBundle = bundleInfo.bundle_type === 'run';

        //Get the correct run time display
        const bundleRunTime = bundleInfo.metadata.time
            ? renderDuration(bundleInfo.metadata.time)
            : "-- --";

		return (
            <div className={ classes.outter }>
    			<Grid container classes={ { container: classes.container } } spacing={16}>    
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
                    { /** Run bundle specific components =========================================================== */}
                    { isRunBundle &&
                        <Grid container xs={12} md="auto" direction="row" justify='flex-end' style={{marginRight: 10}}>
                            <Grid item style={{ marginRight: 2 }}>
                                <AccessTimeIcon/>
                            </Grid>
                            <Grid item>
                                <Typography variant="body1">
                                    run time: { bundleRunTime }
                                </Typography>
                            </Grid>
                        </Grid>
                    }
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
	snippet: {
		fontFamily: 'monospace',
		backgroundColor: theme.color.grey.lightest,
		height: 160,
		marginBottom: theme.spacing.large,
        overflow: 'auto',
	},
});

export default withStyles(styles)(MainContent);
