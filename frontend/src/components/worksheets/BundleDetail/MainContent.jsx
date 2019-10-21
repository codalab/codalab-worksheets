// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import Grid from '@material-ui/core/Grid';
import { withStyles } from '@material-ui/core/styles';
import AccessTimeIcon from '@material-ui/icons/AccessTime';
import {renderDuration} from '../../../util/worksheet_utils';
import { FileBrowserLite } from '../../FileBrowser';
import Button from '@material-ui/core/Button';
import ChevronRightIcon from '@material-ui/icons/ChevronRight';
import KeyboardArrowDownIcon from '@material-ui/icons/KeyboardArrowDown';

class MainContent extends React.Component<
		{
			bundleInfo: {},
			stdout: string | null,
			strerr: string | null,
			fileContents: string | null,
            classes: {},
		}
> {	
    state = {
        showFileBrowser: this.props.bundleInfo.bundle_type !== 'run',
    }

    toggleFileViewer() {
        this.setState({showFileBrowser: !this.state.showFileBrowser});
    }
	
	render() {
		const {
            classes, bundleInfo, stdout, stderr, fileContents } = this.props;
        const isRunBundle = bundleInfo.bundle_type === 'run';

        //Get the correct run time display
        const bundleRunTime = bundleInfo.metadata.time
            ? renderDuration(bundleInfo.metadata.time)
            : "-- --";

		return (
            <div className={ classes.outter }>
    			<Grid container>    
                    { /** Bundle contents browser ================================================================== */}
                    <Button
                        onClick={e => this.toggleFileViewer()}
                        size='small'
                        color='inherit'
                        aria-label='Expand file viewer'
                        >
                            
                            {'Show contents'}
                        {this.state.showFileBrowser ? <KeyboardArrowDownIcon />
                        : <ChevronRightIcon />}
                    </Button>
                    {this.state.showFileBrowser?
                    <Grid item xs={12}>
        				{ fileContents
        					? <div className={ classes.snippet }>
        						{ fileContents }
        					</div>
        					: <FileBrowserLite
                                uuid={ bundleInfo.uuid }
                            />
        				}
                    </Grid>:null}
                    { /** Stdout/stderr components ================================================================= */}
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
       // flex: 1,
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
        height: 300,
        width: 680,
        padding: 10,
	},
});

export default withStyles(styles)(MainContent);
