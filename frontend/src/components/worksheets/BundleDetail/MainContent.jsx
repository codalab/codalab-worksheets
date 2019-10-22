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
        showStdOut: true,
        showStdError: true,
        showFileBrowser: true,
    }

    toggleFileViewer() {
        this.setState({showFileBrowser: !this.state.showFileBrowser});
    }

    toggleStdOut() {
        this.setState({showStdOut: !this.state.showStdOut});
    }

    toggleStdError() {
        this.setState({showStdError: !this.state.showStdError});
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
                    
                    { /** Stdout/stderr components ================================================================= */}
                    <Grid container>
                        { stdout &&
                            <Grid container>
                                <Button
                                onClick={e => this.toggleStdOut()}
                                size='small'
                                color='inherit'
                                aria-label='Show stdout'
                                >
                                    
                                    {'Stdout'}
                                {this.state.showStdOut 
                                    ? <KeyboardArrowDownIcon />
                                    : <ChevronRightIcon />}
                                </Button>
                                {this.state.showStdOut &&
                                    <Grid item xs={12}>
                                        <div className={ classes.snippet }>
                                            { stdout }
                                        </div>
                                    </Grid>}
                            </Grid>
                        }
                        { stderr &&
                            <Grid container>
                                <Button
                                onClick={e => this.toggleStdError()}
                                size='small'
                                color='inherit'
                                aria-label='Show stderr'
                                >
                                    
                                    {'Stderr'}
                                {this.state.showStdError 
                                    ? <KeyboardArrowDownIcon />
                                    : <ChevronRightIcon />}
                                </Button>
                                {this.state.showStdError &&
                                    <Grid item xs={12}>
                                        <div className={ classes.snippet }>
                                            { stderr }
                                        </div>
                                    </Grid>}
                            </Grid>
                        }
                    </Grid>
                    { /** Bundle contents browser ================================================================== */}
                    <Button
                        onClick={e => this.toggleFileViewer()}
                        size='small'
                        color='inherit'
                        aria-label='Expand file viewer'
                        >
                        {'Files'}
                        {this.state.showFileBrowser 
                            ? <KeyboardArrowDownIcon />
                            : <ChevronRightIcon />}
                    </Button>
                    {this.state.showFileBrowser
                        ?   <Grid item xs={12}>
                                { fileContents
                                    ?   <div className={ classes.fileSnippet }>
                                            { fileContents }
                                        </div>
                                    :   
                                        <div className={ classes.fileSnippet }>
                                            <FileBrowserLite
                                            uuid={ bundleInfo.uuid }
                                        />
                                        </div>
                                }
                            </Grid>
                        :   null}
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
        backgroundColor: theme.color.grey.lightest,
        maxHeight: 300, 
        width: 680,
        padding: 10,
        flexWrap: 'wrap', 
        flexShrink: 1,
        overflow:'auto',
    },
    fileSnippet: {
		fontFamily: 'monospace',
        maxHeight: 300, 
        width: 680,
        padding: 10,
        flexShrink: 1,
        overflow:'auto',
    },
});

export default withStyles(styles)(MainContent);
