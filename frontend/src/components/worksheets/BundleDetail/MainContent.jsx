// @flow
import * as React from 'react';
import Typography from '@material-ui/core/Typography';
import CopyIcon from '@material-ui/icons/FileCopy';
import Tooltip from '@material-ui/core/Tooltip';
import { withStyles } from '@material-ui/core';
import { CopyToClipboard } from 'react-copy-to-clipboard';

import { FileBrowser } from '../../FileBrowser';

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

    const bundleStateClass = `bundle-state state-${
        bundleInfo.state || 'ready' }`;
    // State of the bundle, IMPORTANT
    
    return (
      <div>
        <div className={ bundleStateClass }>{ bundleState }</div>
        { bundleInfo.bundle_type === 'run' &&
          <div className={ classes.section }>
            
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
            
            <Typography variant="body1">
              run time: { bundleInfo.metadata.time || 'unavailable' }
            </Typography>
          </div>
        }
        { stdout &&
          <div className={ classes.snippet }>
            <b>stdout</b>
            { stdout }
          </div>
        }
        { stderr &&
          <div className={ classes.snippet }>
            <b>stderr</b>
            { stderr }
          </div>
        }
        { fileContents
          ? <div className={ classes.snippet }>
            { fileContents }
          </div>
          : <FileBrowser uuid={ bundleInfo.uuid } />
        }
      </div>
    );
  }
}

const styles = (theme) => ({
  section: {
    marginTop: theme.spacing.large,
  },
  row: {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
  },
  command: {
    backgroundColor: '#333',
    color: 'white',
    fontFamily: 'monospace',
    padding: theme.spacing.unit,
  },
  snippet: {
    fontFamily: 'monospace',
    backgroundColor: theme.color.grey.lightest,
    height: 160,
    marginTop: theme.spacing.large,
  },
});

export default withStyles(styles)(MainContent);
